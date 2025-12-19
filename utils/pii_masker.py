"""Маскирование персональных данных (PII) в тексте.

Проходит по тексту регулярными выражениями и заменяет
персональные данные на плейсхолдеры вида [ТЕЛЕФОН], [EMAIL] и т.д.

Для распознавания ФИО используется библиотека Natasha:
- NER модель для поиска спанов с типом PER
- NamesExtractor для валидации (разбор на first/last/middle)

Публичный API:
    - mask_pii_in_text() — маскирование текста одной страницы
    - mask_pii_in_document() — маскирование всего документа (OCR результат)
    - MaskingResult — результат маскирования с метаданными
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import NamedTuple

from config import logger

# Natasha NER для распознавания ФИО (lazy loading)
try:
    from natasha import (
        Segmenter,
        NewsEmbedding,
        NewsNERTagger,
        MorphVocab,
        NamesExtractor,
        Doc,
    )
    NATASHA_AVAILABLE = True
except ImportError:
    NATASHA_AVAILABLE = False
    logger.warning("Natasha не установлена — распознавание ФИО отключено")


# =============================================================================
# Natasha NER + NamesExtractor (lazy initialization)
# =============================================================================

class _NatashaModels:
    """Lazy-loaded Natasha модели для NER и валидации имён.

    Использует двухуровневую систему:
    1. NER (NewsNERTagger) — находит спаны с типом PER
    2. NamesExtractor — валидирует, что спан действительно ФИО
       (может разобрать на first/last/middle)
    """

    _instance: "_NatashaModels | None" = None
    _initialized: bool = False

    def __new__(cls) -> "_NatashaModels":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def _ensure_initialized(self) -> None:
        """Инициализирует модели при первом использовании."""
        if self._initialized or not NATASHA_AVAILABLE:
            return

        logger.info("Инициализация Natasha NER + NamesExtractor...")

        # Базовые компоненты
        self.segmenter = Segmenter()
        self.morph_vocab = MorphVocab()
        self.emb = NewsEmbedding()

        # NER для поиска PER спанов
        self.ner_tagger = NewsNERTagger(self.emb)

        # NamesExtractor для валидации имён (использует словари 190k+ слов)
        self.names_extractor = NamesExtractor(self.morph_vocab)

        self._initialized = True
        logger.info("Natasha готова к работе (NER + NamesExtractor)")

    def _is_valid_name(self, text: str) -> bool:
        """Проверяет, является ли текст реальным ФИО через NamesExtractor.

        NamesExtractor использует словари Natasha (7k имён, 182k фамилий)
        и грамматические правила для разбора текста на структуру:
        - first (имя)
        - last (фамилия)
        - middle (отчество)

        Если разбор успешен (есть хотя бы first или last) — это реальное ФИО.

        Args:
            text: Текст для проверки

        Returns:
            True если текст распознан как ФИО
        """
        if not text or not text.strip():
            return False

        try:
            # Пытаемся разобрать текст как имя
            match = self.names_extractor.find(text)

            if match and match.fact:
                # Проверяем что есть хотя бы имя или фамилия
                fact = match.fact
                has_first = getattr(fact, 'first', None) is not None
                has_last = getattr(fact, 'last', None) is not None
                return has_first or has_last

            return False

        except Exception:
            return False

    def extract_validated_names(self, text: str) -> list[tuple[int, int, str]]:
        """Извлекает и валидирует ФИО из текста.

        Двухэтапный процесс:
        1. NER находит все спаны с типом PER
        2. NamesExtractor валидирует каждый спан

        Args:
            text: Исходный текст

        Returns:
            Список кортежей (start, stop, text) только для валидных имён
        """
        if not NATASHA_AVAILABLE:
            return []

        self._ensure_initialized()

        try:
            # Шаг 1: NER находит PER спаны
            doc = Doc(text)
            doc.segment(self.segmenter)
            doc.tag_ner(self.ner_tagger)

            validated_names = []

            for span in doc.spans:
                if span.type != "PER":
                    continue

                # Шаг 2: Исключаем OCR-мусор
                span_text = span.text

                # Слишком короткие (типа "Е.", "А.")
                clean_text = span_text.strip().rstrip(".")
                if len(clean_text) < 3:
                    continue

                # Многострочные строки (OCR-артефакты)
                if "\n" in span_text:
                    continue

                # Спецсимволы (OCR-мусор типа "Е |", "Ми [Ире")
                if any(c in span_text for c in "|[]{}"):
                    continue

                # Смесь кириллицы и латиницы (кроме точек и пробелов)
                has_cyrillic = any("а" <= c.lower() <= "я" or c in "ёЁ" for c in span_text)
                has_latin = any("a" <= c.lower() <= "z" for c in span_text)
                if has_cyrillic and has_latin:
                    continue

                # Шаг 3: Исключаем строительные термины
                if _contains_construction_term(span.text):
                    continue

                # Шаг 4: Валидация через NamesExtractor
                if self._is_valid_name(span.text):
                    validated_names.append((span.start, span.stop, span.text))

            return validated_names

        except Exception as e:
            logger.warning("Ошибка Natasha NER: %s", e)
            return []


# Глобальный singleton для моделей
_natasha = _NatashaModels()


# =============================================================================
# Строительные термины (исключаются из ФИО)
# =============================================================================

# Слова, которые NER может ошибочно принять за имена в строительных документах
# "Пол" = Paul, "Ламинат" может походить на фамилию и т.д.
_CONSTRUCTION_TERMS: set[str] = {
    # Локации дефектов
    "Пол", "Потолок", "Стена", "Стены", "Полы", "Потолки",
    "Откос", "Откосы", "Порог", "Пороги",
    # Двери и окна
    "Дверь", "Двери", "Окно", "Окна", "Балкон", "Лоджия",
    "Оконный", "Дверной", "Балконный",
    # Материалы (могут быть приняты за фамилии)
    "Ламинат", "Плитка", "Паркет", "Линолеум", "Кафель",
    "Обои", "Штукатурка", "Шпаклёвка", "Шпаклевка",
    "Гипсокартон", "Бетон", "Кирпич", "Стяжка",
    # Комнаты
    "Кухня", "Комната", "Коридор", "Прихожая", "Санузел",
    "Ванная", "Туалет", "Спальня", "Гостиная", "Зал",
    # Элементы отделки
    "Плинтус", "Плинтуса", "Наличник", "Наличники",
    "Подоконник", "Карниз", "Розетка", "Выключатель",
    # Инженерные системы
    "Радиатор", "Батарея", "Вентиляция", "Кондиционер",
    "Счётчик", "Счетчик", "Смеситель", "Унитаз", "Раковина",
}


def _contains_construction_term(text: str) -> bool:
    """Проверяет, содержит ли текст строительные термины."""
    words = text.split()
    for word in words:
        # Проверяем слово без знаков препинания
        clean_word = word.strip(".,;:!?()[]\"'")
        if clean_word in _CONSTRUCTION_TERMS:
            return True
    return False


# =============================================================================
# Паттерны персональных данных (regex)
# =============================================================================


class PIIPattern(NamedTuple):
    """Паттерн для поиска персональных данных."""

    name: str           # Название типа PII
    pattern: str        # Регулярное выражение
    placeholder: str    # Плейсхолдер для замены


# Паттерны упорядочены по приоритету (более специфичные первыми)
PII_PATTERNS: list[PIIPattern] = [
    # Банковская карта (16 цифр, возможно с пробелами/дефисами)
    PIIPattern(
        name="bank_card",
        pattern=r'\b\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}\b',
        placeholder="[КАРТА]",
    ),

    # СНИЛС (XXX-XXX-XXX XX или без разделителей)
    PIIPattern(
        name="snils",
        pattern=r'\b\d{3}[\s\-]?\d{3}[\s\-]?\d{3}[\s\-]?\d{2}\b',
        placeholder="[СНИЛС]",
    ),

    # ИНН физлица (12 цифр)
    PIIPattern(
        name="inn",
        pattern=r'\b\d{12}\b',
        placeholder="[ИНН]",
    ),

    # Паспорт РФ (серия XXXX номер XXXXXX или слитно)
    PIIPattern(
        name="passport",
        pattern=r'\b\d{2}[\s\-]?\d{2}[\s\-]?\d{6}\b',
        placeholder="[ПАСПОРТ]",
    ),

    # Телефон (российский формат)
    # +7/8 (XXX) XXX-XX-XX или вариации
    PIIPattern(
        name="phone",
        pattern=r'(?:\+7|8)[\s\-\(\)]*\d{3}[\s\-\(\)]*\d{3}[\s\-]*\d{2}[\s\-]*\d{2}',
        placeholder="[ТЕЛЕФОН]",
    ),

    # Email
    PIIPattern(
        name="email",
        pattern=r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
        placeholder="[EMAIL]",
    ),
]


# =============================================================================
# Результаты маскирования
# =============================================================================


@dataclass
class PIIMatch:
    """Найденное совпадение PII."""

    pii_type: str       # Тип PII (phone, email, etc.)
    original: str       # Оригинальное значение
    position: int       # Позиция в тексте


@dataclass
class PageMaskingResult:
    """Результат маскирования одной страницы."""

    page_number: int
    original_text: str
    masked_text: str
    matches: list[PIIMatch] = field(default_factory=list)

    @property
    def has_pii(self) -> bool:
        """Были ли найдены персональные данные."""
        return len(self.matches) > 0

    @property
    def pii_count(self) -> int:
        """Количество найденных PII."""
        return len(self.matches)

    @property
    def pii_types(self) -> set[str]:
        """Типы найденных PII."""
        return {m.pii_type for m in self.matches}


@dataclass
class DocumentMaskingResult:
    """Результат маскирования всего документа."""

    total_pages: int
    pages_with_pii: list[int] = field(default_factory=list)
    total_pii_count: int = 0
    pii_by_type: dict[str, int] = field(default_factory=dict)
    page_results: list[PageMaskingResult] = field(default_factory=list)

    @property
    def has_pii(self) -> bool:
        """Были ли найдены персональные данные в документе."""
        return len(self.pages_with_pii) > 0

    def get_summary(self) -> str:
        """Возвращает краткую сводку для логов/уведомлений."""
        if not self.has_pii:
            return "Персональные данные не обнаружены"

        parts = [f"Найдено {self.total_pii_count} PII на страницах: {self.pages_with_pii}"]

        type_info = ", ".join(f"{k}: {v}" for k, v in sorted(self.pii_by_type.items()))
        if type_info:
            parts.append(f"Типы: {type_info}")

        return ". ".join(parts)


# =============================================================================
# Функции маскирования
# =============================================================================


def _mask_names_with_ner(text: str, matches: list[PIIMatch]) -> str:
    """Маскирует ФИО с помощью Natasha NER + NamesExtractor.

    Двухэтапная валидация:
    1. NER находит спаны с типом PER
    2. NamesExtractor проверяет по словарям (190k+ слов)

    Args:
        text: Исходный текст
        matches: Список для добавления найденных совпадений

    Returns:
        Текст с замаскированными именами
    """
    validated_names = _natasha.extract_validated_names(text)

    if not validated_names:
        return text

    # Сортируем по позиции в обратном порядке, чтобы замена не сбивала индексы
    names_sorted = sorted(validated_names, key=lambda x: x[0], reverse=True)

    masked_text = text
    for start, stop, name_text in names_sorted:
        matches.append(PIIMatch(
            pii_type="name",
            original=name_text,
            position=start,
        ))
        masked_text = masked_text[:start] + "[ФИО]" + masked_text[stop:]

    return masked_text


def mask_pii_in_text(text: str, page_number: int = 0) -> PageMaskingResult:
    """Маскирует персональные данные в тексте.

    Использует Natasha NER + NamesExtractor для распознавания ФИО
    и regex для остальных типов PII.

    Args:
        text: Исходный текст
        page_number: Номер страницы (для отчётности)

    Returns:
        PageMaskingResult с замаскированным текстом и метаданными
    """
    if not text:
        return PageMaskingResult(
            page_number=page_number,
            original_text=text,
            masked_text=text,
        )

    matches: list[PIIMatch] = []

    # Шаг 1: Маскируем ФИО с помощью NER + NamesExtractor
    masked_text = _mask_names_with_ner(text, matches)

    # Шаг 2: Маскируем остальные PII с помощью regex
    for pii_pattern in PII_PATTERNS:
        regex = re.compile(pii_pattern.pattern, re.IGNORECASE)

        # Находим все совпадения для отчётности
        for match in regex.finditer(masked_text):
            # Проверяем что это не уже замаскированный плейсхолдер
            if not match.group().startswith("["):
                matches.append(PIIMatch(
                    pii_type=pii_pattern.name,
                    original=match.group(),
                    position=match.start(),
                ))

        # Заменяем все совпадения
        masked_text = regex.sub(pii_pattern.placeholder, masked_text)

    return PageMaskingResult(
        page_number=page_number,
        original_text=text,
        masked_text=masked_text,
        matches=matches,
    )


def mask_pii_in_document(
    pages: list[tuple[int, str]],
) -> tuple[list[tuple[int, str]], DocumentMaskingResult]:
    """Маскирует персональные данные во всём документе.

    Args:
        pages: Список кортежей (номер_страницы, текст)

    Returns:
        Кортеж:
            - Список кортежей (номер_страницы, замаскированный_текст)
            - DocumentMaskingResult с метаданными
    """
    masked_pages: list[tuple[int, str]] = []
    page_results: list[PageMaskingResult] = []
    pages_with_pii: list[int] = []
    pii_by_type: dict[str, int] = {}
    total_pii_count = 0

    for page_number, text in pages:
        result = mask_pii_in_text(text, page_number)
        page_results.append(result)
        masked_pages.append((page_number, result.masked_text))

        if result.has_pii:
            pages_with_pii.append(page_number)
            total_pii_count += result.pii_count

            for match in result.matches:
                pii_by_type[match.pii_type] = pii_by_type.get(match.pii_type, 0) + 1

    doc_result = DocumentMaskingResult(
        total_pages=len(pages),
        pages_with_pii=pages_with_pii,
        total_pii_count=total_pii_count,
        pii_by_type=pii_by_type,
        page_results=page_results,
    )

    if doc_result.has_pii:
        logger.info("PII маскирование: %s", doc_result.get_summary())

    return masked_pages, doc_result
