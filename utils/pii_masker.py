"""Маскирование персональных данных (PII) в тексте.

Проходит по тексту регулярными выражениями и заменяет
персональные данные на плейсхолдеры вида [ТЕЛЕФОН], [EMAIL] и т.д.

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


# =============================================================================
# Паттерны персональных данных
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


def mask_pii_in_text(text: str, page_number: int = 0) -> PageMaskingResult:
    """Маскирует персональные данные в тексте.

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

    masked_text = text
    matches: list[PIIMatch] = []

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
