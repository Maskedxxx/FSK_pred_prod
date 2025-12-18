"""Сервис фильтрации релевантных страниц через Flowise LLM (FSM).

Логика FSM:
  1. SEARCH_START: ищем страницу где НАЧИНАЕТСЯ список дефектов
  2. SEARCH_END: ищем где ЗАКАНЧИВАЕТСЯ список дефектов

Результат: диапазон страниц [start_page, last_defect_page]

Важно:
- Настройки берём ТОЛЬКО из `config.py` (config = источник правды).
- Внутри сервиса настройки не переопределяем.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import requests
from pydantic import BaseModel, Field

from config import (
    logger,
    FLOWISE_API_URL_SEARCH_START,
    FLOWISE_API_URL_SEARCH_END,
    FLOWISE_BATCH_SIZE,
    FLOWISE_TIMEOUT_SECONDS,
    FLOWISE_MAX_CHARS_PER_PAGE,
)


# -----------------------------
# Промпты для Flowise
# -----------------------------

PROMPT_SEARCH_START = """Ты аналитик строительной документации. Твоя задача — найти страницу, где НАЧИНАЕТСЯ детальный список/ведомость дефектов.

<defects_start_definition>
DEFECTS_START — страница, где начинается ДЕТАЛЬНЫЙ перечень выявленных дефектов.

ВАЖНО: Все пункты ниже должны соблюдаться ТОЛЬКО при наличии ОПИСАНИЯ САМОГО ДЕФЕКТА (что именно повреждено, как выглядит повреждение, какое отклонение от нормы):
- Содержит конкретные описания повреждений: "трещины", "отслоения", "отклонение X мм", "царапины", "вздутие", "просадка"
- Указаны конкретные помещения/места: "Комната №3", "Коридор", "Санузел", "Кухня"
- Есть ссылки на нормативы: СП, ГОСТ, СНиП (с указанием конкретного пункта)
- Может иметь заголовки: "Ведомость дефектов", "Результаты обследования", "Перечень недостатков"

Ключевой признак: НА СТРАНИЦЕ ЕСТЬ ТЕКСТ, ОПИСЫВАЮЩИЙ КОНКРЕТНЫЙ ДЕФЕКТ — что сломано/повреждено, где находится, и как это нарушает норматив.
</defects_start_definition>

<not_defects_start>
ЧТО НЕ ЯВЛЯЕТСЯ НАЧАЛОМ (ИГНОРИРОВАТЬ):

ВАЖНО: Пункты ниже применяются, если на странице НЕТ описания конкретных дефектов (нет текста вида "трещина в стене комнаты №3, отклонение 5мм"):
- Титульные листы, оглавление — нет описания дефектов, только структура документа
- Краткие выводы типа "Выявлено 50 дефектов" — упоминание количества БЕЗ детального перечисления самих дефектов
- Юридические документы: претензии, доверенности, договоры — юридический текст без технического описания повреждений
- Нормативные справки: таблицы требований ГОСТ/СП — только нормы без привязки к конкретным найденным дефектам
- Квалификационные аттестаты, описание методологии — информация об эксперте/методах, не о дефектах
- Общие описания объекта — характеристики квартиры/дома без указания что именно повреждено
</not_defects_start>

<examples>
ПРИМЕРЫ НАЧАЛА (DEFECTS_START):

Пример 1:
"Помещение № 3 (Фото 5, 6) Окна ПВХ. Отклонение уровня оконного откоса от вертикали более 5 мм. СП 71.13330.2017 п. 7.2.13"
→ Это НАЧАЛО: конкретное помещение + элемент (окна) + измеримый дефект (отклонение 5мм) + норматив

Пример 2:
"Коридор. Напольное покрытие (ламинат): вздутие кромок ламелей, щели между планками до 3мм. Нарушение СП 29.13330.2011 п.8.1"
→ Это НАЧАЛО: помещение + элемент + описание дефекта + норматив

Пример 3:
"Санузел (Фото 12-14). Керамическая плитка на стенах: трещины в межплиточных швах, отслоение плитки в углах. Не соответствует ГОСТ 6141-91"
→ Это НАЧАЛО: место + материал + конкретные повреждения + ссылка на ГОСТ

---

ПРИМЕРЫ НЕ НАЧАЛА:

Пример 1:
"ЗАКЛЮЧЕНИЕ СПЕЦИАЛИСТА № 752 ПО ОПРЕДЕЛЕНИЮ НЕДОСТАТКОВ В КВ. 801, г. Москва"
→ НЕ начало: это только НАЗВАНИЕ документа, слово "недостатков" в заголовке — не описание дефекта

Пример 2:
"В ходе обследования выявлены многочисленные дефекты отделочных работ, нарушающие требования СП и ГОСТ."
→ НЕ начало: общая фраза о наличии дефектов, но нет КОНКРЕТНОГО описания (какой дефект, где, сколько мм)

Пример 3:
"Таблица допустимых отклонений по СП 71.13330.2017: отклонение от вертикали — не более 5мм на 1м..."
→ НЕ начало: это справочная таблица норм, а не описание найденных дефектов в квартире
</examples>

<pages_to_analyze>
СТРАНИЦЫ ДЛЯ АНАЛИЗА:

{pages_content}
</pages_to_analyze>

<task>
Проанализируй страницы выше. Найди номер страницы, где НАЧИНАЕТСЯ детальный список дефектов.
Если в данном батче нет начала списка — верни start_page = -1 и found = false.
</task>"""


PROMPT_SEARCH_END = """Ты аналитик строительной документации. Твоя задача — найти, где ЗАКАНЧИВАЕТСЯ список дефектов.

<context>
ТАК ВЫГЛЯДИТ НАЧАЛО СПИСКА ДЕФЕКТОВ (страница {start_page}):

{start_page_text}
</context>

<defect_definition>
НАПОМИНАНИЕ — что считается ОПИСАНИЕМ ДЕФЕКТА:
Текст, содержащий: конкретное помещение/место + элемент отделки + описание повреждения + отклонение от нормы.
Пример: "Коридор. Ламинат: вздутие кромок, щели до 3мм. Нарушение СП 29.13330.2011"
</defect_definition>

<end_of_list_definition>
ЧТО ОЗНАЧАЕТ КОНЕЦ СПИСКА ДЕФЕКТОВ:

Список дефектов ЗАКАНЧИВАЕТСЯ, когда начинается один из следующих разделов И после него больше НЕТ описаний конкретных дефектов:

1. СМЕТА/РАСЧЁТ СТОИМОСТИ:
   - Заголовки: "Локальная смета", "Дефектная ведомость и локальная смета", "Расчет стоимости устранения", "Вопрос №2" (о стоимости)
   - Признаки: таблицы с ценами (руб.), единицами измерения (м², шт.), расценками ФЕР/ТЕР
   - Пример: "Демонтаж ламината — 15 м² × 120 руб. = 1800 руб."

2. ВЫВОДЫ/ЗАКЛЮЧЕНИЕ ЭКСПЕРТА:
   - Заголовки: "Выводы", "Заключение эксперта", "Ответы на вопросы"
   - Признаки: итоговые фразы БЕЗ новых описаний дефектов: "Таким образом, выявлены нарушения...", "На основании изложенного..."
   - Пример: "Вывод: качество отделочных работ не соответствует требованиям СП"

3. ПРИЛОЖЕНИЯ/ФОТОМАТЕРИАЛЫ (как отдельный раздел):
   - Заголовки: "Приложение №1", "Фотофиксация", "Фототаблица"
   - Признаки: страницы только с фото без текстового описания дефектов, или список приложенных документов
   - Пример: "Приложение 1. Фотоматериалы осмотра" (далее только фото)

4. НОРМАТИВНАЯ СПРАВКА В КОНЦЕ:
   - Таблицы допусков ГОСТ/СП без привязки к конкретным дефектам квартиры
   - Цитаты нормативов как справочный материал
</end_of_list_definition>

<gaps_in_list>
ВАЖНО ПРО ДЫРКИ (1-2 страницы без дефектов внутри списка):

Если между страницами с описанием дефектов есть 1-2 страницы с фото/иллюстрациями, но ПОСЛЕ них снова идут ОПИСАНИЯ ДЕФЕКТОВ — это НЕ конец.

Конец — когда описания дефектов (помещение + элемент + повреждение + норма) больше НЕ возвращаются.
</gaps_in_list>

<continuation_signs>
ЧТО СЧИТАТЬ ПРОДОЛЖЕНИЕМ СПИСКА (НЕ КОНЕЦ):

ВАЖНО: Пункты ниже означают продолжение ТОЛЬКО если ПОСЛЕ них идёт описание дефектов (помещение + элемент + повреждение + отклонение):

- Подзаголовки разделов работ: "ОКНА", "САНТЕХНИКА", "ЭЛЕКТРИКА", "ПОЛЫ", "СТЕНЫ" — если после заголовка идёт описание дефектов этого раздела
- Страницы с фото внутри описания дефектов — если фото сопровождает текст с описанием дефекта или после фото продолжается список
- Гибридные страницы: дефекты + начало выводов внизу страницы — страница ещё содержит описания дефектов, значит это НЕ конец

Пример продолжения:
"ЭЛЕКТРИКА
Помещение №2 (Кухня). Розетки: отклонение от вертикали 8мм. Нарушение ПУЭ п.6.6.30"
→ Это ПРОДОЛЖЕНИЕ: есть подзаголовок + после него конкретное описание дефекта
</continuation_signs>

<pages_to_analyze>
СТРАНИЦЫ ДЛЯ АНАЛИЗА:

{pages_content}
</pages_to_analyze>

<task>
Проанализируй страницы выше. Найди:
1. last_defect_page — номер ПОСЛЕДНЕЙ страницы, содержащей описание дефектов в этом батче. Если в батче нет страниц с дефектами — верни -1.
2. definitely_ended — true если нашёл чёткий конец (смета/выводы/приложения начались и описания дефектов больше не вернутся), false если список может продолжаться дальше
</task>"""


# -----------------------------
# Pydantic модели
# -----------------------------

class PageData(BaseModel):
    """Данные одной страницы из OCR."""

    page_number: int = Field(..., description="Номер страницы (1..N).")
    text: str = Field(..., description="Текст страницы.")


class BatchDebugInfo(BaseModel):
    """Отладочная информация по одному батчу."""

    batch_num: int = Field(..., description="Номер батча.")
    pages: list[int] = Field(..., description="Номера страниц в батче.")
    elapsed_seconds: float = Field(..., description="Время выполнения запроса.")
    http_status: int | None = Field(None, description="HTTP статус ответа.")
    parsed_response: dict | None = Field(None, description="Распарсенный JSON ответ.")
    raw_response: dict | None = Field(None, description="Сырой ответ Flowise.")


class PageFilterResult(BaseModel):
    """Результат фильтрации релевантных страниц."""

    txt_path: Path = Field(..., description="Путь к исходному OCR файлу.")
    total_pages: int = Field(..., description="Всего страниц в документе.")
    relevant_pages: list[int] = Field(default_factory=list, description="Список релевантных страниц.")
    relevant_count: int = Field(..., description="Количество релевантных страниц.")
    start_page: int | None = Field(None, description="Страница начала дефектов.")
    end_page: int | None = Field(None, description="Страница конца дефектов.")
    fsm_final_state: Literal["FINISHED", "NO_DEFECTS_FOUND"] = Field(
        ..., description="Финальное состояние FSM."
    )
    elapsed_seconds: float = Field(..., description="Общее время выполнения.")
    debug_search_start: list[BatchDebugInfo] = Field(
        default_factory=list, description="Отладка фазы SEARCH_START."
    )
    debug_search_end: list[BatchDebugInfo] = Field(
        default_factory=list, description="Отладка фазы SEARCH_END."
    )


# -----------------------------
# Внутренние функции
# -----------------------------

def _parse_ocr_txt_file(txt_path: Path) -> list[PageData]:
    """Парсит OCR .txt файл и извлекает страницы.

    Формат файла: === Страница N === текст...
    """
    logger.info("Парсинг OCR файла: %s", txt_path)

    content = txt_path.read_text(encoding="utf-8")

    # Формат: === Страница N ===
    pattern = r"=== Страница (\d+) ==="
    parts = re.split(pattern, content)

    pages: list[PageData] = []
    for i in range(1, len(parts), 2):
        if i + 1 < len(parts):
            page_num = int(parts[i])
            page_text = parts[i + 1].strip()
            if page_text:
                pages.append(PageData(page_number=page_num, text=page_text))

    logger.info("Извлечено страниц: %s", len(pages))
    return pages


def _format_pages_for_prompt(pages: list[PageData], max_chars: int) -> str:
    """Форматирует страницы для промпта."""
    lines: list[str] = []
    for page in pages:
        preview = page.text[:max_chars].replace("\n", " ")
        lines.append(f"PAGE {page.page_number}: {preview}")
    return "\n\n".join(lines)


def _extract_json_response(flowise_response: Any) -> dict | None:
    """Извлекает JSON из ответа Flowise."""
    if not isinstance(flowise_response, dict):
        return None

    # Flowise возвращает {"json": {...}}
    if isinstance(flowise_response.get("json"), dict):
        return flowise_response["json"]

    # Fallback: попробовать распарсить из text/answer
    for key in ("text", "answer", "result"):
        val = flowise_response.get(key)
        if isinstance(val, str) and val.strip():
            try:
                raw = val.strip()
                raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
                raw = re.sub(r"\s*```\s*$", "", raw)
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass

    return None


def _post_flowise(api_url: str, question: str, session_id: str, timeout: float) -> dict[str, Any]:
    """Отправляет запрос в Flowise API."""
    payload = {
        "question": question,
        "overrideConfig": {"sessionId": session_id},
    }

    try:
        resp = requests.post(api_url, json=payload, timeout=timeout)
        data = resp.json()
    except Exception as e:
        logger.error("Ошибка запроса Flowise: %s", e)
        return {"status_code": 0, "response": {"error": str(e)}}

    return {"status_code": resp.status_code, "response": data}


def _search_start(
    all_pages: list[PageData],
    batch_size: int,
    max_chars: int,
    timeout: float,
) -> tuple[int | None, list[BatchDebugInfo]]:
    """Фаза 1: Поиск начала списка дефектов."""
    logger.info("=" * 60)
    logger.info("ФАЗА 1: ПОИСК НАЧАЛА СПИСКА ДЕФЕКТОВ")
    logger.info("=" * 60)

    debug_info: list[BatchDebugInfo] = []
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    for i in range(0, len(all_pages), batch_size):
        batch = all_pages[i : i + batch_size]
        batch_num = i // batch_size + 1
        batch_pages = [p.page_number for p in batch]

        logger.info("Батч %s: страницы %s", batch_num, batch_pages)

        pages_content = _format_pages_for_prompt(batch, max_chars)
        full_prompt = PROMPT_SEARCH_START.format(pages_content=pages_content)

        session_id = f"search_start_{run_ts}_batch_{batch_num}"

        t0 = time.perf_counter()
        result = _post_flowise(FLOWISE_API_URL_SEARCH_START, full_prompt, session_id, timeout)
        elapsed = time.perf_counter() - t0

        response = result.get("response", {})
        parsed = _extract_json_response(response)

        batch_debug = BatchDebugInfo(
            batch_num=batch_num,
            pages=batch_pages,
            elapsed_seconds=round(elapsed, 2),
            http_status=result.get("status_code"),
            parsed_response=parsed,
            raw_response=response,
        )
        debug_info.append(batch_debug)

        if result.get("status_code", 0) >= 400:
            logger.error("HTTP ошибка %s", result.get("status_code"))
            logger.error("Ответ сервера: %s", response)
            continue

        if not parsed:
            logger.warning("Не удалось распарсить ответ батча %s", batch_num)
            continue

        found = parsed.get("found", False)
        start_page = parsed.get("start_page")
        reason = parsed.get("reason", "")

        logger.info("  found=%s, start_page=%s", found, start_page)
        logger.info("  reason: %s...", reason[:100] if reason else "")

        # start_page = -1 означает "не найдено"
        if found and start_page is not None and int(start_page) > 0:
            logger.info(">>> НАЙДЕНО НАЧАЛО на странице %s", start_page)
            return int(start_page), debug_info

    logger.warning("Начало списка дефектов НЕ НАЙДЕНО")
    return None, debug_info


def _search_end(
    all_pages: list[PageData],
    start_page: int,
    start_page_text: str,
    batch_size: int,
    max_chars: int,
    timeout: float,
) -> tuple[int | None, list[BatchDebugInfo]]:
    """Фаза 2: Поиск конца списка дефектов."""
    logger.info("=" * 60)
    logger.info("ФАЗА 2: ПОИСК КОНЦА (начиная со страницы %s)", start_page)
    logger.info("=" * 60)

    debug_info: list[BatchDebugInfo] = []
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Фильтруем страницы начиная с start_page
    pages_from_start = [p for p in all_pages if p.page_number >= start_page]

    last_known_defect_page = start_page

    for i in range(0, len(pages_from_start), batch_size):
        batch = pages_from_start[i : i + batch_size]
        batch_num = i // batch_size + 1
        batch_pages = [p.page_number for p in batch]

        logger.info("Батч %s: страницы %s", batch_num, batch_pages)

        pages_content = _format_pages_for_prompt(batch, max_chars)

        # Контекст стартовой страницы (сокращённый)
        start_context = start_page_text[:10000].replace("\n", " ")

        full_prompt = PROMPT_SEARCH_END.format(
            start_page=start_page,
            start_page_text=start_context,
            pages_content=pages_content,
        )

        session_id = f"search_end_{run_ts}_batch_{batch_num}"

        t0 = time.perf_counter()
        result = _post_flowise(FLOWISE_API_URL_SEARCH_END, full_prompt, session_id, timeout)
        elapsed = time.perf_counter() - t0

        response = result.get("response", {})
        parsed = _extract_json_response(response)

        batch_debug = BatchDebugInfo(
            batch_num=batch_num,
            pages=batch_pages,
            elapsed_seconds=round(elapsed, 2),
            http_status=result.get("status_code"),
            parsed_response=parsed,
            raw_response=response,
        )
        debug_info.append(batch_debug)

        if result.get("status_code", 0) >= 400:
            logger.error("HTTP ошибка %s", result.get("status_code"))
            logger.error("Ответ сервера: %s", response)
            continue

        if not parsed:
            logger.warning("Не удалось распарсить ответ батча %s", batch_num)
            continue

        last_defect_page = parsed.get("last_defect_page")
        definitely_ended = parsed.get("definitely_ended", False)
        reason = parsed.get("reason", "")

        logger.info("  last_defect_page=%s, definitely_ended=%s", last_defect_page, definitely_ended)
        logger.info("  reason: %s...", reason[:100] if reason else "")

        # last_defect_page = -1 означает "нет дефектов в батче"
        if last_defect_page is not None and int(last_defect_page) > 0:
            last_known_defect_page = int(last_defect_page)

        if definitely_ended:
            logger.info(">>> НАЙДЕН КОНЕЦ: последняя страница с дефектами = %s", last_known_defect_page)
            return last_known_defect_page, debug_info

    # Если дошли до конца документа без явного STOP
    logger.info("Документ закончился. Последняя страница с дефектами: %s", last_known_defect_page)
    return last_known_defect_page, debug_info


# -----------------------------
# Публичный API
# -----------------------------

def filter_relevant_pages(
    txt_path: str | Path,
    *,
    max_pages: int | None = None,
    batch_size: int | None = None,
) -> PageFilterResult:
    """Фильтрует релевантные страницы через Flowise LLM (FSM).

    Args:
        txt_path: путь к OCR .txt файлу (формат: === Страница N ===)
        max_pages: ограничение по страницам (None = все)
        batch_size: размер батча (None = из config)

    Returns:
        PageFilterResult с диапазоном релевантных страниц.
    """
    txt_file = Path(txt_path).expanduser().resolve()
    if not txt_file.exists():
        raise FileNotFoundError(f"OCR файл не найден: {txt_file}")

    _batch_size = batch_size if batch_size and batch_size > 0 else int(FLOWISE_BATCH_SIZE)
    _max_chars = int(FLOWISE_MAX_CHARS_PER_PAGE)
    _timeout = float(FLOWISE_TIMEOUT_SECONDS)

    logger.info("=" * 60)
    logger.info("FLOWISE FSM ФИЛЬТРАЦИЯ (ПОИСК ГРАНИЦ)")
    logger.info("=" * 60)
    logger.info("OCR файл: %s", txt_file)
    logger.info("Размер батча: %s", _batch_size)
    logger.info("API START: %s", FLOWISE_API_URL_SEARCH_START)
    logger.info("API END: %s", FLOWISE_API_URL_SEARCH_END)

    all_pages = _parse_ocr_txt_file(txt_file)

    # Применяем лимит страниц
    if max_pages and max_pages > 0:
        all_pages = [p for p in all_pages if p.page_number <= max_pages]

    if not all_pages:
        raise ValueError("Не найдено страниц в OCR файле")

    # Словарь для быстрого доступа к тексту
    page_dict = {p.page_number: p.text for p in all_pages}

    started_at = time.perf_counter()

    # === ФАЗА 1: ПОИСК НАЧАЛА ===
    start_page, start_debug = _search_start(all_pages, _batch_size, _max_chars, _timeout)

    if start_page is None:
        # Не нашли начало — нет релевантных страниц
        return PageFilterResult(
            txt_path=txt_file,
            total_pages=len(all_pages),
            relevant_pages=[],
            relevant_count=0,
            start_page=None,
            end_page=None,
            fsm_final_state="NO_DEFECTS_FOUND",
            elapsed_seconds=round(time.perf_counter() - started_at, 2),
            debug_search_start=[d.model_dump() for d in start_debug],  # type: ignore[misc]
            debug_search_end=[],
        )

    # === ФАЗА 2: ПОИСК КОНЦА ===
    start_page_text = page_dict.get(start_page, "")
    end_page, end_debug = _search_end(
        all_pages, start_page, start_page_text, _batch_size, _max_chars, _timeout
    )

    # Формируем список релевантных страниц
    relevant_pages = list(range(start_page, (end_page or start_page) + 1))

    return PageFilterResult(
        txt_path=txt_file,
        total_pages=len(all_pages),
        relevant_pages=relevant_pages,
        relevant_count=len(relevant_pages),
        start_page=start_page,
        end_page=end_page,
        fsm_final_state="FINISHED",
        elapsed_seconds=round(time.perf_counter() - started_at, 2),
        debug_search_start=[d.model_dump() for d in start_debug],  # type: ignore[misc]
        debug_search_end=[d.model_dump() for d in end_debug],  # type: ignore[misc]
    )


def save_filter_result(result: PageFilterResult, *, result_dir: str | Path) -> Path:
    """Сохраняет результат фильтрации в JSON.

    Args:
        result: результат фильтрации
        result_dir: директория для сохранения

    Returns:
        Путь к сохранённому JSON файлу.
    """
    out_dir = Path(result_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = result.txt_path.stem
    out_path = out_dir / f"page_filter_{stem}_{run_ts}.json"

    # Сериализация в JSON
    if hasattr(result, "model_dump_json"):
        json_text = result.model_dump_json(indent=2)
    else:
        json_text = result.json(indent=2, ensure_ascii=False)

    out_path.write_text(json_text, encoding="utf-8")
    logger.info("Результат фильтрации сохранён: %s", out_path)

    return out_path
