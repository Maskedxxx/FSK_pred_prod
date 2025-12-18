"""Сервис извлечения дефектов из VLM-очищенных страниц через Flowise LLM.

Принимает результат VLM очистки (VLMCleaningResult), обрабатывает постранично
с контекстом соседних страниц, возвращает структурированный список дефектов.

Публичный API:
    - extract_defects() — основная async функция извлечения
    - save_extraction_result() — сохранение результата в JSON
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, Field

from config import (
    logger,
    FLOWISE_API_URL_DEFECT_EXTRACT,
    DEFECT_EXTRACTION_CONCURRENCY,
    DEFECT_EXTRACTION_CONTEXT_CHARS,
    DEFECT_EXTRACTION_TIMEOUT_SECONDS,
    DEFECT_EXTRACTION_MAX_RETRIES,
    DEFECT_EXTRACTION_RETRY_DELAY_SECONDS,
)
from services.vlm_page_cleaner import VLMCleaningResult


# =============================================================================
# Промпт для извлечения дефектов
# =============================================================================

DEFECT_EXTRACTION_PROMPT = """You are an experienced construction expert and technical quality control specialist.

<document_structure>
The provided text is a construction work expertise report organized by SECTIONS. Each section focuses on a specific CONSTRUCTION TYPE in the premises (floor, ceiling, wall, door, window, etc.). Each section lists specific defects identified for that construction type.
</document_structure>

<task_definition>
Extract ALL defects from each section of the expertise report and structure them according to the schema fields.
</task_definition>

<extraction_rules>
DEFECT IDENTIFICATION RULE:
- A defect is a UNIQUE physical issue tied to a specific room + location (construction element).
- Do NOT split one defect into multiple records just because it cites multiple clauses/points
  of the same standard. Keep it as ONE defect and include all cited clauses in the source_text.
- Create separate defects only when the text clearly describes different issues and/or different room/location.
- Defect details (dimensions, rooms, characteristics) are combined into one description (do not fragment).
- General phrases WITHOUT normative references = section headers, NOT defects

EXTRACTION PROCESS:
- Within each section, find ALL defect statements (usually supported by technical references)
- If a defect has nested details/specifics - include them in the defect description, do NOT create separate entry
</extraction_rules>

<analysis_rules>
1. SECTION AND LOCALIZATION IDENTIFICATION:
   - Find sections by construction types (e.g.: "ПОТОЛКИ", "ПОЛЫ", "СТЕНЫ", "ДВЕРИ")
   - All defects from "ПОТОЛКИ" section → location = "Потолок"
   - All defects from "ПОЛЫ" section → location = "Пол"
   - All defects from "СТЕНЫ" section → location = "Стена"
   - And so on for each construction type

2. DEFECT EXTRACTION:
   - Inside each section find ALL defect statements (usually supported by technical references)
   - Do NOT create separate records per clause/point of the same standard; keep 1 record per physical defect
   - If one paragraph contains multiple distinct defects (different room/location/issue) — create multiple records
</analysis_rules>

<field_filling_rules>
According to DefectAnalysisResult schema:

source_text - source excerpt for the defect (verbatim):
- Copy the FULL defect fragment (typically 1-3 sentences) from the document text
- Preserve technical terminology
- Include ALL measurements and ALL normative references/clauses mentioned for this defect
- Do NOT paraphrase and do NOT shorten aggressively

room - room type where defect was found:
- Use the exact room name from the document (headers like "Кухня", "Лоджия", etc.)
- IMPORTANT: do not downgrade "Кухня/Лоджия/..." to "Комната" if the room is explicitly known.
- If not specified in the text: use "Комната"

location - defect localization according to expertise section:
- "Пол", "Потолок", "Стена", "Межкомнатная дверь", "Входная дверь", "Оконный блок", "Откосы"

defect - select short key from defect reference list:
- Choose the most semantically appropriate key from the provided defect mapping
- Select based on technical description and construction type
- Use exact key name from the reference list

work_type - work type for defect elimination:
- "Отделочные работы", "Сантехнические работы", "Электромонтажные работы", "Плиточные работы", "Малярные работы", "Штукатурные работы", "Демонтажные работы"
</field_filling_rules>

<important_notes>
- DO NOT SKIP defects because they seem minor
- DO NOT CREATE separate records for nested defect details
- COMBINE details into main defect description
- If section contains no defects - do not create records for it
- Use ONLY values from provided lists for fields with limited choice
</important_notes>

<defect_reference_mapping>
Select defect key from this reference list based on technical description.

KEY PREFIX GUIDE - Use prefixes to quickly identify defect category:
- ventilation_*: Ventilation grilles and diffusers defects (4 keys)
- heating_*: Heating pipe rosettes and heating system defects (8 keys)
- wallpaper_*: Wallpaper and wallpaper painting defects (8 keys)
- window_*: Window and balcony door blocks defects, window slopes (8 keys)
- entrance_*: Entrance door defects (8 keys)
- interior_*: Interior door defects (3 keys)
- door_*: Door trims and extensions defects (4 keys)
- balcony_*: Balcony and loggia defects (3 keys)
- baseboards_*: Baseboards and thresholds defects (5 keys)
- threshold_*: Threshold-specific defects (1 key)
- ceiling_*: Ceiling painting defects (2 keys)
- stretch_*: Stretch ceiling defects (5 keys)
- inspection_*: Inspection hatch defects (4 keys)
- floor_*: Floor tile defects (10 keys)
- wall_*: Wall tile defects (10 keys)
- plumbing_*: Plumbing fixtures defects (6 keys)
- laminate_*: Laminate flooring defects (6 keys)
- bath_*: Bath screen defects (1 key)
- wet_*: Cleaning defects (1 key)

DEFECT REFERENCE LIST:

- ventilation_system_malfunction: Работоспособность системы
- ventilation_project_mismatch: Соответствие проекту
- ventilation_wall_ceiling_gap: Зазор по стене/потолку
- ventilation_surface_defects: Дефекты поверхности
- heating_pipes_joint_overlap: Перекрытие швов
- heating_pipes_surface_defects: Дефекты поверхности
- heating_pipes_sewerage: Канализация
- heating_pipes_gaps: Зазоры
- heating_pipes_fire_protection: Противопожарный водопровод и спринклерное пожаротушение
- heating_pipes_water_supply: Водопровод
- heating_pipes_cold_supply: Холодоснабжение
- wallpaper_paint_uniformity: Равномерность окраски
- wallpaper_surface_chalking: Меление поверхности
- wallpaper_surface_defects: Дефекты поверхности
- window_mounting_seam_mismatch: Монтажный шов не соответствует проекту
- window_trim_cracks_gaps: Трещины, зазоры в примыкание пластиковых нащельников к откосам
- window_adjustment_missing: Не выполнена регулировка
- window_glazing_beads_missing: Отсутствие, повреждение штапиков
- window_trim_incorrect_mounting: Некорректный монтаж нащельников
- window_hardware_missing: Отсутствие, повреждение фурнитуры
- interior_door_adjustment_missing: Не выполнена регулировка дверного блока
- interior_door_surface_defects: Дефекты поверхности
- interior_door_hardware_adjustment: Не выполнена регулировка фурнитуры
- balcony_tile_steps_chips: Плитка пол-уступы, сколы
- balcony_paint_drips_stains: Пропуски, потеки, окрашивания стен и потолков
- balcony_tile_grout_issues: Плитка пол -пропуски, излишки затирки
- wallpaper_joints: Стыки
- wallpaper_peeling: Отслоения
- wallpaper_gluing_surface_defects: Дефекты поверхности
- wallpaper_glue_stains: Загрязнения, следы клея на поверхности
- wallpaper_overlap: Нахлест
- entrance_door_reinstall_needed: Демонтаж, монтаж двери
- entrance_door_adjustment_missing: Не выполнена регулировка
- entrance_door_trim_missing: Отсутствие примыкание доборов и наличников
- entrance_door_hardware_damage: Мех.повреждения фурнитуры и др.
- entrance_door_cleanliness: Чистота
- entrance_door_surface_defects: Дефекты поверхности
- entrance_door_opening_filling: Заполнение проемов
- entrance_door_locking_devices: Запирающие устройства
- baseboards_surface_defects: Дефекты поверхности
- threshold_steps: Уступы
- baseboards_floor_gaps: Зазоры полы
- baseboards_connecting_elements: Соединительные элементы
- baseboards_joint_overlap: Перекрытие швов
- baseboards_insufficient_fasteners: Недостаточное количество крепежей
- bath_screen_not_fixed: Не закреплен экран под ванну
- ceiling_paint_uniformity: Равномерность окраски
- ceiling_surface_defects: Дефекты поверхности
- inspection_hatch_door_adjustment: Регулировка дверцы люка
- inspection_hatch_vertical_deviation: Отклонение от вертикали
- inspection_hatch_surface_defects: Дефекты поверхности
- inspection_hatch_wall_gap: Зазор на стене
- floor_tile_voids: Пустоты
- floor_tile_layout_mismatch: Раскладка не соответствует проекту
- floor_tile_grout: Затирка
- floor_tile_unevenness: Неровности по плоскости более 4 мм на 2 м рейку
- floor_tile_joint_displacement: Смещение швов
- floor_tile_cracks_chips: Трещины и сколы
- floor_tile_joint_placement: Расположение швов
- floor_tile_steps: Уступы
- floor_tile_joint_width: Ширина швов
- floor_level_deviation: Отклонение уровня пола более 4 мм на 2 м
- stretch_ceiling_embedded_parts: Выпирание закладных деталей
- stretch_ceiling_contamination: Загрязнение полотна
- stretch_ceiling_baseboard_gap: Зазор между стеной и потолочным плинтусом
- stretch_ceiling_pipe_gap: Зазор у труб стояков отопления
- stretch_ceiling_sagging: Втягивание полотна потолка
- plumbing_leaks_malfunction: Протечки и неисправность
- plumbing_joint_sealing: Герметизация швов
- plumbing_surface_defects: Дефекты поверхности
- plumbing_mounting: Крепление
- plumbing_mechanical_damage: Механические повреждения
- plumbing_decorative_covers: Декоративные накладки
- wet_cleaning: Влажная уборка
- door_trim_connection_gaps: Зазор в соединениях
- door_trim_mounting: Крепление
- door_trim_wall_gaps: Зазор по стенам
- door_trim_surface_defects: Дефекты поверхности
- heating_pipes_paint_defects: Дефекты окраски труб отопления
- laminate_chips_scratches: Сколы, царапины, разнотон досок ламината
- laminate_board_gaps: Зазоры между досками ламината
- laminate_ruler_gap: Зазор между 2х метровой рейкой более 2мм
- laminate_steps: Уступы
- laminate_floor_level_deviation: Отклонение уровня пола более 4 мм на 2 м рейку
- laminate_wall_gap_missing: Отсутствует или менее 10 мм зазор между ламинатом и вертикальными конструкциями
- window_slopes_paint_uniformity: Равномерность окраски
- window_slopes_surface_defects: Дефекты поверхности
- wall_tile_joint_displacement: Смещение швов
- wall_tile_glue_residue: Остатки клея
- wall_tile_layout_mismatch: Раскладка не соответствует проекту
- wall_tile_unevenness: Неровности по плоскости более 2 мм
- wall_tile_grout: Затирка
- wall_tile_steps: Уступы более 1 мм
- wall_tile_voids: Пустоты
- wall_tile_hole_shapes: Формы отверстий
- wall_tile_cracks_chips: Трещины и сколы
- wall_tile_joint_width: Ширина швов
</defect_reference_mapping>

<page_context_instructions>
You will receive THREE text blocks:
1. PREV_PAGE_END - the END of the previous page (for context about current room/section, or if defect started there)
2. CURRENT_PAGE_TEXT - the TARGET page to extract defects from (extract ONLY from here)
3. NEXT_PAGE_START - the START of the next page (in case defect description continues there)

CRITICAL RULES:
- Extract defects ONLY from CURRENT_PAGE_TEXT
- Use PREV_PAGE_END only to understand: which room/section we are in, or to complete a defect that started on previous page
- Use NEXT_PAGE_START only to complete a defect description that is cut off at the end of current page
- Do NOT extract defects that are fully contained in PREV or NEXT context
- If CURRENT_PAGE_TEXT has no defects, return empty array: []
</page_context_instructions>

<response_format>
Return ONLY valid JSON array of defects. No markdown, no code blocks, no explanations, no wrapper object.
Return the array directly: [{...}, {...}]

Example response with defects:
[
  {
    "source_text": "Трещина в штукатурном слое стены длиной 0,5 м, шириной раскрытия до 1 мм. Нарушение требований СП 71.13330.2017 п. 7.2.13",
    "room": "Кухня",
    "location": "Стена",
    "defect": "wallpaper_surface_defects",
    "work_type": "Штукатурные работы"
  },
  {
    "source_text": "Отслоение керамической плитки пола площадью 0,3 м² в зоне входа. Нарушение требований СП 71.13330.2017 п. 8.4.5",
    "room": "Кухня",
    "location": "Пол",
    "defect": "floor_tile_voids",
    "work_type": "Плиточные работы"
  }
]

Example response if no defects found:
[]
</response_format>"""


# =============================================================================
# Pydantic-модели
# =============================================================================


class ExtractedDefect(BaseModel):
    """Один извлечённый дефект."""

    source_text: str = Field(..., description="Полный оригинальный текст дефекта из документа")
    room: str = Field(..., description="Помещение (Кухня, Коридор, Ванная...)")
    location: str = Field(..., description="Локализация (Стена, Пол, Потолок...)")
    defect: str = Field(..., description="Ключ дефекта из справочника")
    work_type: str = Field(..., description="Тип работы для устранения")
    page_number: int = Field(default=0, description="Номер страницы источника")


class DefectExtractionResult(BaseModel):
    """Результат извлечения дефектов из документа."""

    source_pdf: str = Field(..., description="Путь к исходному PDF")
    total_defects: int = Field(..., description="Общее количество найденных дефектов")
    pages_processed: int = Field(..., description="Количество обработанных страниц")
    defects: list[ExtractedDefect] = Field(default_factory=list, description="Список дефектов")
    elapsed_seconds: float = Field(default=0.0, description="Время обработки (сек)")


# =============================================================================
# Внутренние структуры
# =============================================================================


@dataclass(frozen=True)
class PageContext:
    """Контекст страницы для извлечения дефектов."""

    page_number: int
    text: str  # текст целевой страницы
    prev_text: str = ""  # конец предыдущей страницы
    next_text: str = ""  # начало следующей страницы


# =============================================================================
# Внутренние функции
# =============================================================================


def _trim_context(text: str, limit: int, *, from_end: bool) -> str:
    """Обрезает текст до limit символов с начала или конца."""
    cleaned = (text or "").strip()
    if not cleaned or limit <= 0:
        return ""
    return cleaned[-limit:] if from_end else cleaned[:limit]


def _build_page_contexts(
    pages: list[tuple[int, str]],
    context_chars: int,
) -> list[PageContext]:
    """Строит список PageContext с контекстом соседних страниц."""
    result: list[PageContext] = []
    n = len(pages)

    for i, (page_num, text) in enumerate(pages):
        prev_text = ""
        next_text = ""

        if i > 0:
            prev_text = _trim_context(pages[i - 1][1], context_chars, from_end=True)
        if i < n - 1:
            next_text = _trim_context(pages[i + 1][1], context_chars, from_end=False)

        result.append(
            PageContext(
                page_number=page_num,
                text=text,
                prev_text=prev_text,
                next_text=next_text,
            )
        )

    return result


def _build_user_message(ctx: PageContext) -> str:
    """Формирует user message для LLM с контекстом страницы."""
    parts = [
        f"PREV_PAGE_END:\n{ctx.prev_text}" if ctx.prev_text else "PREV_PAGE_END:\n(нет)",
        f"\nCURRENT_PAGE_NUMBER: {ctx.page_number}",
        f"CURRENT_PAGE_TEXT:\n{ctx.text}",
        f"\nNEXT_PAGE_START:\n{ctx.next_text}" if ctx.next_text else "\nNEXT_PAGE_START:\n(нет)",
    ]
    return "\n".join(parts)


def _parse_flowise_response(response_data: Any) -> list[dict]:
    """Парсит ответ Flowise и извлекает массив defects."""
    # Flowise может вернуть:
    # 1. {"text": "[{...}, {...}]"} — массив в текстовом поле (приоритет)
    # 2. {"json": {"source_text": ...}} — один объект в structured output
    # 3. {"defects": [...]} — напрямую
    # 4. строку с JSON

    raw_text = ""

    if isinstance(response_data, str):
        raw_text = response_data
    elif isinstance(response_data, dict):
        # Приоритет: поле "text" (там должен быть JSON массив)
        if "text" in response_data and response_data["text"]:
            raw_text = str(response_data["text"])
        # Проверяем поле "json" (structured output — один объект)
        elif "json" in response_data and isinstance(response_data["json"], dict):
            json_obj = response_data["json"]
            # Если это один дефект (не массив) — оборачиваем в массив
            if json_obj and "source_text" in json_obj:
                return [json_obj]
            if "defects" in json_obj:
                return list(json_obj["defects"])
            if json_obj:
                raw_text = json.dumps(json_obj)
        elif "defects" in response_data:
            return list(response_data["defects"])
        else:
            raw_text = json.dumps(response_data)

    # Удаляем markdown блоки если есть
    raw_text = raw_text.strip()
    if raw_text.startswith("```"):
        match = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw_text)
        if match:
            raw_text = match.group(1).strip()

    try:
        parsed = json.loads(raw_text)
        if isinstance(parsed, dict) and "defects" in parsed:
            return list(parsed["defects"])
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass

    return []


async def _call_flowise_extract(ctx: PageContext) -> list[ExtractedDefect]:
    """Отправляет страницу в Flowise и возвращает список дефектов."""
    user_message = _build_user_message(ctx)

    # DEBUG: показать текст страницы
    logger.info(
        "Страница %d: текст %d символов, prev %d, next %d",
        ctx.page_number,
        len(ctx.text),
        len(ctx.prev_text),
        len(ctx.next_text),
    )
    if len(ctx.text) < 100:
        logger.warning("Страница %d: КОРОТКИЙ ТЕКСТ: %s", ctx.page_number, ctx.text[:200])

    payload = {
        "question": f"{DEFECT_EXTRACTION_PROMPT}\n\n{user_message}",
    }

    last_error: Exception | None = None

    for attempt in range(DEFECT_EXTRACTION_MAX_RETRIES):
        try:
            async with httpx.AsyncClient(timeout=DEFECT_EXTRACTION_TIMEOUT_SECONDS) as client:
                logger.debug(
                    "Defect extraction: страница %d, попытка %d/%d",
                    ctx.page_number,
                    attempt + 1,
                    DEFECT_EXTRACTION_MAX_RETRIES,
                )

                response = await client.post(
                    FLOWISE_API_URL_DEFECT_EXTRACT,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                response.raise_for_status()

                data = response.json()

                # DEBUG: показать сырой ответ Flowise
                logger.debug("Flowise raw response (page %d): %s", ctx.page_number, data)

                raw_defects = _parse_flowise_response(data)

                # DEBUG: показать распарсенные дефекты
                if not raw_defects:
                    logger.warning(
                        "Страница %d: Flowise вернул пустой список. Raw: %s",
                        ctx.page_number,
                        str(data)[:500],
                    )

                defects: list[ExtractedDefect] = []
                for d in raw_defects:
                    try:
                        defect = ExtractedDefect(
                            source_text=str(d.get("source_text", "")),
                            room=str(d.get("room", "Комната")),
                            location=str(d.get("location", "")),
                            defect=str(d.get("defect", "")),
                            work_type=str(d.get("work_type", "")),
                            page_number=ctx.page_number,
                        )
                        defects.append(defect)
                    except Exception as e:
                        logger.warning("Не удалось распарсить дефект: %s, ошибка: %s", d, e)

                logger.info(
                    "Страница %d: найдено %d дефектов (попытка %d)",
                    ctx.page_number,
                    len(defects),
                    attempt + 1,
                )
                return defects

        except httpx.TimeoutException as e:
            last_error = e
            logger.warning(
                "Timeout страница %d, попытка %d/%d",
                ctx.page_number,
                attempt + 1,
                DEFECT_EXTRACTION_MAX_RETRIES,
            )
        except httpx.HTTPStatusError as e:
            last_error = e
            if e.response.status_code in (429, 500, 502, 503, 504):
                logger.warning(
                    "HTTP %d страница %d, попытка %d/%d",
                    e.response.status_code,
                    ctx.page_number,
                    attempt + 1,
                    DEFECT_EXTRACTION_MAX_RETRIES,
                )
            else:
                raise
        except (httpx.ConnectError, httpx.ReadError) as e:
            last_error = e
            logger.warning(
                "Connection error страница %d: %s, попытка %d/%d",
                ctx.page_number,
                e,
                attempt + 1,
                DEFECT_EXTRACTION_MAX_RETRIES,
            )

        if attempt < DEFECT_EXTRACTION_MAX_RETRIES - 1:
            delay = DEFECT_EXTRACTION_RETRY_DELAY_SECONDS * (2**attempt)
            await asyncio.sleep(delay)

    logger.error(
        "Все попытки исчерпаны для страницы %d: %s",
        ctx.page_number,
        last_error,
    )
    return []


# =============================================================================
# Публичный API
# =============================================================================


async def extract_defects(
    vlm_result: VLMCleaningResult,
    context_chars: int | None = None,
) -> DefectExtractionResult:
    """Извлекает дефекты из VLM-очищенных страниц.

    Args:
        vlm_result: Результат VLM очистки страниц
        context_chars: Сколько символов брать из соседних страниц (default из config)

    Returns:
        DefectExtractionResult со списком дефектов
    """
    if not vlm_result.cleaned_pages:
        return DefectExtractionResult(
            source_pdf=vlm_result.source_pdf,
            total_defects=0,
            pages_processed=0,
            defects=[],
            elapsed_seconds=0.0,
        )

    context_chars = context_chars or DEFECT_EXTRACTION_CONTEXT_CHARS

    # Собираем страницы в список (page_number, text)
    pages = [
        (p.page_number, p.cleaned_text)
        for p in sorted(vlm_result.cleaned_pages, key=lambda x: x.page_number)
    ]

    logger.info(
        "Извлечение дефектов: %d страниц, контекст %d символов",
        len(pages),
        context_chars,
    )

    start_time = time.perf_counter()

    # Строим контексты
    contexts = _build_page_contexts(pages, context_chars)

    # Параллельная обработка
    semaphore = asyncio.Semaphore(DEFECT_EXTRACTION_CONCURRENCY)

    async def process_one(ctx: PageContext) -> list[ExtractedDefect]:
        async with semaphore:
            return await _call_flowise_extract(ctx)

    results = await asyncio.gather(*(process_one(ctx) for ctx in contexts))

    # Собираем все дефекты
    all_defects: list[ExtractedDefect] = []
    for defects in results:
        all_defects.extend(defects)

    elapsed = time.perf_counter() - start_time

    result = DefectExtractionResult(
        source_pdf=vlm_result.source_pdf,
        total_defects=len(all_defects),
        pages_processed=len(pages),
        defects=all_defects,
        elapsed_seconds=round(elapsed, 2),
    )

    logger.info(
        "Извлечение завершено: %d дефектов из %d страниц за %.2f сек",
        len(all_defects),
        len(pages),
        elapsed,
    )

    return result


async def save_extraction_result(
    result: DefectExtractionResult,
    result_dir: str | Path = "artifacts/defects",
) -> str:
    """Сохраняет результат извлечения в JSON файл.

    Args:
        result: Результат извлечения дефектов
        result_dir: Папка для сохранения

    Returns:
        Путь к сохранённому JSON файлу
    """
    result_path = Path(result_dir).expanduser().resolve()
    result_path.mkdir(parents=True, exist_ok=True)

    pdf_stem = Path(result.source_pdf).stem
    json_file = result_path / f"defects_{pdf_stem}.json"

    logger.info("Сохраняю результат извлечения: %s", json_file)

    json_file.write_text(result.model_dump_json(indent=2), encoding="utf-8")

    logger.info("Результат сохранён: %d дефектов", result.total_defects)
    return str(json_file)
