"""Сервис VLM очистки и структурирования страниц PDF через Flowise Vision API.

Принимает PDF и список релевантных страниц, отправляет изображения в Flowise VLM,
возвращает очищенный Markdown-текст.

Публичный API:
    - clean_relevant_pages() — основная async функция обработки
    - save_vlm_result() — сохранение результата в JSON и TXT
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
import time
from io import BytesIO
from pathlib import Path
from typing import Any

import httpx
from pdf2image import convert_from_path
from PIL import Image
from pydantic import BaseModel, Field

from config import (
    logger,
    FLOWISE_API_URL_VLM_CLEAN,
    VLM_RENDER_DPI,
    VLM_IMAGE_TARGET_WIDTH,
    VLM_IMAGE_TARGET_HEIGHT,
    VLM_IMAGE_JPEG_QUALITY,
    VLM_MAX_RETRIES,
    VLM_RETRY_BASE_DELAY_SECONDS,
    VLM_TIMEOUT_SECONDS,
)


# =============================================================================
# Промпт для VLM очистки страниц
# =============================================================================

VLM_CLEAN_PROMPT = (
    "Это страница технического отчёта о дефектах ремонта помещений. "
    "Извлеки и приведи текст в аккуратную СТРУКТУРУ, сохранив порядок, "
    "пункты, нумерацию, ЗАГОЛОВКИ/ПОДЗАГОЛОВКИ если таковые имются "
    "и каждую техническую деталь. "
    "Сохраняй структуру, как на изображении: если видишь заголовок/подзаголовок — выделяй его, "
    "если видишь таблицу — оформи в Markdown-таблицу.\n\n"
    "Форматирование:\n"
    "- Используй Markdown-синтаксис: заголовки (#/##/###), таблицы (|...|) и так далее.\n"
    "- Заголовки разделов оставляй на отдельных строках.\n"
    "- Ничего не сокращай и не опускай, не добавляй комментариев.\n"
    "- Не оборачивай текст в ``` (кодблоки) — это должна быть строка.\n\n"
    "Ответ верни строго в JSON объекте с одним ключом `cleaned_text` (строка с Markdown внутри). "
    "Никаких других ключей и никакого текста вне JSON."
)


# =============================================================================
# Pydantic-модели результата
# =============================================================================


class CleanedPageData(BaseModel):
    """Результат VLM обработки одной страницы."""

    page_number: int = Field(..., description="Номер страницы в PDF (1-based)")
    cleaned_text: str = Field(default="", description="Очищенный текст в Markdown")


class VLMCleaningResult(BaseModel):
    """Результат VLM обработки всего документа."""

    source_pdf: str = Field(..., description="Путь к исходному PDF")
    processed_pages: int = Field(..., description="Количество обработанных страниц")
    cleaned_pages: list[CleanedPageData] = Field(
        default_factory=list, description="Список очищенных страниц"
    )
    elapsed_seconds: float = Field(default=0.0, description="Время обработки (сек)")

    def get_all_text(self, separator: str = "\n\n") -> str:
        """Возвращает объединённый текст всех страниц."""
        return separator.join(
            p.cleaned_text for p in self.cleaned_pages if p.cleaned_text
        )


# =============================================================================
# Внутренние функции
# =============================================================================


def _preprocess_page_image(image: Image.Image) -> Image.Image:
    """Приводит изображение к единому размеру (letterbox) и RGB."""
    img = image.convert("RGB")
    target_w, target_h = VLM_IMAGE_TARGET_WIDTH, VLM_IMAGE_TARGET_HEIGHT

    # Масштабируем с сохранением пропорций
    img.thumbnail((target_w, target_h), Image.Resampling.LANCZOS)

    # Создаём белый холст и центрируем изображение
    canvas = Image.new("RGB", (target_w, target_h), color=(255, 255, 255))
    left = (target_w - img.width) // 2
    top = (target_h - img.height) // 2
    canvas.paste(img, (left, top))
    return canvas


def _encode_image_base64(image: Image.Image) -> tuple[str, str]:
    """Кодирует изображение в JPEG base64. Возвращает (mime_type, base64_string)."""
    buffer = BytesIO()
    image.save(buffer, format="JPEG", quality=int(VLM_IMAGE_JPEG_QUALITY), optimize=True)
    return "image/jpeg", base64.b64encode(buffer.getvalue()).decode("utf-8")


def _convert_pdf_page_to_base64(pdf_path: Path, page_number: int) -> tuple[str, str]:
    """Конвертирует страницу PDF в base64 изображение (после letterbox).

    Args:
        pdf_path: Путь к PDF файлу
        page_number: Номер страницы (1-based)

    Returns:
        (mime_type, base64_string)
    """
    images = convert_from_path(
        str(pdf_path),
        first_page=page_number,
        last_page=page_number,
        fmt="png",
        dpi=int(VLM_RENDER_DPI),
    )

    if not images:
        raise RuntimeError(f"Не удалось получить страницу {page_number} из {pdf_path}")

    processed = _preprocess_page_image(images[0])
    mime_type, image_base64 = _encode_image_base64(processed)

    logger.debug(
        "Страница %s → base64 (%dx%d, dpi=%d)",
        page_number,
        VLM_IMAGE_TARGET_WIDTH,
        VLM_IMAGE_TARGET_HEIGHT,
        VLM_RENDER_DPI,
    )
    return mime_type, image_base64


def _parse_vlm_response(response_data: Any) -> str:
    """Извлекает cleaned_text из ответа Flowise.

    Flowise может возвращать:
    - {"text": "...json или markdown..."} — строка с JSON внутри
    - {"cleaned_text": "..."} — напрямую
    - просто строку
    """
    if isinstance(response_data, str):
        # Попробуем распарсить как JSON
        try:
            parsed = json.loads(response_data)
            return str(parsed.get("cleaned_text", response_data)).strip()
        except json.JSONDecodeError:
            return response_data.strip()

    if isinstance(response_data, dict):
        # Если есть "text" — это обёртка Flowise
        if "text" in response_data and isinstance(response_data["text"], str):
            text = response_data["text"]

            # Удаляем markdown-блоки ```json ... ```
            match = re.search(r"```json\s*([\s\S]*?)```", text, re.IGNORECASE)
            raw = match.group(1) if match else text

            try:
                parsed = json.loads(raw)
                return str(parsed.get("cleaned_text", raw)).strip()
            except json.JSONDecodeError:
                return raw.strip()

        # Прямой ответ с cleaned_text
        if "cleaned_text" in response_data:
            return str(response_data["cleaned_text"]).strip()

    return ""


async def _call_flowise_vlm(
    mime_type: str,
    image_base64: str,
    page_number: int,
) -> str:
    """Отправляет изображение страницы в Flowise VLM API.

    Args:
        mime_type: MIME тип изображения (image/jpeg)
        image_base64: Base64-строка изображения
        page_number: Номер страницы (для логов и промпта)

    Returns:
        Очищенный текст страницы
    """
    prompt = f"{VLM_CLEAN_PROMPT}\nСтраница: {page_number}."

    payload = {
        "question": prompt,
        "uploads": [
            {
                "data": f"data:{mime_type};base64,{image_base64}",
                "type": "file",
                "name": f"page_{page_number}.jpg",
                "mime": mime_type,
            }
        ],
    }

    last_error: Exception | None = None

    for attempt in range(VLM_MAX_RETRIES):
        try:
            async with httpx.AsyncClient(timeout=VLM_TIMEOUT_SECONDS) as client:
                logger.debug(
                    "VLM запрос: страница %d, попытка %d/%d",
                    page_number,
                    attempt + 1,
                    VLM_MAX_RETRIES,
                )

                response = await client.post(
                    FLOWISE_API_URL_VLM_CLEAN,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                response.raise_for_status()

                data = response.json()
                cleaned_text = _parse_vlm_response(data)

                logger.info(
                    "Страница %d обработана VLM (попытка %d, %d символов)",
                    page_number,
                    attempt + 1,
                    len(cleaned_text),
                )
                return cleaned_text

        except httpx.TimeoutException as e:
            last_error = e
            logger.warning(
                "VLM timeout страница %d, попытка %d/%d",
                page_number,
                attempt + 1,
                VLM_MAX_RETRIES,
            )
        except httpx.HTTPStatusError as e:
            last_error = e
            # 429 (rate limit) или 5xx — retry
            if e.response.status_code in (429, 500, 502, 503, 504):
                logger.warning(
                    "VLM HTTP %d страница %d, попытка %d/%d",
                    e.response.status_code,
                    page_number,
                    attempt + 1,
                    VLM_MAX_RETRIES,
                )
            else:
                raise
        except (httpx.ConnectError, httpx.ReadError) as e:
            last_error = e
            logger.warning(
                "VLM connection error страница %d: %s, попытка %d/%d",
                page_number,
                e,
                attempt + 1,
                VLM_MAX_RETRIES,
            )

        # Exponential backoff
        if attempt < VLM_MAX_RETRIES - 1:
            delay = VLM_RETRY_BASE_DELAY_SECONDS * (2**attempt)
            logger.debug("Ожидание %d сек перед retry...", delay)
            await asyncio.sleep(delay)

    # Все попытки исчерпаны
    raise RuntimeError(
        f"VLM: все {VLM_MAX_RETRIES} попыток исчерпаны для страницы {page_number}"
    ) from last_error


# =============================================================================
# Публичный API
# =============================================================================


async def clean_relevant_pages(
    pdf_path: str | Path,
    page_numbers: list[int],
    raw_text_by_page: dict[int, str] | None = None,
) -> VLMCleaningResult:
    """Обрабатывает список страниц PDF через Flowise VLM.

    Args:
        pdf_path: Путь к PDF файлу
        page_numbers: Список номеров страниц для обработки (1-based)
        raw_text_by_page: Опциональный fallback — сырой OCR текст по номерам страниц.
                          Используется если VLM не смог обработать страницу.

    Returns:
        VLMCleaningResult с очищенными страницами
    """
    pdf_path = Path(pdf_path).expanduser().resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF не найден: {pdf_path}")

    if not page_numbers:
        raise ValueError("Не переданы номера страниц для VLM обработки")

    # Убираем дубликаты и сортируем
    ordered_pages = sorted(set(page_numbers))

    logger.info(
        "VLM обработка: %d страниц из %s → %s",
        len(ordered_pages),
        pdf_path.name,
        ordered_pages,
    )

    start_time = time.perf_counter()
    cleaned_pages: list[CleanedPageData] = []
    raw_text_by_page = raw_text_by_page or {}

    for page_num in ordered_pages:
        cleaned_text = ""

        try:
            # Конвертируем страницу в base64
            mime_type, image_base64 = _convert_pdf_page_to_base64(pdf_path, page_num)

            # Отправляем в VLM
            cleaned_text = await _call_flowise_vlm(mime_type, image_base64, page_num)

            logger.info("Страница %d успешно очищена", page_num)

        except Exception as e:
            # Fallback на сырой OCR текст
            fallback = raw_text_by_page.get(page_num, "")
            cleaned_text = fallback
            logger.warning(
                "VLM не обработал страницу %d, fallback на OCR (len=%d). Ошибка: %s",
                page_num,
                len(fallback),
                e,
            )

        cleaned_pages.append(
            CleanedPageData(page_number=page_num, cleaned_text=cleaned_text)
        )

    elapsed = time.perf_counter() - start_time

    result = VLMCleaningResult(
        source_pdf=str(pdf_path),
        processed_pages=len(cleaned_pages),
        cleaned_pages=cleaned_pages,
        elapsed_seconds=round(elapsed, 2),
    )

    logger.info(
        "VLM обработка завершена: %d страниц за %.2f сек",
        len(cleaned_pages),
        elapsed,
    )
    return result


async def save_vlm_result(
    vlm_result: VLMCleaningResult,
    result_dir: str | Path = "artifacts/vlm",
) -> tuple[str, str]:
    """Сохраняет результат VLM обработки в JSON и TXT файлы.

    Args:
        vlm_result: Результат VLM обработки
        result_dir: Папка для сохранения

    Returns:
        (json_path, txt_path) — пути к сохранённым файлам
    """
    result_path = Path(result_dir).expanduser().resolve()
    result_path.mkdir(parents=True, exist_ok=True)

    pdf_stem = Path(vlm_result.source_pdf).stem
    json_file = result_path / f"vlm_result_{pdf_stem}.json"
    txt_file = result_path / f"vlm_cleaned_{pdf_stem}.txt"

    logger.info("Сохраняю VLM результат: %s, %s", json_file, txt_file)

    # JSON с полной структурой
    json_file.write_text(vlm_result.model_dump_json(indent=2), encoding="utf-8")

    # TXT с очищенным текстом
    lines: list[str] = []
    for page in vlm_result.cleaned_pages:
        lines.append(f"=== Страница {page.page_number} (VLM) ===")
        lines.append(page.cleaned_text)
        lines.append("")

    txt_file.write_text("\n".join(lines), encoding="utf-8")

    logger.info("VLM результаты сохранены")
    return str(json_file), str(txt_file)
