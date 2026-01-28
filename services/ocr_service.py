"""OCR сервис проекта FSK_pred_prod.

OCR выполняется через внешний OCR Worker (FastAPI на хосте).
Это даёт ~10x ускорение по сравнению с tesseract внутри Docker.

Модели данных (TextElement, PageData, DocumentData) дублируются здесь
для независимости от OCR Worker.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from config import (
    logger,
    OCR_WORKER_TIMEOUT_SECONDS,
    OCR_WORKER_URL,
)


# -----------------------------
# Pydantic модели результата OCR
# -----------------------------
class TextElement(BaseModel):
    """Текстовый элемент страницы."""

    category: str = Field(..., description="Категория элемента (например: 'ocr').")
    content: str = Field(..., description="Текст элемента.")
    type: Literal["text"] = Field(default="text", description="Тип элемента.")


class PageData(BaseModel):
    """Данные одной страницы документа после OCR."""

    page_number: int = Field(..., description="Номер страницы (1..N).")
    full_text: str = Field(..., description="Полный текст страницы.")
    elements: list[TextElement] = Field(
        default_factory=list,
        description="Список элементов текста.",
    )
    total_elements: int = Field(..., description="Количество элементов на странице.")


class DocumentData(BaseModel):
    """Результат OCR по всему документу."""

    filename: str = Field(..., description="Имя исходного файла.")
    pages: list[PageData] = Field(default_factory=list, description="Страницы документа с OCR-текстом.")
    total_pages: int = Field(..., description="Количество страниц в документе.")

    def get_all_text(self) -> str:
        """Склеивает документ в удобный .txt формат."""
        chunks: list[str] = []
        for page in self.pages:
            chunks.append(f"=== Страница {page.page_number} ===\n{page.full_text}".rstrip())
        return "\n\n".join(chunks).rstrip() + "\n"


class OCRResult(BaseModel):
    """Результат выполнения OCR с метаданными."""

    pdf_path: Path = Field(..., description="Путь к исходному PDF.")
    seconds: float = Field(..., description="Длительность выполнения OCR в секундах.")
    document: DocumentData = Field(..., description="Структурированный OCR-результат.")


# -----------------------------
# Внутренние функции
# -----------------------------
def _ensure_pdf_file(pdf_path: str | Path) -> Path:
    """Проверяет существование PDF файла."""
    pdf = Path(pdf_path).expanduser().resolve()
    if not pdf.exists():
        raise FileNotFoundError(f"PDF не найден: {pdf}")
    if not pdf.is_file():
        raise IsADirectoryError(f"Ожидался файл PDF, но получено: {pdf}")
    return pdf


async def _call_ocr_worker(pdf: Path, *, max_pages: int | None) -> OCRResult:
    """Выполняет OCR через OCR Worker.

    Args:
        pdf: путь к PDF файлу
        max_pages: ограничение по страницам

    Returns:
        OCRResult
    """
    try:
        import httpx
    except ImportError as e:
        raise RuntimeError("Для OCR нужен пакет httpx: pip install httpx") from e

    started_at = time.perf_counter()
    ocr_url = f"{OCR_WORKER_URL.rstrip('/')}/ocr"

    logger.info(
        "OCR старт: pdf=%s, size=%s bytes, url=%s, max_pages=%s",
        pdf.name, pdf.stat().st_size, ocr_url, max_pages
    )

    # Параметры запроса
    params = {}
    if max_pages is not None:
        params["max_pages"] = max_pages

    try:
        async with httpx.AsyncClient(timeout=float(OCR_WORKER_TIMEOUT_SECONDS)) as client:
            with open(pdf, "rb") as f:
                files = {"file": (pdf.name, f, "application/pdf")}
                response = await client.post(ocr_url, files=files, params=params)

        response.raise_for_status()
        data = response.json()

    except httpx.TimeoutException as e:
        raise TimeoutError(f"OCR Worker timeout ({OCR_WORKER_TIMEOUT_SECONDS}s): {ocr_url}") from e
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"OCR Worker HTTP error: {e.response.status_code} {e.response.text}") from e
    except httpx.RequestError as e:
        raise RuntimeError(f"OCR Worker connection error: {e}") from e

    # Проверяем успешность
    if not data.get("success"):
        error_msg = data.get("error", "Unknown error")
        raise RuntimeError(f"OCR Worker error: {error_msg}")

    # Парсим DocumentData из ответа
    doc_data = data.get("document", {})
    pages: list[PageData] = []
    for page_dict in doc_data.get("pages", []):
        elements: list[TextElement] = []
        for elem_dict in page_dict.get("elements", []):
            elements.append(TextElement(
                category=elem_dict.get("category", "ocr"),
                content=elem_dict.get("content", ""),
                type=elem_dict.get("type", "text"),
            ))
        pages.append(PageData(
            page_number=page_dict.get("page_number", 0),
            full_text=page_dict.get("full_text", ""),
            elements=elements,
            total_elements=page_dict.get("total_elements", len(elements)),
        ))

    document = DocumentData(
        filename=doc_data.get("filename", pdf.name),
        pages=pages,
        total_pages=doc_data.get("total_pages", len(pages)),
    )

    duration = time.perf_counter() - started_at
    remote_duration = data.get("seconds", 0.0)

    logger.info(
        "OCR завершён: pages=%s, worker_time=%.2fs, total_time=%.2fs",
        document.total_pages, remote_duration, duration
    )

    return OCRResult(
        pdf_path=pdf,
        seconds=duration,
        document=document,
    )


# -----------------------------
# Публичный API сервиса
# -----------------------------
async def process_pdf_ocr(
    pdf_path: str | Path, *, max_pages: int | None = None
) -> OCRResult:
    """Выполняет OCR документа через OCR Worker.

    Args:
        pdf_path: путь к PDF
        max_pages: ограничение по страницам (None = все)

    Returns:
        OCRResult
    """
    pdf = _ensure_pdf_file(pdf_path)
    return await _call_ocr_worker(pdf, max_pages=max_pages)


def _document_to_json(document: DocumentData) -> str:
    """Сериализует DocumentData в JSON."""
    if hasattr(document, "model_dump_json"):
        return document.model_dump_json(indent=2)
    return document.json(indent=2, ensure_ascii=False)  # pydantic v1 fallback


async def save_ocr_result(result: OCRResult, *, result_dir: str | Path) -> tuple[Path, Path]:
    """Сохраняет результат OCR в JSON и TXT.

    Args:
        result: результат OCR
        result_dir: директория для сохранения

    Returns:
        (json_path, txt_path)
    """
    out_dir = Path(result_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    stem = Path(result.document.filename).stem
    json_path = out_dir / f"ocr_result_{stem}.json"
    txt_path = out_dir / f"full_text_{stem}.txt"

    json_text = _document_to_json(result.document)
    txt_text = result.document.get_all_text()

    def _write() -> None:
        json_path.write_text(json_text, encoding="utf-8")
        txt_path.write_text(txt_text, encoding="utf-8")

    logger.info("OCR: сохраняю артефакты: json=%s, txt=%s", json_path.name, txt_path.name)
    await asyncio.to_thread(_write)
    logger.info("OCR: артефакты сохранены: %s", out_dir)
    return json_path, txt_path
