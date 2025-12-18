"""OCR сервис проекта FSK_pred_prod (пока только tesseract).

Принцип: сервис выполняет OCR и возвращает структурированный результат в одном формате.
Модели держим в этом же файле (как вы просили), чтобы на старте было проще переносить/собирать.

Важно:
- Настройки берём ТОЛЬКО из `config.py` (config = источник правды).
- Внутри сервиса настройки не переопределяем.
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from config import (
    logger,
    OCR_KEEP_PREPROCESS_WORKDIR,
    OCR_PAGE_CONCURRENCY,
    TESSERACT_LANG,
    TESSERACT_OEM,
    TESSERACT_PSM,
    TESSERACT_PRESERVE_INTERWORD_SPACES,
    TESSERACT_PAGE_TIMEOUT_SECONDS,
)
from services.pdf_preprocessor import PreprocessedPage, preprocess_pdf_to_images


# -----------------------------
# Pydantic модели результата OCR
# -----------------------------
class TextElement(BaseModel):
    """Текстовый элемент страницы (минимальная структура для дальнейшего пайплайна)."""

    category: str = Field(..., description="Категория элемента (например: 'ocr').")
    content: str = Field(..., description="Текст элемента.")
    type: Literal["text"] = Field(default="text", description="Тип элемента (фиксированный).")


class PageData(BaseModel):
    """Данные одной страницы документа после OCR."""

    page_number: int = Field(..., description="Номер страницы (1..N).")
    full_text: str = Field(..., description="Полный текст страницы.")
    elements: list[TextElement] = Field(
        default_factory=list,
        description="Список элементов текста (сейчас один элемент на страницу).",
    )
    total_elements: int = Field(..., description="Количество элементов на странице.")


class DocumentData(BaseModel):
    """Результат OCR по всему документу."""

    filename: str = Field(..., description="Имя исходного файла (для идентификации).")
    pages: list[PageData] = Field(default_factory=list, description="Страницы документа с OCR-текстом.")
    total_pages: int = Field(..., description="Количество страниц в документе.")

    def get_all_text(self) -> str:
        """Склеивает документ в удобный .txt формат (как артефакт для отладки/поиска)."""
        chunks: list[str] = []
        for page in self.pages:
            chunks.append(f"=== Страница {page.page_number} ===\n{page.full_text}".rstrip())
        return "\n\n".join(chunks).rstrip() + "\n"


class OCRResult(BaseModel):
    """Результат выполнения OCR с метаданными для дальнейшего пайплайна."""

    pdf_path: Path = Field(..., description="Путь к исходному PDF.")
    seconds: float = Field(..., description="Длительность выполнения OCR в секундах.")
    document: DocumentData = Field(..., description="Структурированный OCR-результат.")
    preprocess_workdir: Path = Field(..., description="Workdir предпроцессинга (может быть уже удалён).")
    preprocess_workdir_kept: bool = Field(..., description="True если workdir был оставлен на диске.")


# -----------------------------
# Внутренние функции OCR
# -----------------------------
def _ensure_pdf_file(pdf_path: str | Path) -> Path:
    pdf = Path(pdf_path).expanduser().resolve()
    if not pdf.exists():
        raise FileNotFoundError(f"PDF не найден: {pdf}")
    if not pdf.is_file():
        raise IsADirectoryError(f"Ожидался файл PDF, но получено: {pdf}")
    return pdf


def _ensure_tesseract_available() -> None:
    """Проверяет, что бинарь `tesseract` доступен в PATH."""
    if not shutil.which("tesseract"):
        raise RuntimeError("Не найден бинарь `tesseract` в PATH. Установите tesseract-ocr и добавьте в PATH.")


def _tesseract_ocr_image(*, image_path: Path) -> str:
    """Запускает tesseract для одного изображения страницы и возвращает распознанный текст."""
    cmd = [
        "tesseract",
        str(image_path),
        "stdout",
        "-l",
        str(TESSERACT_LANG),
        "--oem",
        str(int(TESSERACT_OEM)),
        "--psm",
        str(int(TESSERACT_PSM)),
        "-c",
        f"preserve_interword_spaces={int(TESSERACT_PRESERVE_INTERWORD_SPACES)}",
    ]

    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            check=False,
            timeout=float(TESSERACT_PAGE_TIMEOUT_SECONDS),
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except subprocess.TimeoutExpired as e:
        raise TimeoutError(
            f"Tesseract timeout after {TESSERACT_PAGE_TIMEOUT_SECONDS}s: {image_path.name}"
        ) from e

    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        raise RuntimeError(f"Tesseract завершился с ошибкой (code={completed.returncode}): {stderr}")

    return completed.stdout or ""


def _normalize_ocr_text(text: str) -> str:
    # Не используем strip(): ведущие пробелы могут быть полезны для таблиц/формата.
    return (text or "").replace("\r\n", "\n").rstrip()


def _normalize_concurrency(concurrency: int | None) -> int:
    if concurrency is None:
        value = int(OCR_PAGE_CONCURRENCY)
    else:
        value = int(concurrency)
    return value if value > 0 else 1


def _build_document_from_page_texts(filename: str, page_texts: list[str]) -> DocumentData:
    """Собирает `DocumentData` из списка строк по страницам."""
    pages: list[PageData] = []
    for page_number, text in enumerate(page_texts, start=1):
        normalized = _normalize_ocr_text(text)
        elements: list[TextElement] = []
        if normalized:
            elements.append(TextElement(category="ocr", content=normalized, type="text"))

        pages.append(
            PageData(
                page_number=page_number,
                full_text=normalized,
                elements=elements,
                total_elements=len(elements),
            )
        )

    return DocumentData(filename=filename, pages=pages, total_pages=len(pages))


def _ocr_pdf_sync(pdf_path: str | Path, *, max_pages: int | None, concurrency: int | None) -> OCRResult:
    """Синхронная реализация OCR: PDF -> preprocess -> tesseract per page."""
    _ensure_tesseract_available()

    started_at = time.perf_counter()
    pdf = _ensure_pdf_file(pdf_path)
    max_workers = _normalize_concurrency(concurrency)

    logger.info("OCR старт: pdf=%s, size_bytes=%s, max_pages=%s", pdf.name, pdf.stat().st_size, max_pages)
    logger.info(
        "OCR конфиг: lang=%s, oem=%s, psm=%s, preserve_spaces=%s, page_timeout=%ss",
        TESSERACT_LANG,
        TESSERACT_OEM,
        TESSERACT_PSM,
        TESSERACT_PRESERVE_INTERWORD_SPACES,
        TESSERACT_PAGE_TIMEOUT_SECONDS,
    )
    logger.info("OCR параллельность: page_concurrency=%s", max_workers)

    preprocessed = preprocess_pdf_to_images(pdf, max_pages=max_pages)
    logger.info("OCR: предпроцессинг готов, pages=%s, workdir=%s", len(preprocessed.pages), preprocessed.workdir)

    total_pages = len(preprocessed.pages)
    keep_workdir = bool(OCR_KEEP_PREPROCESS_WORKDIR)

    try:
        texts_by_page: dict[int, str] = {}
        future_meta: dict[Future[str], tuple[PreprocessedPage, float]] = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for page in preprocessed.pages:
                logger.info(
                    "OCR старт страницы %s/%s: preprocessed=%s",
                    page.page_number,
                    total_pages,
                    page.preprocessed_path.name,
                )
                future = executor.submit(_tesseract_ocr_image, image_path=page.preprocessed_path)
                future_meta[future] = (page, time.perf_counter())

            for future in as_completed(future_meta):
                page, submitted_at = future_meta[future]
                page_number = page.page_number
                page_name = page.preprocessed_path.name
                try:
                    text = future.result()
                except Exception as e:
                    raise RuntimeError(f"OCR ошибка на странице {page_number}/{total_pages}: {page_name}") from e

                texts_by_page[int(page_number)] = text
                logger.info(
                    "OCR страница %s/%s готова: chars=%s, seconds=%.2f",
                    page_number,
                    total_pages,
                    len(text),
                    time.perf_counter() - float(submitted_at),
                )

        page_texts = [texts_by_page[i] for i in range(1, total_pages + 1)]

        document = _build_document_from_page_texts(pdf.name, page_texts)
        duration = time.perf_counter() - started_at
        logger.info("OCR завершён: pages=%s, seconds=%.2f", document.total_pages, duration)
        return OCRResult(
            pdf_path=pdf,
            seconds=duration,
            document=document,
            preprocess_workdir=preprocessed.workdir,
            preprocess_workdir_kept=keep_workdir,
        )
    finally:
        if keep_workdir:
            logger.info("OCR: workdir сохранён для отладки: %s", preprocessed.workdir)
        else:
            try:
                preprocessed.cleanup()
                logger.info("OCR: workdir удалён: %s", preprocessed.workdir)
            except Exception:
                logger.warning("OCR: не удалось удалить workdir: %s", preprocessed.workdir, exc_info=True)


# -----------------------------
# Публичный API сервиса
# -----------------------------
async def process_pdf_ocr(
    pdf_path: str | Path, *, max_pages: int | None = None, concurrency: int | None = None
) -> OCRResult:
    """Асинхронный wrapper для OCR.

    Args:
        pdf_path: путь к PDF
        max_pages: ограничение по страницам для тестов (None = все)

    Returns:
        OCRResult
    """
    pdf = _ensure_pdf_file(pdf_path)
    return await asyncio.to_thread(_ocr_pdf_sync, pdf, max_pages=max_pages, concurrency=concurrency)


def _document_to_json(document: DocumentData) -> str:
    # Совместимость pydantic v1/v2, чтобы не зависеть от конкретной версии.
    if hasattr(document, "model_dump_json"):
        return document.model_dump_json(indent=2)  # type: ignore[attr-defined]
    return document.json(indent=2, ensure_ascii=False)


async def save_ocr_result(result: OCRResult, *, result_dir: str | Path) -> tuple[Path, Path]:
    """Сохраняет результат OCR в JSON и TXT (артефакты для проверки человеком).

    Args:
        result: результат OCR
        result_dir: директория, куда сохранять

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
