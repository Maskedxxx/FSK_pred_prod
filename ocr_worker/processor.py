"""Логика OCR обработки для OCR Worker.

Содержит функции для предпроцессинга PDF и распознавания через tesseract.
Адаптировано из services/ocr_service.py для standalone работы.
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING

import cv2
from pdf2image import convert_from_path

from ocr_worker.config import (
    logger,
    OCR_PAGE_CONCURRENCY,
    PDF_PREPROCESS_NORMALIZE,
    PDF_PREPROCESS_OUTPUT_FORMAT,
    PDF_RENDER_DPI,
    PDF_RENDER_FORMAT,
    PDF_RENDER_THREAD_COUNT,
    TESSERACT_LANG,
    TESSERACT_OEM,
    TESSERACT_PAGE_TIMEOUT_SECONDS,
    TESSERACT_PRESERVE_INTERWORD_SPACES,
    TESSERACT_PSM,
)
from ocr_worker.models import DocumentData, PageData, TextElement

if TYPE_CHECKING:
    from collections.abc import Sequence


# -----------------------------
# Предпроцессинг PDF
# -----------------------------
class PreprocessedPage:
    """Результат предпроцессинга одной страницы."""

    def __init__(self, page_number: int, rendered_path: Path, preprocessed_path: Path):
        self.page_number = page_number
        self.rendered_path = rendered_path
        self.preprocessed_path = preprocessed_path


class PreprocessedPDF:
    """Результат предпроцессинга всего PDF."""

    def __init__(self, workdir: Path, pages: list[PreprocessedPage]):
        self.workdir = workdir
        self.pages = pages

    def cleanup(self) -> None:
        """Удаляет рабочую директорию с артефактами."""
        if self.workdir.exists():
            shutil.rmtree(self.workdir, ignore_errors=True)


def _preprocess_image(image_path: Path, output_path: Path) -> None:
    """Предобработка изображения: grayscale + нормализация контраста."""
    # Читаем изображение
    img = cv2.imread(str(image_path))
    if img is None:
        raise ValueError(f"Не удалось прочитать изображение: {image_path}")

    # Конвертируем в grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # Нормализация контраста (если включена)
    if PDF_PREPROCESS_NORMALIZE:
        gray = cv2.normalize(gray, None, 0, 255, cv2.NORM_MINMAX)

    # Сохраняем результат
    cv2.imwrite(str(output_path), gray)


def preprocess_pdf_to_images(pdf_path: Path, *, max_pages: int | None = None) -> PreprocessedPDF:
    """Конвертирует PDF в изображения с предобработкой.

    Args:
        pdf_path: путь к PDF файлу
        max_pages: максимальное количество страниц (None = все)

    Returns:
        PreprocessedPDF с путями к rendered и preprocessed изображениям
    """
    # Создаём временную директорию
    workdir = Path(tempfile.mkdtemp(prefix="ocr_worker_"))
    rendered_dir = workdir / "rendered"
    preprocessed_dir = workdir / "preprocessed"
    rendered_dir.mkdir()
    preprocessed_dir.mkdir()

    logger.info("Предпроцессинг: pdf=%s, workdir=%s", pdf_path.name, workdir)

    try:
        # Рендерим PDF в изображения
        # paths_only=True для экономии RAM
        image_paths = convert_from_path(
            str(pdf_path),
            dpi=PDF_RENDER_DPI,
            fmt=PDF_RENDER_FORMAT,
            output_folder=str(rendered_dir),
            paths_only=True,
            thread_count=PDF_RENDER_THREAD_COUNT,
            last_page=max_pages if max_pages else None,
        )

        logger.info("Рендеринг завершён: pages=%s", len(image_paths))

        # Предобрабатываем каждое изображение
        pages: list[PreprocessedPage] = []
        for idx, rendered_path_str in enumerate(image_paths, start=1):
            rendered_path = Path(rendered_path_str)
            preprocessed_path = preprocessed_dir / f"page_{idx:04d}.{PDF_PREPROCESS_OUTPUT_FORMAT}"

            _preprocess_image(rendered_path, preprocessed_path)

            pages.append(PreprocessedPage(
                page_number=idx,
                rendered_path=rendered_path,
                preprocessed_path=preprocessed_path,
            ))

        logger.info("Предобработка завершена: pages=%s", len(pages))
        return PreprocessedPDF(workdir=workdir, pages=pages)

    except Exception:
        # При ошибке удаляем workdir
        shutil.rmtree(workdir, ignore_errors=True)
        raise


# -----------------------------
# OCR через tesseract
# -----------------------------
def _ensure_tesseract_available() -> bool:
    """Проверяет доступность tesseract."""
    return shutil.which("tesseract") is not None


def _ensure_poppler_available() -> bool:
    """Проверяет доступность poppler (pdftoppm)."""
    return shutil.which("pdftoppm") is not None


def _tesseract_ocr_image(image_path: Path) -> str:
    """Запускает tesseract для одного изображения и возвращает текст."""
    cmd = [
        "tesseract",
        str(image_path),
        "stdout",
        "-l", str(TESSERACT_LANG),
        "--oem", str(TESSERACT_OEM),
        "--psm", str(TESSERACT_PSM),
        "-c", f"preserve_interword_spaces={TESSERACT_PRESERVE_INTERWORD_SPACES}",
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
        raise RuntimeError(f"Tesseract error (code={completed.returncode}): {stderr}")

    return completed.stdout or ""


def _normalize_ocr_text(text: str) -> str:
    """Нормализует текст после OCR."""
    return (text or "").replace("\r\n", "\n").rstrip()


def _build_document_from_page_texts(filename: str, page_texts: Sequence[str]) -> DocumentData:
    """Собирает DocumentData из списка текстов страниц."""
    pages: list[PageData] = []
    for page_number, text in enumerate(page_texts, start=1):
        normalized = _normalize_ocr_text(text)
        elements: list[TextElement] = []
        if normalized:
            elements.append(TextElement(category="ocr", content=normalized, type="text"))

        pages.append(PageData(
            page_number=page_number,
            full_text=normalized,
            elements=elements,
            total_elements=len(elements),
        ))

    return DocumentData(filename=filename, pages=pages, total_pages=len(pages))


def process_pdf(pdf_path: Path, *, max_pages: int | None = None) -> tuple[DocumentData, float]:
    """Выполняет полный цикл OCR: preprocess -> tesseract.

    Args:
        pdf_path: путь к PDF файлу
        max_pages: ограничение по страницам (None = все)

    Returns:
        (DocumentData, время_выполнения_в_секундах)
    """
    if not _ensure_tesseract_available():
        raise RuntimeError("Tesseract не найден в PATH")

    started_at = time.perf_counter()
    max_workers = OCR_PAGE_CONCURRENCY

    logger.info(
        "OCR старт: pdf=%s, size=%s bytes, max_pages=%s",
        pdf_path.name, pdf_path.stat().st_size, max_pages
    )
    logger.info(
        "OCR конфиг: lang=%s, oem=%s, psm=%s, concurrency=%s",
        TESSERACT_LANG, TESSERACT_OEM, TESSERACT_PSM, max_workers
    )

    # Предпроцессинг PDF
    preprocessed = preprocess_pdf_to_images(pdf_path, max_pages=max_pages)
    total_pages = len(preprocessed.pages)

    try:
        # Параллельный OCR
        texts_by_page: dict[int, str] = {}
        future_meta: dict[Future[str], tuple[PreprocessedPage, float]] = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for page in preprocessed.pages:
                logger.info(
                    "OCR страница %s/%s: %s",
                    page.page_number, total_pages, page.preprocessed_path.name
                )
                future = executor.submit(_tesseract_ocr_image, page.preprocessed_path)
                future_meta[future] = (page, time.perf_counter())

            for future in as_completed(future_meta):
                page, submitted_at = future_meta[future]
                try:
                    text = future.result()
                except Exception as e:
                    raise RuntimeError(
                        f"OCR ошибка на странице {page.page_number}/{total_pages}"
                    ) from e

                texts_by_page[page.page_number] = text
                logger.info(
                    "OCR страница %s/%s готова: chars=%s, time=%.2fs",
                    page.page_number, total_pages, len(text), time.perf_counter() - submitted_at
                )

        # Собираем результат
        page_texts = [texts_by_page[i] for i in range(1, total_pages + 1)]
        document = _build_document_from_page_texts(pdf_path.name, page_texts)
        duration = time.perf_counter() - started_at

        logger.info("OCR завершён: pages=%s, time=%.2fs", document.total_pages, duration)
        return document, duration

    finally:
        # Всегда удаляем временные файлы
        preprocessed.cleanup()
        logger.info("OCR: workdir удалён")


def check_dependencies() -> tuple[bool, bool]:
    """Проверяет доступность tesseract и poppler.

    Returns:
        (tesseract_available, poppler_available)
    """
    return _ensure_tesseract_available(), _ensure_poppler_available()
