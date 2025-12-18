"""Сервис предпроцессинга PDF: рендер + предобработка изображений.

Задача: подготовить страницы PDF в виде файлов изображений, пригодных для OCR.
OCR здесь НЕ выполняется — только подготовка входа для OCR-движка.
"""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path

from pydantic import BaseModel, Field

from config import (
    logger,
    PDF_PREPROCESS_NORMALIZE,
    PDF_PREPROCESS_OUTPUT_FORMAT,
    PDF_RENDER_DPI,
    PDF_RENDER_FORMAT,
    PDF_RENDER_THREAD_COUNT,
)


class PreprocessedPage(BaseModel):
    """Артефакты одной страницы после предпроцессинга."""

    page_number: int = Field(..., description="Номер страницы (1..N) в исходном PDF.")
    rendered_path: Path = Field(..., description="Путь к файлу страницы после рендера PDF→image.")
    preprocessed_path: Path = Field(
        ...,
        description="Путь к файлу страницы после предобработки (grayscale/normalize) для OCR.",
    )


class PreprocessedPDF(BaseModel):
    """Артефакты предпроцессинга всего PDF."""

    pdf_path: Path = Field(..., description="Путь к исходному PDF.")
    workdir: Path = Field(..., description="Временная директория с артефактами (rendered/ и preprocessed/).")
    pages: list[PreprocessedPage] = Field(
        default_factory=list,
        description="Список страниц (в порядке документа) с путями к артефактам.",
    )

    def cleanup(self) -> None:
        """Удаляет временную директорию с рендером/предобработкой (best-effort)."""
        shutil.rmtree(self.workdir, ignore_errors=True)


def _ensure_pdf_file(pdf_path: str | Path) -> Path:
    pdf = Path(pdf_path).expanduser().resolve()
    if not pdf.exists():
        raise FileNotFoundError(f"PDF не найден: {pdf}")
    if not pdf.is_file():
        raise IsADirectoryError(f"Ожидался файл PDF, но получено: {pdf}")
    return pdf


def _normalize_max_pages(max_pages: int | None) -> int | None:
    if max_pages is None:
        return None
    try:
        value = int(max_pages)
    except (TypeError, ValueError) as e:
        raise TypeError(f"max_pages должен быть int или None, получено: {max_pages!r}") from e
    return value if value > 0 else None


def _normalize_output_ext(fmt: str) -> str:
    ext = str(fmt).strip().lower().lstrip(".")
    if not ext:
        raise ValueError("PDF_PREPROCESS_OUTPUT_FORMAT не должен быть пустым.")
    return ext


def _import_convert_from_path():
    try:
        from pdf2image import convert_from_path

        return convert_from_path
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Не удалось импортировать pdf2image. Установите `pdf2image` и Poppler "
            "(macOS: `brew install poppler`)."
        ) from e


def _import_cv2():
    try:
        import cv2  # type: ignore

        return cv2
    except Exception as e:  # pragma: no cover
        raise RuntimeError("Нужен пакет `opencv-python` (и numpy) для предобработки изображений.") from e


def _create_workdir(*, prefix: str = "fsk_pdf_preprocess_") -> tuple[Path, Path, Path]:
    workdir = Path(tempfile.mkdtemp(prefix=prefix))
    render_dir = workdir / "rendered"
    preprocess_dir = workdir / "preprocessed"
    render_dir.mkdir(parents=True, exist_ok=True)
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    return workdir, render_dir, preprocess_dir


def _preprocess_page_to_file(
    *,
    cv2,
    rendered_path: Path,
    preprocess_dir: Path,
    page_number: int,
    normalize: bool,
    output_ext: str,
) -> Path:
    gray = cv2.imread(str(rendered_path), cv2.IMREAD_GRAYSCALE)
    if gray is None:
        raise RuntimeError(f"Не удалось загрузить изображение: {rendered_path}")

    if normalize:
        gray = cv2.normalize(gray, None, 0, 255, cv2.NORM_MINMAX)

    preprocessed_path = preprocess_dir / f"page_{page_number:04d}.{output_ext}"
    ok = cv2.imwrite(str(preprocessed_path), gray)
    if not ok:
        raise RuntimeError(f"Не удалось сохранить предобработанное изображение: {preprocessed_path}")

    return preprocessed_path


def preprocess_pdf_to_images(pdf_path: str | Path, *, max_pages: int | None = None) -> PreprocessedPDF:
    """Рендерит PDF в изображения и делает простую предобработку (grayscale + normalize).

    Важно:
    - Настройки берутся ТОЛЬКО из `config.py`.
    - `max_pages` — runtime-ограничение для тестов (не "настройка качества").

    Args:
        pdf_path: путь к PDF
        max_pages: ограничение по количеству страниц (None/<=0 = все)

    Returns:
        PreprocessedPDF с путями на (rendered_path, preprocessed_path) по каждой странице.
    """
    pdf = _ensure_pdf_file(pdf_path)
    last_page = _normalize_max_pages(max_pages)

    convert_from_path = _import_convert_from_path()
    cv2 = _import_cv2()

    workdir, render_dir, preprocess_dir = _create_workdir()
    started_at = time.perf_counter()

    logger.info(
        "PDF предпроцессинг: name=%s, size_bytes=%s, dpi=%s, fmt=%s, threads=%s, max_pages=%s",
        pdf.name,
        pdf.stat().st_size,
        PDF_RENDER_DPI,
        PDF_RENDER_FORMAT,
        PDF_RENDER_THREAD_COUNT,
        last_page,
    )

    try:
        image_paths = convert_from_path(
            str(pdf),
            dpi=int(PDF_RENDER_DPI),
            first_page=1,
            last_page=last_page,
            output_folder=str(render_dir),
            paths_only=True,
            fmt=str(PDF_RENDER_FORMAT),
            thread_count=int(PDF_RENDER_THREAD_COUNT),
        )

        logger.info("PDF рендер завершён: pages=%s, rendered_dir=%s", len(image_paths), render_dir)

        out_ext = _normalize_output_ext(str(PDF_PREPROCESS_OUTPUT_FORMAT))
        pages: list[PreprocessedPage] = []
        total_pages = len(image_paths)

        for page_number, img_path in enumerate(image_paths, start=1):
            rendered_path = Path(img_path)
            logger.info("Предобработка страницы %s/%s: %s", page_number, total_pages, rendered_path.name)

            preprocessed_path = _preprocess_page_to_file(
                cv2=cv2,
                rendered_path=rendered_path,
                preprocess_dir=preprocess_dir,
                page_number=page_number,
                normalize=bool(PDF_PREPROCESS_NORMALIZE),
                output_ext=out_ext,
            )

            pages.append(
                PreprocessedPage(
                    page_number=page_number,
                    rendered_path=rendered_path,
                    preprocessed_path=preprocessed_path,
                )
            )

        duration = time.perf_counter() - started_at
        logger.info(
            "PDF предпроцессинг завершён: pages=%s, preprocessed_dir=%s, workdir=%s, seconds=%.2f",
            len(pages),
            preprocess_dir,
            workdir,
            duration,
        )

        return PreprocessedPDF(pdf_path=pdf, workdir=workdir, pages=pages)
    except Exception:
        logger.exception("Ошибка предпроцессинга PDF: %s", pdf)
        shutil.rmtree(workdir, ignore_errors=True)
        raise
