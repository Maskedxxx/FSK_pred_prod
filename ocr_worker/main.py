"""FastAPI приложение OCR Worker.

Endpoints:
- GET /health — проверка состояния сервиса
- POST /ocr — обработка PDF и возврат OCR-результата
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Query, UploadFile

from ocr_worker.config import logger, OCR_WORKER_HOST, OCR_WORKER_PORT
from ocr_worker.models import HealthResponse, OCRResponse
from ocr_worker.processor import check_dependencies, process_pdf

# Создаём FastAPI приложение
app = FastAPI(
    title="OCR Worker",
    description="Сервис OCR для обработки PDF документов через tesseract",
    version="1.0.0",
)


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Проверка состояния сервиса и доступности зависимостей."""
    tesseract_ok, poppler_ok = check_dependencies()

    if tesseract_ok and poppler_ok:
        return HealthResponse(
            status="ok",
            tesseract=tesseract_ok,
            poppler=poppler_ok,
            error=None,
        )
    else:
        errors = []
        if not tesseract_ok:
            errors.append("tesseract не найден")
        if not poppler_ok:
            errors.append("poppler не найден")
        return HealthResponse(
            status="error",
            tesseract=tesseract_ok,
            poppler=poppler_ok,
            error="; ".join(errors),
        )


@app.post("/ocr", response_model=OCRResponse)
async def process_ocr(
    file: UploadFile = File(..., description="PDF файл для обработки"),
    max_pages: int | None = Query(None, description="Максимальное количество страниц"),
) -> OCRResponse:
    """Обрабатывает PDF файл и возвращает OCR-результат.

    Args:
        file: загруженный PDF файл
        max_pages: ограничение по количеству страниц (опционально)

    Returns:
        OCRResponse с результатом OCR или ошибкой
    """
    # Проверяем тип файла
    if not file.filename:
        raise HTTPException(status_code=400, detail="Имя файла не указано")

    filename = file.filename
    if not filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Файл должен быть PDF")

    logger.info("OCR запрос: filename=%s, max_pages=%s", filename, max_pages)

    # Сохраняем файл во временную директорию
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = Path(tmp.name)

        logger.info("Файл сохранён: %s, size=%s bytes", tmp_path, len(content))

        # Выполняем OCR
        try:
            document, seconds = process_pdf(tmp_path, max_pages=max_pages)
            return OCRResponse(
                success=True,
                filename=filename,
                document=document,
                seconds=seconds,
                total_pages=document.total_pages,
                error=None,
            )
        except Exception as e:
            logger.exception("OCR ошибка: %s", str(e))
            return OCRResponse(
                success=False,
                filename=filename,
                document=None,
                seconds=0.0,
                total_pages=0,
                error=str(e),
            )

    finally:
        # Удаляем временный файл
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()
            logger.info("Временный файл удалён: %s", tmp_path)


@app.get("/")
async def root() -> dict[str, str]:
    """Корневой endpoint с информацией о сервисе."""
    return {
        "service": "OCR Worker",
        "version": "1.0.0",
        "endpoints": "/health, /ocr",
    }


# Точка входа для запуска через uvicorn напрямую
if __name__ == "__main__":
    import uvicorn

    logger.info("Запуск OCR Worker: host=%s, port=%s", OCR_WORKER_HOST, OCR_WORKER_PORT)
    uvicorn.run(
        "ocr_worker.main:app",
        host=OCR_WORKER_HOST,
        port=OCR_WORKER_PORT,
        reload=False,
    )
