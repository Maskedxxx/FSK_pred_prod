"""Конфигурация OCR Worker.

Настройки для standalone OCR сервиса на хосте.
Значения можно переопределить через переменные окружения.
"""

from __future__ import annotations

import logging
import os

# -----------------------------
# Логирование
# -----------------------------
LOG_LEVEL = os.getenv("OCR_WORKER_LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("ocr_worker")

# -----------------------------
# Сервер
# -----------------------------
# Порт для FastAPI сервера
OCR_WORKER_PORT = int(os.getenv("OCR_WORKER_PORT", "8765"))

# Хост для биндинга (0.0.0.0 для доступа извне)
OCR_WORKER_HOST = os.getenv("OCR_WORKER_HOST", "0.0.0.0")

# -----------------------------
# PDF -> images (рендер страниц)
# -----------------------------
# DPI рендера PDF в изображения
PDF_RENDER_DPI = int(os.getenv("PDF_RENDER_DPI", "200"))

# Формат рендера страниц на диск
PDF_RENDER_FORMAT = os.getenv("PDF_RENDER_FORMAT", "jpeg")

# Кол-во потоков рендера (pdf2image)
PDF_RENDER_THREAD_COUNT = int(os.getenv("PDF_RENDER_THREAD_COUNT", "2"))

# -----------------------------
# Предобработка изображений
# -----------------------------
# Включить нормализацию контраста
PDF_PREPROCESS_NORMALIZE = os.getenv("PDF_PREPROCESS_NORMALIZE", "true").lower() == "true"

# Формат сохранения предобработанных страниц
PDF_PREPROCESS_OUTPUT_FORMAT = os.getenv("PDF_PREPROCESS_OUTPUT_FORMAT", "png")

# -----------------------------
# OCR (tesseract)
# -----------------------------
# Языки tesseract
TESSERACT_LANG = os.getenv("TESSERACT_LANG", "rus+eng")

# OCR Engine Mode и Page Segmentation Mode
TESSERACT_OEM = int(os.getenv("TESSERACT_OEM", "3"))
TESSERACT_PSM = int(os.getenv("TESSERACT_PSM", "6"))

# Сохранять интервалы между словами
TESSERACT_PRESERVE_INTERWORD_SPACES = int(os.getenv("TESSERACT_PRESERVE_INTERWORD_SPACES", "1"))

# Таймаут на распознавание одной страницы (сек)
TESSERACT_PAGE_TIMEOUT_SECONDS = int(os.getenv("TESSERACT_PAGE_TIMEOUT_SECONDS", "300"))

# Сколько страниц OCR обрабатывать параллельно
OCR_PAGE_CONCURRENCY = int(os.getenv("OCR_PAGE_CONCURRENCY", "4"))
