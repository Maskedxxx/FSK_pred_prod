"""Единый конфиг проекта FSK_pred_prod.

Принцип: "config — источник правды". Значения не должны переопределяться внутри сервисов.
Если нужно изменить поведение — меняем константы здесь.
"""

from __future__ import annotations

import logging
import os

# Обязательная инициализация логирования вынесена в util, но настройки — тут (config = truth).
from utils.logging_utils import setup_console_logging

# -----------------------------
# Логирование
# -----------------------------
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

setup_console_logging(level=LOG_LEVEL, fmt=LOG_FORMAT)
logger = logging.getLogger("FSK_pred_prod")

# -----------------------------
# PDF -> images (рендер страниц)
# -----------------------------
# DPI рендера PDF в изображения. Чем выше — тем лучше OCR, но тем больше время/память.
PDF_RENDER_DPI = 200

# Формат рендера страниц на диск. Для скорости обычно достаточно jpeg.
PDF_RENDER_FORMAT = "jpeg"  # "jpeg" | "png"

# Кол-во потоков рендера (pdf2image). Не делайте слишком большим.
PDF_RENDER_THREAD_COUNT = 2

# -----------------------------
# Предобработка изображений (перед OCR)
# -----------------------------
# Включить нормализацию контраста (cv2.normalize)
PDF_PREPROCESS_NORMALIZE = True

# Формат сохранения предобработанных страниц
PDF_PREPROCESS_OUTPUT_FORMAT = "png"  # удобнее для дальнейшего OCR

# -----------------------------
# Flowise Page Filter (фильтрация релевантных страниц через LLM)
# -----------------------------
# API URLs для двух фаз FSM (разные JSON схемы в Flowise)
FLOWISE_API_URL_SEARCH_START = "https://app.osmi-it.ru/api/v1/prediction/63a99846-4740-41d5-9e4a-bc5b9dbfca8c"
FLOWISE_API_URL_SEARCH_END = "https://app.osmi-it.ru/api/v1/prediction/32a0f689-d231-41e4-9531-251661143744"

# Размер батча: сколько страниц отправлять за один запрос к LLM
FLOWISE_BATCH_SIZE = 10

# Таймаут HTTP запроса к Flowise (секунды)
FLOWISE_TIMEOUT_SECONDS = 300

# Максимум символов текста страницы в промпте (обрезка для экономии токенов)
FLOWISE_MAX_CHARS_PER_PAGE = 15000

# -----------------------------
# VLM Page Cleaner (Flowise Vision)
# -----------------------------
# API URL для VLM очистки страниц (подставьте свой endpoint)
FLOWISE_API_URL_VLM_CLEAN = "https://app.osmi-it.ru/api/v1/prediction/0916e84d-0957-4230-b23d-7c1c162278c5"

# DPI рендера страниц для VLM (ниже чем OCR — экономия токенов, но достаточно для vision)
VLM_RENDER_DPI = 400

# Максимальный размер изображения. Страница масштабируется с сохранением пропорций (без letterbox).
VLM_IMAGE_MAX_WIDTH = 1536
VLM_IMAGE_MAX_HEIGHT = 1536

# Качество JPEG при кодировании в base64 (0-100)
VLM_IMAGE_JPEG_QUALITY = 85

# Retry настройки для VLM запросов
VLM_MAX_RETRIES = 3
VLM_RETRY_BASE_DELAY_SECONDS = 2

# Таймаут HTTP запроса к Flowise VLM (секунды)
VLM_TIMEOUT_SECONDS = 240

# Сколько страниц VLM обрабатывать параллельно (ограничение одновременных запросов)
VLM_PAGE_CONCURRENCY = 3

# -----------------------------
# Defect Extractor (Flowise LLM)
# -----------------------------
# API URL для извлечения дефектов (подставьте свой endpoint)
FLOWISE_API_URL_DEFECT_EXTRACT = "https://app.osmi-it.ru/api/v1/prediction/69eecc47-ab48-4f48-90a1-049d3e374f8a"

# Сколько страниц обрабатывать параллельно
DEFECT_EXTRACTION_CONCURRENCY = 3

# Сколько символов брать из соседних страниц для контекста (prev/next)
DEFECT_EXTRACTION_CONTEXT_CHARS = 1500

# Таймаут HTTP запроса (секунды)
DEFECT_EXTRACTION_TIMEOUT_SECONDS = 180

# Retry настройки
DEFECT_EXTRACTION_MAX_RETRIES = 3
DEFECT_EXTRACTION_RETRY_DELAY_SECONDS = 2

# -----------------------------
# OCR Worker (внешний сервис на хосте)
# -----------------------------
# URL OCR Worker — FastAPI сервис с tesseract, запущенный на хосте
# host.docker.internal — специальный адрес для доступа к хосту из Docker
OCR_WORKER_URL = os.getenv("OCR_WORKER_URL", "http://host.docker.internal:8765")

# Таймаут запроса к OCR Worker (секунды)
OCR_WORKER_TIMEOUT_SECONDS = int(os.getenv("OCR_WORKER_TIMEOUT_SECONDS", "600"))
