"""Единый конфиг проекта FSK_pred_prod.

Принцип: "config — источник правды". Значения не должны переопределяться внутри сервисов.
Если нужно изменить поведение — меняем константы здесь.
"""

from __future__ import annotations

import logging

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
PDF_RENDER_DPI = 300

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
# OCR (tesseract)
# -----------------------------
# Языки tesseract. Обычно для наших документов достаточно rus+eng.
TESSERACT_LANG = "rus+eng"

# OCR Engine Mode (oem) и Page Segmentation Mode (psm) — параметры tesseract.
TESSERACT_OEM = 3
TESSERACT_PSM = 6

# Сохранять интервалы между словами (иногда улучшает читаемость табличного/форматного текста).
TESSERACT_PRESERVE_INTERWORD_SPACES = 1

# Таймаут на распознавание одной страницы (сек). Нужен, чтобы сервис не зависал на "тяжёлых" страницах.
TESSERACT_PAGE_TIMEOUT_SECONDS = 300

# Сколько страниц OCR обрабатывать параллельно (ограничение по одновременным tesseract-процессам).
# 3–4 обычно даёт хороший баланс скорости/нагрузки.
OCR_PAGE_CONCURRENCY = 4

# Оставлять ли временную папку предпроцессинга (workdir) после OCR.
# Для отладки можно включить True и смотреть изображения. В проде обычно False.
OCR_KEEP_PREPROCESS_WORKDIR = False

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
VLM_RENDER_DPI = 250

# Целевой размер изображения (letterbox). Страница масштабируется с сохранением пропорций.
VLM_IMAGE_TARGET_WIDTH = 1024
VLM_IMAGE_TARGET_HEIGHT = 1024

# Качество JPEG при кодировании в base64 (0-100)
VLM_IMAGE_JPEG_QUALITY = 85

# Retry настройки для VLM запросов
VLM_MAX_RETRIES = 3
VLM_RETRY_BASE_DELAY_SECONDS = 2

# Таймаут HTTP запроса к Flowise VLM (секунды)
VLM_TIMEOUT_SECONDS = 240

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
