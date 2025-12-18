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
