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
