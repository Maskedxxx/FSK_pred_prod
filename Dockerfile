# =============================================================================
# FSK Defect Analysis Bot - Docker Image
# =============================================================================
# Multi-stage build для оптимизации размера образа
#
# Системные зависимости:
#   - poppler-utils: для pdf2image (рендеринг PDF)
#   - libgl1-mesa-glx: для opencv-python
#   - libglib2.0-0: для opencv-python
#
# Запуск:
#   docker-compose up --build
#
# =============================================================================

FROM python:3.11-slim AS base

# Метаданные
LABEL maintainer="FSK Team"
LABEL description="Telegram bot for defect analysis in technical documents"

# Переменные окружения
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Рабочая директория
WORKDIR /app

# =============================================================================
# Stage: builder - установка зависимостей
# =============================================================================
FROM base AS builder

# Устанавливаем системные зависимости для сборки
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements и устанавливаем Python-зависимости
COPY requirements.txt .
RUN pip install --user -r requirements.txt

# =============================================================================
# Stage: runtime - финальный образ
# =============================================================================
FROM base AS runtime

# Устанавливаем runtime системные зависимости
# Примечание: tesseract убран из Docker — OCR выполняется через OCR Worker на хосте
# (настраивается через OCR_MODE=remote в .env)
RUN apt-get update && apt-get install -y --no-install-recommends \
    # pdf2image требует poppler-utils для конвертации PDF в изображения (нужен для VLM)
    poppler-utils \
    # opencv-python требует эти библиотеки
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    # Для healthcheck и отладки
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Копируем установленные Python-пакеты из builder
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Копируем код приложения
COPY . .

# Создаём директорию для временных файлов пайплайна
RUN mkdir -p /tmp/fsk_pipeline

# Пользователь без root-привилегий (опционально, для продакшена)
# RUN useradd -m -r appuser && chown -R appuser:appuser /app /tmp/fsk_pipeline
# USER appuser

# Healthcheck - проверяем что Python работает
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import aiogram; import natasha; print('OK')" || exit 1

# Точка входа - запуск Telegram бота
CMD ["python", "-m", "bot.main"]
