# FSK Defect Bot

Telegram бот для анализа дефектов в технических отчётах (PDF).

## Что делает

1. Принимает PDF через Telegram (ссылка Google Drive или файл)
2. OCR — распознаёт текст (Tesseract)
3. Фильтрует релевантные страницы (LLM)
4. Очищает страницы через Vision LLM
5. Извлекает список дефектов (LLM)
6. Генерирует Excel отчёт

## Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│  Docker контейнер                                           │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Telegram Bot (aiogram)                              │   │
│  │  └─► Pipeline: OCR → Filter → VLM → Extract → Excel │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                 │
│                           │ HTTP (OCR запросы)              │
└───────────────────────────│─────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Хост (вне Docker)                                          │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  OCR Worker (FastAPI + Tesseract)                    │   │
│  │  http://localhost:8765                               │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

OCR вынесен на хост потому что Tesseract в Docker работает в 10x медленнее.

## Структура проекта

```
├── bot/                    # Telegram бот
│   ├── main.py            # Точка входа
│   └── handlers/          # Обработчики сообщений
├── services/              # Бизнес-логика
│   ├── ocr_service.py     # OCR (клиент к OCR Worker)
│   ├── pipeline.py        # Полный пайплайн
│   └── ...
├── ocr_worker/            # OCR сервис (FastAPI)
│   ├── main.py            # Endpoints /health, /ocr
│   ├── processor.py       # Tesseract + предобработка
│   └── config.py          # Настройки OCR
├── config.py              # Главный конфиг
├── docker-compose.yml     # Docker конфигурация
├── Dockerfile
├── start.sh               # Скрипт запуска
└── .env.example           # Пример переменных окружения
```

## Требования

- Python 3.11+
- Docker + Docker Compose
- Tesseract OCR (на хосте)
- Poppler (на хосте, для pdf2image)

## Установка

### 1. Клонировать репозиторий

```bash
git clone <repo-url>
cd FSK_pred_prod
```

### 2. Системные зависимости

**macOS:**

```bash
brew install tesseract tesseract-lang poppler
```

**Ubuntu/Debian:**

```bash
sudo apt-get update
sudo apt-get install -y tesseract-ocr tesseract-ocr-rus poppler-utils python3-venv
```

### 3. Python окружение для OCR Worker

```bash
# Создать venv
python3 -m venv ocr_worker_venv

# Активировать
source ocr_worker_venv/bin/activate  # Linux/macOS

# Установить зависимости
pip install -r ocr_worker/requirements.txt

# Деактивировать (опционально)
deactivate
```

> Примечание: `start.sh` создаёт venv автоматически, если его нет.

### 4. Настройка переменных окружения

```bash
# Скопировать пример
cp .env.example .env

# Заполнить BOT_TOKEN (получить у @BotFather в Telegram)
nano .env
```

## Запуск

### Через скрипт (рекомендуется)

```bash
# Запустить всё (OCR Worker + Docker бот)
./start.sh

# Проверить статус
./start.sh status

# Остановить
./start.sh stop
```

### Другие команды

```bash
./start.sh check        # Проверить зависимости
./start.sh worker       # Только OCR Worker
./start.sh bot          # Только Docker бот
./start.sh logs         # Логи Docker
./start.sh logs-worker  # Логи OCR Worker
./start.sh help         # Справка
```

## Настройки

### Переменные окружения (.env)

```bash
BOT_TOKEN=...                                    # Telegram бот токен (обязательно)
OCR_WORKER_URL=http://host.docker.internal:8765  # URL OCR Worker
OCR_WORKER_TIMEOUT_SECONDS=600                   # Таймаут OCR (сек)
LOG_LEVEL=INFO                                   # Уровень логирования
```

### Конфигурация

- `config.py` — основные настройки (Flowise URLs, VLM, таймауты)
- `ocr_worker/config.py` — настройки OCR (Tesseract параметры)
