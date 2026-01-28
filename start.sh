#!/bin/bash
# =============================================================================
# FSK Defect Bot — скрипт запуска
# =============================================================================
# Проверяет зависимости, запускает OCR Worker и Docker контейнер
#
# Использование:
#   ./start.sh          # Запуск всего
#   ./start.sh worker   # Только OCR Worker
#   ./start.sh bot      # Только Docker бот
#   ./start.sh check    # Только проверка зависимостей
#   ./start.sh stop     # Остановить всё
# =============================================================================

set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Директория скрипта
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Конфигурация
OCR_WORKER_PORT=8765
OCR_WORKER_VENV="ocr_worker_venv"
OCR_WORKER_LOG="ocr_worker.log"
OCR_WORKER_PID="ocr_worker.pid"

# -----------------------------------------------------------------------------
# Функции
# -----------------------------------------------------------------------------

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Проверка зависимости
check_command() {
    local cmd=$1
    local name=$2
    if command -v "$cmd" &> /dev/null; then
        log_success "$name: $(command -v $cmd)"
        return 0
    else
        log_error "$name: не найден"
        return 1
    fi
}

# Проверка всех зависимостей
check_dependencies() {
    log_info "Проверка зависимостей..."
    echo ""

    local errors=0

    # Системные зависимости
    check_command "python3" "Python 3" || ((errors++))
    check_command "docker" "Docker" || ((errors++))
    check_command "docker-compose" "Docker Compose" || ((errors++))
    check_command "tesseract" "Tesseract OCR" || ((errors++))
    check_command "pdftoppm" "Poppler (pdftoppm)" || ((errors++))

    echo ""

    # Проверка .env файла
    if [[ -f ".env" ]]; then
        if grep -q "BOT_TOKEN=" .env && ! grep -q "BOT_TOKEN=your_telegram" .env; then
            log_success ".env файл: настроен"
        else
            log_warn ".env файл: BOT_TOKEN не задан"
            ((errors++))
        fi
    else
        log_error ".env файл: не найден (скопируйте .env.example)"
        ((errors++))
    fi

    # Проверка venv для OCR Worker
    if [[ -d "$OCR_WORKER_VENV" ]]; then
        log_success "OCR Worker venv: $OCR_WORKER_VENV"
    else
        log_warn "OCR Worker venv: не найден (будет создан)"
    fi

    echo ""

    if [[ $errors -gt 0 ]]; then
        log_error "Найдено $errors ошибок. Исправьте перед запуском."
        return 1
    else
        log_success "Все зависимости в порядке!"
        return 0
    fi
}

# Создание venv для OCR Worker
setup_ocr_worker_venv() {
    if [[ ! -d "$OCR_WORKER_VENV" ]]; then
        log_info "Создание venv для OCR Worker..."
        python3 -m venv "$OCR_WORKER_VENV"
        source "$OCR_WORKER_VENV/bin/activate"
        pip install --upgrade pip
        pip install -r ocr_worker/requirements.txt
        deactivate
        log_success "OCR Worker venv создан"
    fi
}

# Запуск OCR Worker
start_ocr_worker() {
    # Проверяем, не запущен ли уже
    if [[ -f "$OCR_WORKER_PID" ]]; then
        local pid=$(cat "$OCR_WORKER_PID")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_warn "OCR Worker уже запущен (PID: $pid)"
            return 0
        else
            rm -f "$OCR_WORKER_PID"
        fi
    fi

    # Проверяем порт
    if lsof -i ":$OCR_WORKER_PORT" > /dev/null 2>&1; then
        log_warn "Порт $OCR_WORKER_PORT уже занят"
        return 0
    fi

    setup_ocr_worker_venv

    log_info "Запуск OCR Worker на порту $OCR_WORKER_PORT..."

    source "$OCR_WORKER_VENV/bin/activate"
    nohup python -m uvicorn ocr_worker.main:app \
        --host 0.0.0.0 \
        --port "$OCR_WORKER_PORT" \
        > "$OCR_WORKER_LOG" 2>&1 &

    local pid=$!
    echo "$pid" > "$OCR_WORKER_PID"
    deactivate

    # Ждём запуска
    sleep 2

    if ps -p "$pid" > /dev/null 2>&1; then
        log_success "OCR Worker запущен (PID: $pid)"
        log_info "Логи: tail -f $OCR_WORKER_LOG"

        # Проверяем health
        if curl -s "http://localhost:$OCR_WORKER_PORT/health" | grep -q '"status":"ok"'; then
            log_success "OCR Worker health check: OK"
        else
            log_warn "OCR Worker health check: не прошёл (возможно ещё запускается)"
        fi
    else
        log_error "OCR Worker не запустился. Проверьте $OCR_WORKER_LOG"
        return 1
    fi
}

# Остановка OCR Worker
stop_ocr_worker() {
    if [[ -f "$OCR_WORKER_PID" ]]; then
        local pid=$(cat "$OCR_WORKER_PID")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_info "Останавливаем OCR Worker (PID: $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 1
            if ps -p "$pid" > /dev/null 2>&1; then
                kill -9 "$pid" 2>/dev/null || true
            fi
            log_success "OCR Worker остановлен"
        fi
        rm -f "$OCR_WORKER_PID"
    else
        log_info "OCR Worker не запущен"
    fi
}

# Запуск Docker бота
start_bot() {
    log_info "Запуск Docker контейнера..."
    docker-compose up -d --build
    log_success "Docker контейнер запущен"
    log_info "Логи: docker-compose logs -f"
}

# Остановка Docker бота
stop_bot() {
    log_info "Останавливаем Docker контейнер..."
    docker-compose down
    log_success "Docker контейнер остановлен"
}

# Показать статус
show_status() {
    echo ""
    log_info "=== Статус ==="
    echo ""

    # OCR Worker
    if [[ -f "$OCR_WORKER_PID" ]] && ps -p "$(cat $OCR_WORKER_PID)" > /dev/null 2>&1; then
        log_success "OCR Worker: запущен (PID: $(cat $OCR_WORKER_PID))"
    else
        log_warn "OCR Worker: не запущен"
    fi

    # Docker
    if docker-compose ps 2>/dev/null | grep -q "Up"; then
        log_success "Docker бот: запущен"
        docker-compose ps
    else
        log_warn "Docker бот: не запущен"
    fi

    echo ""
}

# Показать помощь
show_help() {
    echo "FSK Defect Bot — скрипт запуска"
    echo ""
    echo "Использование: ./start.sh [команда]"
    echo ""
    echo "Команды:"
    echo "  (без аргументов)  Запустить всё (OCR Worker + Docker бот)"
    echo "  worker            Только OCR Worker"
    echo "  bot               Только Docker бот"
    echo "  check             Проверка зависимостей"
    echo "  status            Показать статус сервисов"
    echo "  stop              Остановить всё"
    echo "  stop-worker       Остановить OCR Worker"
    echo "  stop-bot          Остановить Docker бот"
    echo "  logs              Показать логи Docker"
    echo "  logs-worker       Показать логи OCR Worker"
    echo "  help              Показать эту справку"
    echo ""
}

# -----------------------------------------------------------------------------
# Основная логика
# -----------------------------------------------------------------------------

case "${1:-}" in
    "check")
        check_dependencies
        ;;
    "worker")
        check_dependencies && start_ocr_worker
        ;;
    "bot")
        start_bot
        ;;
    "status")
        show_status
        ;;
    "stop")
        stop_ocr_worker
        stop_bot
        ;;
    "stop-worker")
        stop_ocr_worker
        ;;
    "stop-bot")
        stop_bot
        ;;
    "logs")
        docker-compose logs -f
        ;;
    "logs-worker")
        tail -f "$OCR_WORKER_LOG"
        ;;
    "help"|"-h"|"--help")
        show_help
        ;;
    "")
        # Запуск всего
        echo ""
        echo "=========================================="
        echo "  FSK Defect Bot — Запуск"
        echo "=========================================="
        echo ""

        if ! check_dependencies; then
            exit 1
        fi

        echo ""
        start_ocr_worker
        echo ""
        start_bot
        echo ""

        show_status
        ;;
    *)
        log_error "Неизвестная команда: $1"
        show_help
        exit 1
        ;;
esac
