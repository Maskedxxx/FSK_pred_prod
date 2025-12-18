"""Точка входа Telegram бота для анализа дефектов.

Запуск:
    python -m bot.main

Или из корня проекта:
    python bot/main.py
"""

from __future__ import annotations

import asyncio
import signal
import sys
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from config import logger
from bot.config import BOT_TOKEN
from bot.keyboards.main import ButtonText
from bot.handlers.start import cmd_start
from bot.handlers.documents import (
    handle_upload_button,
    handle_google_drive_link,
    is_google_drive_link,
)
from bot.handlers.common import fallback


def create_bot() -> Bot:
    """Создаёт экземпляр бота с настройками по умолчанию.

    Returns:
        Настроенный экземпляр Bot
    """
    return Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN),
    )


def create_dispatcher() -> Dispatcher:
    """Создаёт и настраивает диспетчер с роутерами.

    Returns:
        Настроенный Dispatcher с зарегистрированными хендлерами
    """
    dp = Dispatcher()

    # === Регистрация хендлеров (порядок важен!) ===

    # 1. Команда /start
    dp.message.register(cmd_start, CommandStart())

    # 2. Кнопка "Загрузить документ"
    dp.message.register(
        handle_upload_button,
        F.text == ButtonText.UPLOAD_DOCUMENT,
    )

    # 3. Ссылки Google Drive
    dp.message.register(
        handle_google_drive_link,
        F.text,
        F.func(is_google_drive_link),
    )

    # 4. Fallback (должен быть последним!)
    dp.message.register(fallback)

    return dp


async def on_startup(bot: Bot) -> None:
    """Действия при запуске бота.

    Args:
        bot: Экземпляр бота
    """
    bot_info = await bot.get_me()
    logger.info(
        "Бот запущен: @%s (id=%s)",
        bot_info.username,
        bot_info.id,
    )


async def on_shutdown(bot: Bot) -> None:
    """Действия при остановке бота.

    Args:
        bot: Экземпляр бота
    """
    logger.info("Бот останавливается...")
    await bot.session.close()


async def main() -> None:
    """Главная функция запуска бота."""
    logger.info("=" * 50)
    logger.info("ЗАПУСК TELEGRAM БОТА")
    logger.info("=" * 50)

    bot = create_bot()
    dp = create_dispatcher()

    # Регистрируем lifecycle хуки
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Graceful shutdown по сигналам
    loop = asyncio.get_event_loop()

    def signal_handler() -> None:
        logger.info("Получен сигнал завершения...")
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        # Удаляем webhook если был (для polling режима)
        await bot.delete_webhook(drop_pending_updates=True)

        # Запускаем polling
        logger.info("Запуск polling...")
        await dp.start_polling(
            bot,
            allowed_updates=["message", "callback_query"],
        )
    finally:
        await on_shutdown(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем (Ctrl+C)")
    except Exception as e:
        logger.exception("Критическая ошибка бота: %s", e)
        sys.exit(1)
