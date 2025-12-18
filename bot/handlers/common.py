"""Общие хендлеры бота.

Содержит fallback обработчик для неизвестных сообщений.
"""

from __future__ import annotations

from aiogram import types
from aiogram.enums import ParseMode

from bot.config import Messages
from bot.keyboards.main import get_main_keyboard


async def fallback(message: types.Message) -> None:
    """Обрабатывает все неизвестные сообщения.

    Отправляет информационное сообщение с инструкцией.

    Args:
        message: Входящее сообщение
    """
    await message.answer(
        Messages.FALLBACK,
        reply_markup=get_main_keyboard(),
        parse_mode=ParseMode.MARKDOWN,
    )
