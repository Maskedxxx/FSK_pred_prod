"""Хендлер команды /start.

Отвечает за приветствие пользователя и показ основной клавиатуры.
"""

from __future__ import annotations

from aiogram import types
from aiogram.enums import ParseMode

from bot.config import Messages
from bot.keyboards.main import get_main_keyboard


async def cmd_start(message: types.Message) -> None:
    """Обрабатывает команду /start.

    Отправляет приветственное сообщение и показывает основную клавиатуру.

    Args:
        message: Входящее сообщение от пользователя
    """
    await message.answer(
        Messages.WELCOME,
        reply_markup=get_main_keyboard(),
        parse_mode=ParseMode.MARKDOWN,
    )
