"""Клавиатуры Telegram бота.

Содержит Reply и Inline клавиатуры для взаимодействия с пользователем.
"""

from __future__ import annotations

from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)


# =============================================================================
# Reply Keyboards (постоянные кнопки внизу экрана)
# =============================================================================


def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Основная клавиатура с кнопкой загрузки документа.

    Returns:
        ReplyKeyboardMarkup с кнопкой "Загрузить документ"
    """
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📄 Загрузить документ")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Отправьте ссылку на PDF...",
    )


def get_cancel_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура с кнопкой отмены (для будущего использования).

    Returns:
        ReplyKeyboardMarkup с кнопкой "Отмена"
    """
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="❌ Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


# =============================================================================
# Inline Keyboards (кнопки под сообщениями)
# =============================================================================


def get_retry_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой повторной попытки.

    Returns:
        InlineKeyboardMarkup с кнопкой "Повторить"
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Повторить", callback_data="retry_pipeline")],
        ]
    )


def get_help_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с кнопками помощи.

    Returns:
        InlineKeyboardMarkup с кнопками помощи
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📖 Инструкция", callback_data="help_instruction"),
                InlineKeyboardButton(text="❓ FAQ", callback_data="help_faq"),
            ],
        ]
    )


# =============================================================================
# Константы для текстов кнопок (для фильтров)
# =============================================================================


class ButtonText:
    """Тексты кнопок для использования в фильтрах."""

    UPLOAD_DOCUMENT = "📄 Загрузить документ"
    CANCEL = "❌ Отмена"
