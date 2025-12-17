"""Утилиты для логирования.

Зачем отдельный модуль:
- конфиг (settings) остаётся единым источником правды в `FSK_pred_prod/config.py`
- а код инициализации/хелперы для логирования живут в одном месте и переиспользуются сервисами.
"""

from __future__ import annotations

import logging


def setup_console_logging(*, level: int, fmt: str) -> None:
    """Настраивает логирование в консоль.

    Важно: вызывается один раз при старте приложения (обычно из config.py).
    """
    logging.basicConfig(level=level, format=fmt)

