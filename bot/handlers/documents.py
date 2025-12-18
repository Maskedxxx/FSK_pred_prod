"""Хендлеры работы с документами и запуск пайплайна анализа дефектов.

Основная логика бота: приём ссылок Google Drive и пошаговый запуск пайплайна
с отправкой статусных сообщений пользователю на каждом этапе.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from aiogram import types
from aiogram.enums import ParseMode

from config import logger
from bot.config import Messages, VLM_MAX_RETRIES, VLM_RETRY_DELAY
from bot.keyboards.main import get_main_keyboard
from services.pipeline import (
    DefectAnalysisPipeline,
    PipelineError,
    extract_google_drive_file_id,
    format_size,
)


# =============================================================================
# Вспомогательные функции
# =============================================================================

# Маппинг типов PII на русские названия для отображения в Telegram
PII_TYPE_NAMES: dict[str, str] = {
    "phone": "тел",
    "email": "email",
    "inn": "ИНН",
    "snils": "СНИЛС",
    "passport": "паспорт",
    "bank_card": "карта",
}


def is_google_drive_link(message: types.Message) -> bool:
    """Проверяет, содержит ли сообщение ссылку Google Drive.

    Args:
        message: Входящее сообщение

    Returns:
        True если сообщение содержит валидную ссылку GDrive
    """
    text = (message.text or "").strip()
    return bool(extract_google_drive_file_id(text))


async def _send_status(message: types.Message, text: str) -> types.Message:
    """Отправляет статусное сообщение с Markdown разметкой.

    Args:
        message: Исходное сообщение пользователя
        text: Текст для отправки

    Returns:
        Отправленное сообщение
    """
    return await message.answer(text, parse_mode=ParseMode.MARKDOWN)


# =============================================================================
# Хендлеры
# =============================================================================


async def handle_upload_button(message: types.Message) -> None:
    """Обрабатывает нажатие кнопки «Загрузить документ».

    Отправляет инструкцию по загрузке документа.

    Args:
        message: Входящее сообщение
    """
    await _send_status(message, Messages.UPLOAD_INSTRUCTION)


async def handle_google_drive_link(message: types.Message) -> None:
    """Обрабатывает ссылку Google Drive и запускает пайплайн анализа.

    Оркестрирует последовательный запуск всех 7 шагов пайплайна,
    отправляя пользователю статусные сообщения после каждого шага.

    Args:
        message: Входящее сообщение со ссылкой
    """
    link = (message.text or "").strip()

    # Валидация ссылки
    if not extract_google_drive_file_id(link):
        await _send_status(message, Messages.INVALID_LINK)
        return

    # Создаём пайплайн
    pipeline = DefectAnalysisPipeline(link)

    await _send_status(message, Messages.LINK_ACCEPTED)

    try:
        # === ШАГ 0: Скачивание документа ===
        await _send_status(message, Messages.STEP_DOWNLOAD_START)

        download_meta = await pipeline.download_document()

        await _send_status(
            message,
            Messages.STEP_DOWNLOAD_DONE.format(
                filename=download_meta.filename,
                size=format_size(download_meta.size_bytes),
                session=pipeline.pipeline_dir.name,
            ),
        )

        # === ШАГ 1: OCR ===
        await _send_status(message, Messages.STEP_OCR_START)

        ocr_meta = await pipeline.run_ocr()

        await _send_status(
            message,
            Messages.STEP_OCR_DONE.format(
                pages=ocr_meta.total_pages,
                duration=ocr_meta.duration,
            ),
        )

        # === ШАГ 1.5: Маскирование персональных данных ===
        await _send_status(message, Messages.STEP_PII_START)

        pii_meta = await pipeline.run_pii_masking()

        if pii_meta.has_pii:
            # Форматируем типы PII для отображения (с русскими названиями)
            pii_types_str = ", ".join(
                f"{PII_TYPE_NAMES.get(k, k)}: {v}"
                for k, v in sorted(pii_meta.pii_by_type.items())
            )
            # Форматируем список страниц без квадратных скобок
            pages_str = ", ".join(str(p) for p in pii_meta.pages_with_pii)
            await _send_status(
                message,
                Messages.STEP_PII_DONE_MASKED.format(
                    pages=pages_str,
                    count=pii_meta.total_pii_count,
                    types=pii_types_str,
                ),
            )
        else:
            await _send_status(message, Messages.STEP_PII_DONE_CLEAN)

        # === ШАГ 2: Фильтрация релевантных страниц ===
        await _send_status(message, Messages.STEP_FILTER_START)

        filter_meta = await pipeline.run_page_filter()

        if not filter_meta.relevant_pages:
            await _send_status(message, Messages.STEP_FILTER_NO_PAGES)
            return

        await _send_status(
            message,
            Messages.STEP_FILTER_DONE.format(
                relevant=len(filter_meta.relevant_pages),
                total=filter_meta.total_pages,
                start=filter_meta.start_page,
                end=filter_meta.end_page,
                duration=filter_meta.duration,
            ),
        )

        # === ШАГ 3: VLM очистка (с retry логикой) ===
        await _send_status(message, Messages.STEP_VLM_START)

        vlm_meta = None
        for attempt in range(VLM_MAX_RETRIES):
            try:
                vlm_meta = await pipeline.run_vlm_cleaning()
                break
            except Exception as e:
                is_network_error = "Connection" in str(e) or "Timeout" in str(e)
                is_last_attempt = attempt >= VLM_MAX_RETRIES - 1

                if is_network_error and not is_last_attempt:
                    await _send_status(
                        message,
                        Messages.STEP_VLM_RETRY.format(
                            attempt=attempt + 1,
                            max_attempts=VLM_MAX_RETRIES,
                        ),
                    )
                    await asyncio.sleep(VLM_RETRY_DELAY)
                    continue
                else:
                    logger.exception("VLM шаг провалился после %d попыток", attempt + 1)
                    await _send_status(message, Messages.ERROR_VLM_FAILED)
                    return

        if vlm_meta is None:
            await _send_status(message, Messages.ERROR_VLM_FAILED)
            return

        await _send_status(
            message,
            Messages.STEP_VLM_DONE.format(
                pages=vlm_meta.processed_pages,
                duration=vlm_meta.duration,
            ),
        )

        # === ШАГ 4: Извлечение дефектов ===
        await _send_status(message, Messages.STEP_EXTRACT_START)

        extract_meta = await pipeline.run_defect_extraction()

        await _send_status(
            message,
            Messages.STEP_EXTRACT_DONE.format(
                defects=extract_meta.total_defects,
                duration=extract_meta.duration,
            ),
        )

        # === ШАГ 5: Дедупликация ===
        await _send_status(message, Messages.STEP_DEDUP_START)

        dedup_meta = await pipeline.run_deduplication()

        await _send_status(
            message,
            Messages.STEP_DEDUP_DONE.format(
                total=dedup_meta.total_defects,
                unique=dedup_meta.unique_defects,
                groups=dedup_meta.duplicate_groups,
                duration=dedup_meta.duration,
            ),
        )

        # === ШАГ 6: Генерация Excel ===
        await _send_status(message, Messages.STEP_EXCEL_START)

        excel_meta = await pipeline.run_excel_generation()

        await _send_status(
            message,
            Messages.STEP_EXCEL_DONE.format(duration=excel_meta.duration),
        )

        # === ШАГ 7: Отправка результата ===
        await _send_status(message, Messages.STEP_SEND_START)

        total_duration = pipeline.total_duration()

        # Формируем caption для Excel файла
        caption = Messages.PIPELINE_DONE.format(
            filename=download_meta.filename,
            ocr_pages=ocr_meta.total_pages,
            relevant_pages=len(filter_meta.relevant_pages),
            defects=dedup_meta.total_defects,
            unique=dedup_meta.unique_defects,
            duration=total_duration,
            session=pipeline.pipeline_dir.name,
        )

        # Отправляем Excel файл
        excel_path = Path(excel_meta.excel_path)
        with open(excel_path, "rb") as excel_file:
            document = types.BufferedInputFile(
                excel_file.read(),
                filename=excel_path.name,
            )
            await message.answer_document(
                document,
                caption=caption,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=get_main_keyboard(),
            )

        logger.info(
            "Пайплайн завершён для пользователя %s: %d дефектов, %.1f сек",
            message.from_user.id if message.from_user else "unknown",
            dedup_meta.total_defects,
            total_duration,
        )

    except PipelineError as error:
        logger.warning("Ошибка пайплайна: %s", error)
        await _send_status(
            message,
            Messages.ERROR_PIPELINE.format(error=str(error)),
        )

    except Exception as error:
        logger.exception("Неожиданная ошибка пайплайна")
        await _send_status(message, Messages.ERROR_UNEXPECTED)
