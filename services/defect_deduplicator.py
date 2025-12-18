"""Сервис дедупликации дефектов.

Группирует дефекты по ключам (room, location, defect) и помечает дубликаты.
Не удаляет дубли, а добавляет информацию о них для отображения в Excel.

Публичный API:
    - deduplicate_defects() — основная функция обработки
    - save_dedup_result() — сохранение результата в JSON
"""

from __future__ import annotations

import time
from collections import defaultdict
from pathlib import Path

from pydantic import BaseModel, Field

from config import logger
from services.defect_extractor import DefectExtractionResult, ExtractedDefect


# =============================================================================
# Pydantic-модели результата
# =============================================================================


class DeduplicatedDefect(BaseModel):
    """Дефект с информацией о дубликатах."""

    # Оригинальные поля из ExtractedDefect
    source_text: str = Field(..., description="Исходный текст описания дефекта")
    room: str = Field(..., description="Помещение")
    location: str = Field(..., description="Локализация дефекта")
    defect: str = Field(..., description="Тип дефекта")
    work_type: str = Field(..., description="Тип работы")
    page_number: int = Field(..., description="Номер страницы")

    # Новые поля для дедупликации
    row_number: int = Field(..., description="Номер строки в списке (1-based)")
    duplicates: list[int] = Field(
        default_factory=list,
        description="Номера строк дубликатов (пусто если нет дублей)",
    )

    @property
    def has_duplicates(self) -> bool:
        """Есть ли дубликаты у этого дефекта."""
        return len(self.duplicates) > 0

    @property
    def duplicates_str(self) -> str:
        """Строка с номерами дубликатов для Excel (пусто если нет)."""
        if not self.duplicates:
            return ""
        return ", ".join(str(n) for n in self.duplicates)


class DeduplicationResult(BaseModel):
    """Результат дедупликации дефектов."""

    source_pdf: str = Field(..., description="Путь к исходному PDF")
    total_defects: int = Field(..., description="Общее количество дефектов")
    unique_defects: int = Field(..., description="Количество уникальных дефектов")
    duplicate_groups: int = Field(..., description="Количество групп дубликатов")
    defects: list[DeduplicatedDefect] = Field(
        default_factory=list, description="Список дефектов с пометками дубликатов"
    )
    elapsed_seconds: float = Field(default=0.0, description="Время обработки (сек)")

    @property
    def duplicates_count(self) -> int:
        """Количество дефектов-дубликатов."""
        return self.total_defects - self.unique_defects


# =============================================================================
# Основная логика
# =============================================================================


def _make_dedup_key(defect: ExtractedDefect) -> tuple[str, str, str]:
    """Создаёт ключ для группировки дефектов.

    Ключ: (room, location, defect) — нормализованные к нижнему регистру.
    """
    return (
        defect.room.strip().lower(),
        defect.location.strip().lower(),
        defect.defect.strip().lower(),
    )


def deduplicate_defects(extraction_result: DefectExtractionResult) -> DeduplicationResult:
    """Помечает дубликаты в списке дефектов.

    Дубликаты определяются по совпадению ключа (room, location, defect).
    Дефекты НЕ удаляются, а помечаются номерами строк других дефектов
    из той же группы.

    Args:
        extraction_result: Результат извлечения дефектов из defect_extractor

    Returns:
        DeduplicationResult с помеченными дубликатами
    """
    start_time = time.perf_counter()

    defects = extraction_result.defects
    total = len(defects)

    if total == 0:
        logger.info("Нет дефектов для дедупликации")
        return DeduplicationResult(
            source_pdf=extraction_result.source_pdf,
            total_defects=0,
            unique_defects=0,
            duplicate_groups=0,
            defects=[],
            elapsed_seconds=0.0,
        )

    logger.info("Дедупликация: %d дефектов из %s", total, extraction_result.source_pdf)

    # Шаг 1: Группируем дефекты по ключу
    # key -> list of (row_number, defect)
    groups: dict[tuple[str, str, str], list[tuple[int, ExtractedDefect]]] = defaultdict(list)

    for idx, defect in enumerate(defects):
        row_number = idx + 1  # 1-based для Excel
        key = _make_dedup_key(defect)
        groups[key].append((row_number, defect))

    # Шаг 2: Для каждой группы определяем дубликаты
    # row_number -> list of duplicate row_numbers
    duplicates_map: dict[int, list[int]] = {}

    duplicate_groups_count = 0
    for key, group_items in groups.items():
        if len(group_items) > 1:
            # Это группа дубликатов
            duplicate_groups_count += 1
            row_numbers = [row for row, _ in group_items]

            # Для каждого дефекта в группе — список остальных
            for row_num in row_numbers:
                duplicates_map[row_num] = [r for r in row_numbers if r != row_num]

            logger.debug(
                "Группа дубликатов: %s → строки %s",
                key,
                row_numbers,
            )

    # Шаг 3: Создаём результат с пометками
    deduplicated: list[DeduplicatedDefect] = []

    for idx, defect in enumerate(defects):
        row_number = idx + 1
        dups = duplicates_map.get(row_number, [])

        deduplicated.append(
            DeduplicatedDefect(
                source_text=defect.source_text,
                room=defect.room,
                location=defect.location,
                defect=defect.defect,
                work_type=defect.work_type,
                page_number=defect.page_number,
                row_number=row_number,
                duplicates=dups,
            )
        )

    unique_count = len(groups)  # количество уникальных ключей
    elapsed = time.perf_counter() - start_time

    result = DeduplicationResult(
        source_pdf=extraction_result.source_pdf,
        total_defects=total,
        unique_defects=unique_count,
        duplicate_groups=duplicate_groups_count,
        defects=deduplicated,
        elapsed_seconds=round(elapsed, 3),
    )

    logger.info(
        "Дедупликация завершена: %d дефектов, %d уникальных, %d групп дублей (%.3f сек)",
        total,
        unique_count,
        duplicate_groups_count,
        elapsed,
    )

    return result


# =============================================================================
# Сохранение результата
# =============================================================================


async def save_dedup_result(
    result: DeduplicationResult,
    result_dir: str | Path = "artifacts/dedup",
) -> str:
    """Сохраняет результат дедупликации в JSON файл.

    Args:
        result: Результат дедупликации
        result_dir: Папка для сохранения

    Returns:
        Путь к сохранённому JSON файлу
    """
    result_path = Path(result_dir).expanduser().resolve()
    result_path.mkdir(parents=True, exist_ok=True)

    pdf_stem = Path(result.source_pdf).stem
    json_file = result_path / f"dedup_{pdf_stem}.json"

    logger.info("Сохраняю результат дедупликации: %s", json_file)

    json_file.write_text(result.model_dump_json(indent=2), encoding="utf-8")

    logger.info("Результат дедупликации сохранён")
    return str(json_file)
