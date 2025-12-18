"""Ручной запуск дедупликации дефектов.

Примеры:
  # Из результата defect_extractor
  python3 -m scripts.run_defect_deduplicator "artifacts/defects/defects_doc.json"

  # С выводом дубликатов в консоль
  python3 -m scripts.run_defect_deduplicator "defects.json" --print-duplicates

  # С кастомной папкой для результата
  python3 -m scripts.run_defect_deduplicator "defects.json" --out-dir "artifacts/dedup"
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from config import logger
from services.defect_extractor import DefectExtractionResult
from services.defect_deduplicator import deduplicate_defects, save_dedup_result


def _load_extraction_result(json_path: Path) -> DefectExtractionResult:
    """Загружает DefectExtractionResult из JSON файла."""
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return DefectExtractionResult(**data)


async def async_main(args: argparse.Namespace) -> int:
    """Async точка входа."""
    json_path = Path(args.json).expanduser().resolve()
    if not json_path.exists():
        raise SystemExit(f"JSON файл не найден: {json_path}")

    out_dir = Path(args.out_dir).expanduser().resolve()

    try:
        # Загружаем результат извлечения дефектов
        extraction_result = _load_extraction_result(json_path)
        logger.info(
            "Загружен результат: %s, %d дефектов",
            extraction_result.source_pdf,
            extraction_result.total_defects,
        )

        # Дедупликация
        result = deduplicate_defects(extraction_result)

        print("\n=== DEDUPLICATION RESULT ===")
        print("SOURCE_PDF:", result.source_pdf)
        print("TOTAL_DEFECTS:", result.total_defects)
        print("UNIQUE_DEFECTS:", result.unique_defects)
        print("DUPLICATE_GROUPS:", result.duplicate_groups)
        print("DUPLICATES_COUNT:", result.duplicates_count)
        print("SECONDS:", f"{result.elapsed_seconds:.3f}")

        if args.print_duplicates:
            # Показываем только дефекты с дубликатами
            dups = [d for d in result.defects if d.has_duplicates]
            if dups:
                print(f"\n=== ДУБЛИКАТЫ ({len(dups)} дефектов) ===")
                for d in dups:
                    print(f"\n--- Строка {d.row_number} (стр. {d.page_number}) ---")
                    print(f"Помещение: {d.room}")
                    print(f"Локализация: {d.location}")
                    print(f"Тип дефекта: {d.defect}")
                    print(f"Дубликаты строк: {d.duplicates_str}")
            else:
                print("\nДубликатов не найдено.")

        # Сохраняем результат
        json_out = await save_dedup_result(result, result_dir=out_dir)
        print("\nJSON:", json_out)

        return 0

    except KeyboardInterrupt:
        logger.warning("Остановлено пользователем (Ctrl+C).")
        return 130


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Дедупликация дефектов по ключам (room, location, defect)."
    )
    parser.add_argument(
        "json", help="Путь к JSON результату defect_extractor (defects_*.json)"
    )
    parser.add_argument(
        "--out-dir",
        default="artifacts/dedup",
        help="Куда сохранить результат (default: artifacts/dedup)",
    )
    parser.add_argument(
        "--print-duplicates",
        action="store_true",
        help="Вывести найденные дубликаты в консоль",
    )
    args = parser.parse_args()

    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
