"""Ручной запуск генерации Excel отчёта по дефектам.

Примеры:
  # Из результата дедупликации
  python3 -m scripts.run_excel_generator "artifacts/dedup/dedup_doc.json"

  # С кастомным путём к файлу
  python3 -m scripts.run_excel_generator "dedup.json" --output "report.xlsx"

  # С кастомной папкой
  python3 -m scripts.run_excel_generator "dedup.json" --out-dir "artifacts/excel"
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from config import logger
from services.defect_deduplicator import DeduplicationResult
from services.excel_generator import generate_excel_report


def _load_dedup_result(json_path: Path) -> DeduplicationResult:
    """Загружает DeduplicationResult из JSON файла."""
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return DeduplicationResult(**data)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Генерация Excel отчёта по дефектам."
    )
    parser.add_argument(
        "json", help="Путь к JSON результату дедупликации (dedup_*.json)"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Полный путь к выходному Excel файлу",
    )
    parser.add_argument(
        "--out-dir",
        default="artifacts/excel",
        help="Папка для сохранения (default: artifacts/excel)",
    )
    args = parser.parse_args()

    json_path = Path(args.json).expanduser().resolve()
    if not json_path.exists():
        raise SystemExit(f"JSON файл не найден: {json_path}")

    try:
        # Загружаем результат дедупликации
        dedup_result = _load_dedup_result(json_path)
        logger.info(
            "Загружен результат: %s, %d дефектов",
            dedup_result.source_pdf,
            dedup_result.total_defects,
        )

        # Генерируем Excel
        excel_path = generate_excel_report(
            dedup_result,
            output_path=args.output,
            output_dir=args.out_dir,
        )

        print("\n=== EXCEL REPORT ===")
        print("SOURCE_PDF:", dedup_result.source_pdf)
        print("TOTAL_DEFECTS:", dedup_result.total_defects)
        print("UNIQUE_DEFECTS:", dedup_result.unique_defects)
        print("DUPLICATE_GROUPS:", dedup_result.duplicate_groups)
        print("EXCEL:", excel_path)

        return 0

    except KeyboardInterrupt:
        logger.warning("Остановлено пользователем (Ctrl+C).")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
