"""Ручной запуск извлечения дефектов из VLM результата.

Примеры:
  # Из VLM JSON результата
  python3 -m scripts.run_defect_extractor "artifacts/vlm/vlm_result_doc.json"

  # С выводом дефектов в консоль
  python3 -m scripts.run_defect_extractor "vlm_result.json" --print-defects

  # С кастомной папкой для результата
  python3 -m scripts.run_defect_extractor "vlm_result.json" --out-dir "artifacts/defects"
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from config import logger
from services.vlm_page_cleaner import VLMCleaningResult
from services.defect_extractor import extract_defects, save_extraction_result


def _load_vlm_result(json_path: Path) -> VLMCleaningResult:
    """Загружает VLMCleaningResult из JSON файла."""
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return VLMCleaningResult(**data)


async def async_main(args: argparse.Namespace) -> int:
    """Async точка входа."""
    json_path = Path(args.json).expanduser().resolve()
    if not json_path.exists():
        raise SystemExit(f"VLM JSON не найден: {json_path}")

    out_dir = Path(args.out_dir).expanduser().resolve()

    try:
        # Загружаем VLM результат
        vlm_result = _load_vlm_result(json_path)
        logger.info(
            "Загружен VLM результат: %s, %d страниц",
            vlm_result.source_pdf,
            vlm_result.processed_pages,
        )

        # Извлекаем дефекты
        result = await extract_defects(vlm_result)

        print("\n=== DEFECT EXTRACTION RESULT ===")
        print("SOURCE_PDF:", result.source_pdf)
        print("PAGES_PROCESSED:", result.pages_processed)
        print("TOTAL_DEFECTS:", result.total_defects)
        print("SECONDS:", f"{result.elapsed_seconds:.2f}")

        if args.print_defects and result.defects:
            print("\n=== DEFECTS ===")
            for i, defect in enumerate(result.defects, 1):
                print(f"\n--- Дефект {i} (стр. {defect.page_number}) ---")
                print(f"Помещение: {defect.room}")
                print(f"Локализация: {defect.location}")
                print(f"Тип дефекта: {defect.defect}")
                print(f"Тип работы: {defect.work_type}")
                print(f"Текст: {defect.source_text[:200]}...")

        # Сохраняем результат
        json_out = await save_extraction_result(result, result_dir=out_dir)
        print("\nJSON:", json_out)

        return 0

    except KeyboardInterrupt:
        logger.warning("Остановлено пользователем (Ctrl+C).")
        return 130


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Извлечение дефектов из VLM результата через Flowise LLM."
    )
    parser.add_argument("json", help="Путь к VLM JSON результату (vlm_result_*.json)")
    parser.add_argument(
        "--out-dir",
        default="artifacts/defects",
        help="Куда сохранить результат (default: artifacts/defects)",
    )
    parser.add_argument(
        "--print-defects",
        action="store_true",
        help="Вывести найденные дефекты в консоль",
    )
    args = parser.parse_args()

    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
