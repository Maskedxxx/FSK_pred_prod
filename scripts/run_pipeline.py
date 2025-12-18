"""Запуск полного пайплайна анализа дефектов.

Примеры:
  # Из ссылки Google Drive
  python3 -m scripts.run_pipeline "https://drive.google.com/file/d/xxx/view"

  # С кастомной папкой для артефактов
  python3 -m scripts.run_pipeline "https://drive.google.com/..." --out-dir "my_results"
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from config import logger
from services.pipeline import run_pipeline, PipelineResult


def _print_result(result: PipelineResult) -> None:
    """Выводит красивую сводку результата пайплайна."""
    print("\n" + "=" * 60)
    print("РЕЗУЛЬТАТ ПАЙПЛАЙНА")
    print("=" * 60)

    print(f"Папка артефактов: {result.pipeline_dir}")
    print(f"Исходный URL: {result.source_url}")

    if result.pdf_path:
        print(f"PDF файл: {result.pdf_path}")

    print("\n--- МЕТРИКИ ШАГОВ ---")

    if result.download:
        print(f"[1] Download: {result.download.filename} "
              f"({result.download.size_bytes / 1024 / 1024:.2f} МБ) — {result.download.duration:.2f}с")

    if result.ocr:
        print(f"[2] OCR: {result.ocr.total_pages} страниц — {result.ocr.duration:.2f}с")

    if result.filter:
        print(f"[3] Filter: {len(result.filter.relevant_pages)}/{result.filter.total_pages} "
              f"релевантных [{result.filter.start_page}-{result.filter.end_page}] — {result.filter.duration:.2f}с")

    if result.vlm:
        print(f"[4] VLM: {result.vlm.processed_pages} страниц — {result.vlm.duration:.2f}с")

    if result.extraction:
        print(f"[5] Extraction: {result.extraction.total_defects} дефектов — {result.extraction.duration:.2f}с")

    if result.deduplication:
        print(f"[6] Dedup: {result.deduplication.unique_defects}/{result.deduplication.total_defects} "
              f"уникальных, {result.deduplication.duplicate_groups} групп — {result.deduplication.duration:.2f}с")

    if result.excel:
        print(f"[7] Excel: {result.excel.excel_path} — {result.excel.duration:.2f}с")

    print(f"\nОбщее время: {result.total_duration:.2f}с")

    if result.errors:
        print("\n--- ОШИБКИ ---")
        for err in result.errors:
            print(f"  ! {err}")

    if result.excel_path:
        print(f"\n>>> EXCEL ОТЧЁТ: {result.excel_path}")
    else:
        print("\n>>> Excel отчёт не создан (проверьте ошибки выше)")


async def _async_main(url: str, out_dir: str | None) -> int:
    """Асинхронная точка входа."""
    pipeline_dir = Path(out_dir) if out_dir else None

    try:
        result = await run_pipeline(url, pipeline_dir)
        _print_result(result)

        if result.errors:
            return 1
        return 0

    except KeyboardInterrupt:
        logger.warning("Остановлено пользователем (Ctrl+C)")
        return 130


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Полный пайплайн анализа дефектов из PDF."
    )
    parser.add_argument(
        "url",
        help="Ссылка на PDF в Google Drive",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Папка для артефактов (default: result/YYYYMMDD_HHMMSS)",
    )
    args = parser.parse_args()

    return asyncio.run(_async_main(args.url, args.out_dir))


if __name__ == "__main__":
    raise SystemExit(main())
