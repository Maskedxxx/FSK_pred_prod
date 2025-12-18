"""Ручной запуск фильтрации релевантных страниц через Flowise LLM.

Пример:
  python3 -m scripts.run_flowise_page_filter "ocr_result.txt" --out-dir "artifacts/filter"
"""

from __future__ import annotations

import argparse
from pathlib import Path

from config import logger
from services.flowise_page_filter import filter_relevant_pages, save_filter_result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Фильтрация релевантных страниц через Flowise LLM (FSM)."
    )
    parser.add_argument("txt", help="Путь к OCR .txt файлу (формат: === Страница N ===)")
    parser.add_argument(
        "--max-pages", type=int, default=0, help="0 = без лимита (обработать все страницы)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Размер батча страниц (0 = значение из config.py)",
    )
    parser.add_argument(
        "--out-dir", default="artifacts/page_filter", help="Куда сохранить JSON результат"
    )
    args = parser.parse_args()

    txt_path = Path(args.txt).expanduser().resolve()
    if not txt_path.exists():
        raise SystemExit(f"OCR файл не найден: {txt_path}")

    max_pages = args.max_pages or None
    batch_size = args.batch_size or None
    out_dir = Path(args.out_dir).expanduser().resolve()

    try:
        result = filter_relevant_pages(txt_path, max_pages=max_pages, batch_size=batch_size)

        print("\n=== PAGE FILTER RESULT ===")
        print("TXT_PATH:", result.txt_path)
        print("TOTAL_PAGES:", result.total_pages)
        print("START_PAGE:", result.start_page)
        print("END_PAGE:", result.end_page)
        print("RELEVANT_COUNT:", result.relevant_count)
        print("RELEVANT_PAGES:", result.relevant_pages)
        print("FSM_STATE:", result.fsm_final_state)
        print("SECONDS:", f"{result.elapsed_seconds:.2f}")

        json_path = save_filter_result(result, result_dir=out_dir)
        print("JSON:", json_path)

        return 0

    except KeyboardInterrupt:
        logger.warning("Остановлено пользователем (Ctrl+C).")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
