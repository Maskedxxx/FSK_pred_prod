"""Ручной запуск VLM очистки страниц PDF.

Примеры:
  # Обработать конкретные страницы
  python3 -m scripts.run_vlm_page_cleaner "document.pdf" --pages 5,6,7,8

  # Обработать диапазон страниц
  python3 -m scripts.run_vlm_page_cleaner "document.pdf" --pages 5-10

  # Обработать с fallback из OCR txt файла
  python3 -m scripts.run_vlm_page_cleaner "document.pdf" --pages 5-10 --ocr-txt "ocr_result.txt"

  # Вывести очищенный текст в консоль
  python3 -m scripts.run_vlm_page_cleaner "document.pdf" --pages 1,2,3 --print-text
"""

from __future__ import annotations

import argparse
import asyncio
import re
from pathlib import Path

from config import logger
from services.vlm_page_cleaner import clean_relevant_pages, save_vlm_result


def _parse_pages_arg(pages_str: str) -> list[int]:
    """Парсит строку с номерами страниц.

    Поддерживает:
      - Одиночные: "5"
      - Списки: "1,3,5,7"
      - Диапазоны: "5-10"
      - Комбинации: "1,3,5-10,15"
    """
    result: list[int] = []

    for part in pages_str.split(","):
        part = part.strip()
        if "-" in part:
            # Диапазон
            match = re.match(r"(\d+)-(\d+)", part)
            if match:
                start, end = int(match.group(1)), int(match.group(2))
                result.extend(range(start, end + 1))
        elif part.isdigit():
            result.append(int(part))

    return sorted(set(result))


def _parse_ocr_txt(txt_path: Path) -> dict[int, str]:
    """Парсит OCR txt файл (формат: === Страница N ===) в словарь."""
    if not txt_path.exists():
        return {}

    content = txt_path.read_text(encoding="utf-8")
    raw_text_by_page: dict[int, str] = {}

    # Разбиваем по маркерам страниц
    pattern = r"=== Страница (\d+)[^=]*==="
    parts = re.split(pattern, content)

    # parts: ['', '1', 'текст1', '2', 'текст2', ...]
    for i in range(1, len(parts) - 1, 2):
        page_num = int(parts[i])
        page_text = parts[i + 1].strip()
        raw_text_by_page[page_num] = page_text

    return raw_text_by_page


async def async_main(args: argparse.Namespace) -> int:
    """Async точка входа."""
    pdf_path = Path(args.pdf).expanduser().resolve()
    if not pdf_path.exists():
        raise SystemExit(f"PDF не найден: {pdf_path}")

    page_numbers = _parse_pages_arg(args.pages)
    if not page_numbers:
        raise SystemExit("Не указаны страницы для обработки (--pages)")

    out_dir = Path(args.out_dir).expanduser().resolve()

    # Опциональный fallback из OCR
    raw_text_by_page: dict[int, str] | None = None
    if args.ocr_txt:
        ocr_txt_path = Path(args.ocr_txt).expanduser().resolve()
        if ocr_txt_path.exists():
            raw_text_by_page = _parse_ocr_txt(ocr_txt_path)
            logger.info("Загружен OCR fallback: %d страниц", len(raw_text_by_page))

    try:
        result = await clean_relevant_pages(
            pdf_path=pdf_path,
            page_numbers=page_numbers,
            raw_text_by_page=raw_text_by_page,
        )

        print("\n=== VLM CLEANING RESULT ===")
        print("PDF:", result.source_pdf)
        print("PROCESSED_PAGES:", result.processed_pages)
        print("SECONDS:", f"{result.elapsed_seconds:.2f}")

        if args.print_text:
            print("\n=== CLEANED TEXT ===")
            for page in result.cleaned_pages:
                print(f"\n--- Страница {page.page_number} ---")
                print(page.cleaned_text[:2000] if page.cleaned_text else "(пусто)")
                if len(page.cleaned_text) > 2000:
                    print(f"... (ещё {len(page.cleaned_text) - 2000} символов)")

        json_path, txt_path = await save_vlm_result(result, result_dir=out_dir)
        print("\nJSON:", json_path)
        print("TXT:", txt_path)

        return 0

    except KeyboardInterrupt:
        logger.warning("Остановлено пользователем (Ctrl+C).")
        return 130


def main() -> int:
    parser = argparse.ArgumentParser(
        description="VLM очистка страниц PDF через Flowise Vision API."
    )
    parser.add_argument("pdf", help="Путь к PDF файлу")
    parser.add_argument(
        "--pages",
        required=True,
        help="Номера страниц: '5', '1,3,5', '5-10', '1,3,5-10'",
    )
    parser.add_argument(
        "--ocr-txt",
        default=None,
        help="OCR .txt файл для fallback (формат: === Страница N ===)",
    )
    parser.add_argument(
        "--out-dir",
        default="artifacts/vlm",
        help="Куда сохранить результат (default: artifacts/vlm)",
    )
    parser.add_argument(
        "--print-text",
        action="store_true",
        help="Вывести очищенный текст в консоль",
    )
    args = parser.parse_args()

    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
