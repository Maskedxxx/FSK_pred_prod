"""Ручной запуск OCR (PDF -> text) через tesseract.

Пример:
  python3 -m scripts.run_pdf_ocr "data/your.pdf" --max-pages 2 --out-dir "artifacts/ocr"
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from config import logger
from services.ocr_service import process_pdf_ocr, save_ocr_result


async def _main_async(
    pdf: Path, *, max_pages: int | None, concurrency: int | None, out_dir: Path, print_text: bool
) -> int:
    result = await process_pdf_ocr(pdf, max_pages=max_pages, concurrency=concurrency)

    print("\n=== OCR RESULT ===")
    print("PDF:", result.pdf_path)
    print("PAGES:", result.document.total_pages)
    print("SECONDS:", f"{result.seconds:.2f}")
    print("PREPROCESS_WORKDIR:", result.preprocess_workdir)
    print("PREPROCESS_WORKDIR_KEPT:", result.preprocess_workdir_kept)

    json_path, txt_path = await save_ocr_result(result, result_dir=out_dir)
    print("JSON:", json_path)
    print("TXT:", txt_path)

    if print_text:
        print("\n=== FULL TEXT ===")
        print(result.document.get_all_text())

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="OCR PDF через tesseract (с предпроцессингом страниц).")
    parser.add_argument("pdf", help="Путь к PDF файлу")
    parser.add_argument("--max-pages", type=int, default=0, help="0 = без лимита (обработать все страницы)")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=0,
        help="Сколько страниц OCR обрабатывать параллельно (0 = значение из config.py).",
    )
    parser.add_argument("--out-dir", default="artifacts/ocr", help="Куда сохранить JSON/TXT артефакты")
    parser.add_argument("--print-text", action="store_true", help="Печатать распознанный текст в консоль")
    args = parser.parse_args()

    pdf = Path(args.pdf).expanduser().resolve()
    if not pdf.exists():
        raise SystemExit(f"PDF не найден: {pdf}")

    max_pages = args.max_pages or None
    concurrency = args.concurrency or None
    out_dir = Path(args.out_dir).expanduser().resolve()

    try:
        return asyncio.run(
            _main_async(
                pdf,
                max_pages=max_pages,
                concurrency=concurrency,
                out_dir=out_dir,
                print_text=args.print_text,
            )
        )
    except KeyboardInterrupt:
        logger.warning("Остановлено пользователем (Ctrl+C).")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
