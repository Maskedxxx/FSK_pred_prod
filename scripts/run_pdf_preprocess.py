"""Ручной запуск предпроцессинга PDF (PDF -> изображения страниц).

Пример:
  python3 -m FSK_pred_prod.scripts.run_pdf_preprocess "data/input/285 ... .pdf" --max-pages 2
"""

from __future__ import annotations

import argparse
from pathlib import Path

from config import logger
from services.pdf_preprocessor import preprocess_pdf_to_images


def main() -> None:
    parser = argparse.ArgumentParser(description="FSK_pred_prod: PDF предпроцессинг (рендер + предобработка).")
    parser.add_argument("pdf", help="Путь к PDF файлу")
    parser.add_argument("--max-pages", type=int, default=0, help="0 = без лимита (обработать все страницы)")
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Удалить временную папку (workdir) после завершения. По умолчанию workdir остаётся для проверки.",
    )
    args = parser.parse_args()

    pdf = Path(args.pdf).expanduser().resolve()
    if not pdf.exists():
        raise SystemExit(f"PDF не найден: {pdf}")

    max_pages = args.max_pages or None
    result = preprocess_pdf_to_images(pdf, max_pages=max_pages)

    # Лаконичное резюме для человека.
    print("\n=== RESULT ===")
    print("WORKDIR:", result.workdir)
    print("PAGES:", len(result.pages))

    # Быстрая sanity-проверка: все файлы реально созданы.
    missing = []
    for page in result.pages:
        if not page.rendered_path.exists():
            missing.append(str(page.rendered_path))
        if not page.preprocessed_path.exists():
            missing.append(str(page.preprocessed_path))

    if missing:
        logger.warning("Не найдены файлы (проверьте права/диск): %s", missing[:5])
    else:
        logger.info("Sanity-check OK: все артефакты существуют на диске.")

    if args.cleanup:
        result.cleanup()
        logger.info("workdir удалён: %s", result.workdir)


if __name__ == "__main__":
    main()

