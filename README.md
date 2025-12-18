# FSK_pred_prod

Пайплайн обработки PDF: предпроцессинг страниц, OCR через tesseract, фильтрация релевантных страниц через LLM.

## Установка

```bash
pip install -r requirements.txt
brew install poppler tesseract tesseract-lang  # macOS
```

## Сервисы

### 1. Предпроцессинг PDF

Рендерит PDF в изображения с предобработкой (grayscale, нормализация контраста).

```bash
python3 -m scripts.run_pdf_preprocess "файл.pdf"
python3 -m scripts.run_pdf_preprocess "файл.pdf" --max-pages 2
python3 -m scripts.run_pdf_preprocess "файл.pdf" --cleanup
```

**Артефакты:** `/var/folders/.../fsk_pdf_preprocess_xxxxx/` с подпапками `rendered/`, `preprocessed/`

### 2. OCR (Tesseract)

Выполняет OCR с использованием предпроцессинга. Сохраняет JSON и TXT артефакты.

```bash
python3 -m scripts.run_pdf_ocr "файл.pdf" --out-dir "artifacts/ocr"
python3 -m scripts.run_pdf_ocr "файл.pdf" --max-pages 2 --print-text
```

**Артефакты:** JSON (структурированный результат), TXT (полный текст документа)

### 3. Flowise Page Filter (FSM)

Фильтрует релевантные страницы через Flowise LLM. FSM с двумя фазами: поиск начала и конца списка дефектов.

```bash
python3 -m scripts.run_flowise_page_filter "ocr_result.txt" --out-dir "artifacts/filter"
python3 -m scripts.run_flowise_page_filter "ocr_result.txt" --max-pages 50
python3 -m scripts.run_flowise_page_filter "ocr_result.txt" --batch-size 5
```

**Вход:** OCR .txt файл (формат: `=== Страница N ===`)
**Артефакты:** JSON с диапазоном релевантных страниц `[start_page, end_page]`

## Использование в коде

### Предпроцессинг

```python
from services.pdf_preprocessor import preprocess_pdf_to_images

result = preprocess_pdf_to_images("document.pdf")
for page in result.pages:
    print(f"Страница {page.page_number}: {page.preprocessed_path}")
result.cleanup()
```

### OCR

```python
from services.ocr_service import process_pdf_ocr, save_ocr_result

result = await process_pdf_ocr("document.pdf")
json_path, txt_path = await save_ocr_result(result, result_dir="artifacts")
print(result.document.get_all_text())
```

### Flowise Page Filter

```python
from services.flowise_page_filter import filter_relevant_pages, save_filter_result

result = filter_relevant_pages("ocr_result.txt")
print(f"Релевантные страницы: {result.relevant_pages}")
print(f"Диапазон: {result.start_page} - {result.end_page}")
save_filter_result(result, result_dir="artifacts")
```

## Настройки

Все параметры в `config.py`:
- Предпроцессинг: `PDF_RENDER_DPI`, `PDF_PREPROCESS_NORMALIZE`
- OCR: `TESSERACT_LANG`, `TESSERACT_OEM`, `TESSERACT_PSM`, `OCR_PAGE_CONCURRENCY`
- Flowise: `FLOWISE_API_URL_*`, `FLOWISE_BATCH_SIZE`, `FLOWISE_TIMEOUT_SECONDS`
