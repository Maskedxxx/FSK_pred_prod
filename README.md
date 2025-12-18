# FSK_pred_prod

Пайплайн обработки PDF технических отчётов: предпроцессинг, OCR, фильтрация релевантных страниц, VLM очистка, извлечение дефектов.

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

### 4. VLM Page Cleaner (Vision)

Очищает и структурирует релевантные страницы через Flowise Vision API. Конвертирует страницы в изображения, отправляет в VLM, возвращает Markdown.

```bash
python3 -m scripts.run_vlm_page_cleaner "файл.pdf" --pages 5-10
python3 -m scripts.run_vlm_page_cleaner "файл.pdf" --pages 5,6,7,8 --print-text
python3 -m scripts.run_vlm_page_cleaner "файл.pdf" --pages 5-10 --ocr-txt "ocr_result.txt"
```

**Вход:** PDF файл + номера страниц (из Page Filter)
**Артефакты:** JSON (структурированный результат), TXT (очищенный Markdown)

### 5. Defect Extractor (LLM)

Извлекает структурированный список дефектов из VLM-очищенных страниц через Flowise LLM. Постраничная обработка с контекстом соседних страниц.

```bash
python3 -m scripts.run_defect_extractor "vlm_result.json"
python3 -m scripts.run_defect_extractor "vlm_result.json" --print-defects
python3 -m scripts.run_defect_extractor "vlm_result.json" --out-dir "artifacts/defects"
```

**Вход:** VLM JSON результат (vlm_result_*.json)
**Артефакты:** JSON со списком дефектов (source_text, room, location, defect, work_type)

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

### VLM Page Cleaner

```python
from services.vlm_page_cleaner import clean_relevant_pages, save_vlm_result

result = await clean_relevant_pages(
    pdf_path="document.pdf",
    page_numbers=[5, 6, 7, 8],  # из filter_relevant_pages
    raw_text_by_page={5: "...", 6: "..."}  # fallback из OCR
)
json_path, txt_path = await save_vlm_result(result, result_dir="artifacts/vlm")
print(result.get_all_text())
```

### Defect Extractor

```python
from services.defect_extractor import extract_defects, save_extraction_result

result = await extract_defects(vlm_result)  # из VLM Page Cleaner
print(f"Найдено дефектов: {result.total_defects}")
for defect in result.defects:
    print(f"{defect.room} / {defect.location}: {defect.defect}")
json_path = await save_extraction_result(result, result_dir="artifacts/defects")
```

## Настройки

Все параметры в `config.py`:
- Предпроцессинг: `PDF_RENDER_DPI`, `PDF_PREPROCESS_NORMALIZE`
- OCR: `TESSERACT_LANG`, `TESSERACT_OEM`, `TESSERACT_PSM`, `OCR_PAGE_CONCURRENCY`
- Flowise: `FLOWISE_API_URL_*`, `FLOWISE_BATCH_SIZE`, `FLOWISE_TIMEOUT_SECONDS`
- VLM: `FLOWISE_API_URL_VLM_CLEAN`, `VLM_RENDER_DPI`, `VLM_IMAGE_TARGET_*`, `VLM_TIMEOUT_SECONDS`
- Defect Extractor: `FLOWISE_API_URL_DEFECT_EXTRACT`, `DEFECT_EXTRACTION_CONCURRENCY`, `DEFECT_EXTRACTION_CONTEXT_CHARS`
