# FSK_pred_prod

Пайплайн обработки PDF: предпроцессинг страниц и OCR через tesseract.

## Установка

```bash
pip install -r requirements.txt
brew install poppler tesseract tesseract-lang  # macOS
```

## Сервисы

### 1. Предпроцессинг PDF

Рендерит PDF в изображения с предобработкой (grayscale, нормализация контраста).

```bash
# Весь PDF
python3 -m scripts.run_pdf_preprocess "файл.pdf"

# Первые 2 страницы
python3 -m scripts.run_pdf_preprocess "файл.pdf" --max-pages 2

# С удалением артефактов
python3 -m scripts.run_pdf_preprocess "файл.pdf" --cleanup
```

**Артефакты:** `/var/folders/.../fsk_pdf_preprocess_xxxxx/` с подпапками `rendered/`, `preprocessed/`

### 2. OCR (Tesseract)

Выполняет OCR с использованием предпроцессинга. Сохраняет JSON и TXT артефакты.

```bash
# Весь PDF с сохранением в artifacts/ocr/
python3 -m scripts.run_pdf_ocr "файл.pdf" --out-dir "artifacts/ocr"

# Первые 2 страницы с выводом текста в консоль
python3 -m scripts.run_pdf_ocr "файл.pdf" --max-pages 2 --print-text
```

**Артефакты:** JSON (структурированный результат), TXT (полный текст документа)

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
