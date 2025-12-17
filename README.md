# FSK_pred_prod

Сервис предпроцессинга PDF для подготовки к OCR. Рендерит страницы в изображения и выполняет предобработку (grayscale, нормализация контраста).

## Установка

```bash
pip install -r requirements.txt
brew install poppler  # macOS
```

## Запуск

```bash
# Весь PDF
python -m services.pdf_preprocessor "файл.pdf"

# Первые 2 страницы
python -m services.pdf_preprocessor "файл.pdf" --max-pages 2

# С удалением артефактов
python -m services.pdf_preprocessor "файл.pdf" --cleanup
```

## Хранение артефактов

Временные файлы сохраняются в `/var/folders/.../fsk_pdf_preprocess_xxxxx/` (не удаляются автоматически):

```
workdir/
├── rendered/          # Отрендеренные страницы (JPEG)
└── preprocessed/      # Предобработанные для OCR (PNG, grayscale)
```

Удаление: флаг `--cleanup`, метод `result.cleanup()` или вручную.

## Использование в коде

```python
from services.pdf_preprocessor import preprocess_pdf_to_images

result = preprocess_pdf_to_images("document.pdf")

for page in result.pages:
    print(f"Страница {page.page_number}: {page.preprocessed_path}")

result.cleanup()
```

## Настройки

Все параметры в `config.py` (DPI, форматы, логирование).
