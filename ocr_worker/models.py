"""Pydantic модели для OCR Worker API.

Модели соответствуют структурам из services/ocr_service.py для совместимости.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class TextElement(BaseModel):
    """Текстовый элемент страницы (минимальная структура для дальнейшего пайплайна)."""

    category: str = Field(..., description="Категория элемента (например: 'ocr').")
    content: str = Field(..., description="Текст элемента.")
    type: Literal["text"] = Field(default="text", description="Тип элемента (фиксированный).")


class PageData(BaseModel):
    """Данные одной страницы документа после OCR."""

    page_number: int = Field(..., description="Номер страницы (1..N).")
    full_text: str = Field(..., description="Полный текст страницы.")
    elements: list[TextElement] = Field(
        default_factory=list,
        description="Список элементов текста (сейчас один элемент на страницу).",
    )
    total_elements: int = Field(..., description="Количество элементов на странице.")


class DocumentData(BaseModel):
    """Результат OCR по всему документу."""

    filename: str = Field(..., description="Имя исходного файла (для идентификации).")
    pages: list[PageData] = Field(default_factory=list, description="Страницы документа с OCR-текстом.")
    total_pages: int = Field(..., description="Количество страниц в документе.")

    def get_all_text(self) -> str:
        """Склеивает документ в удобный .txt формат."""
        chunks: list[str] = []
        for page in self.pages:
            chunks.append(f"=== Страница {page.page_number} ===\n{page.full_text}".rstrip())
        return "\n\n".join(chunks).rstrip() + "\n"


class OCRResponse(BaseModel):
    """Ответ API /ocr endpoint."""

    success: bool = Field(..., description="Успешно ли выполнен OCR.")
    filename: str = Field(..., description="Имя обработанного файла.")
    document: DocumentData | None = Field(None, description="Результат OCR (если success=True).")
    seconds: float = Field(..., description="Время выполнения OCR в секундах.")
    total_pages: int = Field(..., description="Количество обработанных страниц.")
    error: str | None = Field(None, description="Сообщение об ошибке (если success=False).")


class HealthResponse(BaseModel):
    """Ответ API /health endpoint."""

    status: Literal["ok", "error"] = Field(..., description="Статус сервиса.")
    tesseract: bool = Field(..., description="Доступен ли tesseract.")
    poppler: bool = Field(..., description="Доступен ли poppler (pdftoppm).")
    error: str | None = Field(None, description="Сообщение об ошибке.")
