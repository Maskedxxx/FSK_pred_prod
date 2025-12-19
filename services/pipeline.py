"""Оркестратор полного пайплайна анализа дефектов.

Шаги пайплайна:
    1. Download — скачивание PDF из Google Drive
    2. OCR — распознавание текста
    3. Page Filter — фильтрация релевантных страниц (FSM)
    4. VLM Clean — очистка страниц через Vision LLM
    5. Defect Extract — извлечение дефектов
    6. Deduplicate — пометка дубликатов
    7. Excel — генерация отчёта

Публичный API:
    - DefectAnalysisPipeline — класс-оркестратор
    - run_pipeline() — запуск полного пайплайна по URL
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import aiohttp

from config import logger
from services.ocr_service import process_pdf_ocr, save_ocr_result, OCRResult
from services.flowise_page_filter import filter_relevant_pages, PageFilterResult
from utils.pii_masker import mask_pii_in_document, DocumentMaskingResult
from services.vlm_page_cleaner import clean_relevant_pages, save_vlm_result, VLMCleaningResult
from services.defect_extractor import extract_defects, save_extraction_result, DefectExtractionResult
from services.defect_deduplicator import deduplicate_defects, save_dedup_result, DeduplicationResult
from services.excel_generator import generate_excel_report


# =============================================================================
# Ошибки пайплайна
# =============================================================================


class PipelineError(Exception):
    """Базовая ошибка пайплайна анализа дефектов."""


# =============================================================================
# Метаданные шагов пайплайна
# =============================================================================


@dataclass
class DownloadMetadata:
    """Метаданные скачивания PDF."""

    filename: str
    size_bytes: int
    local_path: Path
    duration: float


@dataclass
class OCRMetadata:
    """Метаданные OCR шага."""

    total_pages: int
    json_path: Path
    txt_path: Path
    duration: float


@dataclass
class PIIMaskingMetadata:
    """Метаданные маскирования персональных данных."""

    pages_with_pii: list[int]
    total_pii_count: int
    pii_by_type: dict[str, int]
    pii_by_page: dict[int, dict[str, int]]  # {page_num: {pii_type: count}}
    duration: float

    @property
    def has_pii(self) -> bool:
        return len(self.pages_with_pii) > 0


@dataclass
class FilterMetadata:
    """Метаданные фильтрации страниц."""

    total_pages: int
    relevant_pages: list[int]
    start_page: int | None
    end_page: int | None
    duration: float


@dataclass
class VLMMetadata:
    """Метаданные VLM очистки."""

    processed_pages: int
    json_path: Path
    txt_path: Path
    duration: float


@dataclass
class ExtractionMetadata:
    """Метаданные извлечения дефектов."""

    total_defects: int
    pages_processed: int
    json_path: Path
    duration: float


@dataclass
class DeduplicationMetadata:
    """Метаданные дедупликации."""

    total_defects: int
    unique_defects: int
    duplicate_groups: int
    json_path: Path
    duration: float


@dataclass
class ExcelMetadata:
    """Метаданные генерации Excel."""

    excel_path: Path
    duration: float


@dataclass
class PipelineResult:
    """Полный результат пайплайна."""

    pipeline_dir: Path
    source_url: str
    pdf_path: Path | None = None
    excel_path: Path | None = None
    total_duration: float = 0.0

    # Метаданные шагов
    download: DownloadMetadata | None = None
    ocr: OCRMetadata | None = None
    pii_masking: PIIMaskingMetadata | None = None
    filter: FilterMetadata | None = None
    vlm: VLMMetadata | None = None
    extraction: ExtractionMetadata | None = None
    deduplication: DeduplicationMetadata | None = None
    excel: ExcelMetadata | None = None

    # Ошибки
    errors: list[str] = field(default_factory=list)


# =============================================================================
# Утилиты Google Drive
# =============================================================================


def extract_google_drive_file_id(url: str) -> str | None:
    """Извлекает идентификатор файла из ссылки Google Drive."""
    if not url:
        return None

    parsed = urlparse(url.strip())
    if "drive.google." not in parsed.netloc:
        return None

    path_parts = [part for part in parsed.path.split("/") if part]

    # Ссылки вида /file/d/<file_id>/...
    if len(path_parts) >= 3 and path_parts[0] == "file" and path_parts[1] == "d":
        return path_parts[2]

    # Ссылки вида /uc или /open, ID в query параметрах
    query_params = parse_qs(parsed.query)
    if "id" in query_params and query_params["id"]:
        return query_params["id"][0]

    return None


def build_direct_download_url(file_id: str) -> str:
    """Формирует ссылку для прямого скачивания PDF из Google Drive."""
    return f"https://drive.google.com/uc?export=download&id={file_id}"


def _safe_filename(filename: str, default: str) -> str:
    """Приводит имя файла к безопасному формату."""
    name = filename.strip() if filename else default
    if not name.lower().endswith(".pdf"):
        name = f"{name}.pdf"
    sanitized = "".join(ch for ch in name if ch.isalnum() or ch in {"_", "-", "."})
    return sanitized or f"{default}.pdf"


def format_size(size_bytes: int) -> str:
    """Возвращает размер файла в человекочитаемом формате."""
    if size_bytes < 1024:
        return f"{size_bytes} Б"
    kilobytes = size_bytes / 1024
    if kilobytes < 1024:
        return f"{kilobytes:.1f} КБ"
    megabytes = kilobytes / 1024
    return f"{megabytes:.2f} МБ"


# =============================================================================
# Оркестратор пайплайна
# =============================================================================


class DefectAnalysisPipeline:
    """Оркестратор шагов анализа дефектов."""

    def __init__(self, source_url: str, pipeline_dir: Path | None = None):
        """
        Args:
            source_url: Ссылка на PDF в Google Drive
            pipeline_dir: Папка для артефактов (если None — создаётся автоматически)
        """
        self.source_url = source_url.strip()
        self.pipeline_dir = pipeline_dir or self._create_pipeline_dir()
        self.started_at = time.perf_counter()

        # Внутренние данные для передачи между шагами
        self._file_id: str | None = None
        self._pdf_path: Path | None = None
        self._ocr_result: OCRResult | None = None
        self._pii_masking_result: DocumentMaskingResult | None = None
        self._filter_result: PageFilterResult | None = None
        self._vlm_result: VLMCleaningResult | None = None
        self._extraction_result: DefectExtractionResult | None = None
        self._dedup_result: DeduplicationResult | None = None

        # Метаданные
        self._download_meta: DownloadMetadata | None = None
        self._ocr_meta: OCRMetadata | None = None
        self._pii_meta: PIIMaskingMetadata | None = None
        self._filter_meta: FilterMetadata | None = None
        self._vlm_meta: VLMMetadata | None = None
        self._extraction_meta: ExtractionMetadata | None = None
        self._dedup_meta: DeduplicationMetadata | None = None
        self._excel_meta: ExcelMetadata | None = None

        self._errors: list[str] = []

    @staticmethod
    def _create_pipeline_dir() -> Path:
        """Создаёт уникальную директорию для артефактов."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        pipeline_dir = Path("result") / timestamp
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        return pipeline_dir

    def total_duration(self) -> float:
        """Возвращает общее время выполнения пайплайна в секундах."""
        return time.perf_counter() - self.started_at

    # -------------------------------------------------------------------------
    # Шаг 1: Скачивание PDF
    # -------------------------------------------------------------------------

    async def download_document(self) -> DownloadMetadata:
        """Скачивает PDF из Google Drive."""
        logger.info("=" * 60)
        logger.info("ШАГ 1: СКАЧИВАНИЕ PDF")
        logger.info("=" * 60)

        file_id = extract_google_drive_file_id(self.source_url)
        if not file_id:
            raise PipelineError("Не удалось определить ID файла из ссылки Google Drive.")

        direct_url = build_direct_download_url(file_id)
        start = time.perf_counter()

        async with aiohttp.ClientSession() as session:
            async with session.get(direct_url) as response:
                if response.status != 200:
                    raise PipelineError(f"Ошибка загрузки: HTTP {response.status}")

                # Извлекаем имя файла из заголовков
                disposition = response.headers.get("Content-Disposition", "")
                extracted = None
                if "filename*=" in disposition:
                    extracted = disposition.split("filename*=")[-1].split(";")[0]
                    if "''" in extracted:
                        extracted = extracted.split("''", maxsplit=1)[-1]
                    extracted = extracted.strip('"')
                if not extracted and "filename=" in disposition:
                    extracted = disposition.split("filename=")[-1].split(";")[0].strip('"')
                if not extracted:
                    extracted = f"document_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

                local_filename = _safe_filename(extracted, f"document_{file_id}")
                local_path = self.pipeline_dir / local_filename

                # Проверяем что это PDF
                first_chunk = await response.content.read(1024)

                if first_chunk.startswith(b"<!DOCTYPE html") or first_chunk.startswith(b"<html"):
                    raise PipelineError(
                        "Google Drive вернул HTML вместо PDF. "
                        "Проверьте доступ к файлу (должен быть публичный)."
                    )

                if not first_chunk.startswith(b"%PDF-"):
                    raise PipelineError("Скачанный файл не является PDF.")

                # Сохраняем файл
                with open(local_path, "wb") as f:
                    f.write(first_chunk)
                    async for chunk in response.content.iter_chunked(65536):
                        f.write(chunk)

        duration = time.perf_counter() - start
        size_bytes = os.path.getsize(local_path)

        self._file_id = file_id
        self._pdf_path = local_path

        meta = DownloadMetadata(
            filename=local_filename,
            size_bytes=size_bytes,
            local_path=local_path,
            duration=duration,
        )
        self._download_meta = meta

        logger.info(
            "PDF скачан: %s (%s) за %.2f с",
            local_filename,
            format_size(size_bytes),
            duration,
        )
        return meta

    # -------------------------------------------------------------------------
    # Шаг 2: OCR
    # -------------------------------------------------------------------------

    async def run_ocr(self) -> OCRMetadata:
        """Выполняет OCR обработку PDF."""
        logger.info("=" * 60)
        logger.info("ШАГ 2: OCR")
        logger.info("=" * 60)

        if not self._pdf_path:
            raise PipelineError("PDF файл не найден. Сначала выполните download_document().")

        start = time.perf_counter()

        result = await process_pdf_ocr(self._pdf_path)
        json_path, txt_path = await save_ocr_result(result, result_dir=self.pipeline_dir)

        duration = time.perf_counter() - start

        self._ocr_result = result

        meta = OCRMetadata(
            total_pages=result.document.total_pages,
            json_path=json_path,
            txt_path=txt_path,
            duration=duration,
        )
        self._ocr_meta = meta

        logger.info(
            "OCR завершён: %d страниц за %.2f с",
            meta.total_pages,
            duration,
        )
        return meta

    # -------------------------------------------------------------------------
    # Шаг 2.5: Маскирование персональных данных
    # -------------------------------------------------------------------------

    async def run_pii_masking(self) -> PIIMaskingMetadata:
        """Маскирует персональные данные в тексте OCR.

        Проходит по всем страницам и заменяет телефоны, email, ИНН и т.д.
        на плейсхолдеры вида [ТЕЛЕФОН], [EMAIL].
        """
        logger.info("=" * 60)
        logger.info("ШАГ 2.5: МАСКИРОВАНИЕ ПЕРСОНАЛЬНЫХ ДАННЫХ")
        logger.info("=" * 60)

        if not self._ocr_result:
            raise PipelineError("OCR не выполнен. Сначала выполните run_ocr().")

        start = time.perf_counter()

        # Собираем страницы для маскирования
        pages = [
            (page.page_number, page.full_text)
            for page in self._ocr_result.document.pages
        ]

        # Маскируем PII
        masked_pages, masking_result = mask_pii_in_document(pages)

        # Обновляем текст страниц в OCR результате
        masked_text_by_page = dict(masked_pages)
        for page in self._ocr_result.document.pages:
            if page.page_number in masked_text_by_page:
                # Pydantic модель — используем __dict__ для изменения
                page.__dict__["full_text"] = masked_text_by_page[page.page_number]

        duration = time.perf_counter() - start

        self._pii_masking_result = masking_result

        # Собираем детализацию по страницам: {page_num: {pii_type: count}}
        pii_by_page: dict[int, dict[str, int]] = {}
        for page_result in masking_result.page_results:
            if page_result.has_pii:
                type_counts: dict[str, int] = {}
                for match in page_result.matches:
                    type_counts[match.pii_type] = type_counts.get(match.pii_type, 0) + 1
                pii_by_page[page_result.page_number] = type_counts

        meta = PIIMaskingMetadata(
            pages_with_pii=masking_result.pages_with_pii,
            total_pii_count=masking_result.total_pii_count,
            pii_by_type=masking_result.pii_by_type,
            pii_by_page=pii_by_page,
            duration=duration,
        )
        self._pii_meta = meta

        if masking_result.has_pii:
            logger.info(
                "PII замаскировано: %d на страницах %s за %.2f с",
                masking_result.total_pii_count,
                masking_result.pages_with_pii,
                duration,
            )
        else:
            logger.info("PII не обнаружено (%.2f с)", duration)

        return meta

    # -------------------------------------------------------------------------
    # Шаг 3: Фильтрация релевантных страниц
    # -------------------------------------------------------------------------

    async def run_page_filter(self) -> FilterMetadata:
        """Фильтрует релевантные страницы через FSM."""
        logger.info("=" * 60)
        logger.info("ШАГ 3: ФИЛЬТРАЦИЯ СТРАНИЦ")
        logger.info("=" * 60)

        if not self._ocr_meta:
            raise PipelineError("OCR не выполнен. Сначала выполните run_ocr().")

        start = time.perf_counter()

        # filter_relevant_pages — синхронная функция
        result = await asyncio.to_thread(
            filter_relevant_pages, self._ocr_meta.txt_path
        )

        duration = time.perf_counter() - start

        self._filter_result = result

        meta = FilterMetadata(
            total_pages=result.total_pages,
            relevant_pages=result.relevant_pages,
            start_page=result.start_page,
            end_page=result.end_page,
            duration=duration,
        )
        self._filter_meta = meta

        logger.info(
            "Фильтрация: %d/%d релевантных страниц [%s-%s] за %.2f с",
            len(result.relevant_pages),
            result.total_pages,
            result.start_page,
            result.end_page,
            duration,
        )
        return meta

    # -------------------------------------------------------------------------
    # Шаг 4: VLM очистка
    # -------------------------------------------------------------------------

    async def run_vlm_cleaning(self) -> VLMMetadata:
        """Очищает релевантные страницы через VLM."""
        logger.info("=" * 60)
        logger.info("ШАГ 4: VLM ОЧИСТКА")
        logger.info("=" * 60)

        if not self._filter_result or not self._filter_result.relevant_pages:
            raise PipelineError("Нет релевантных страниц для VLM. Выполните run_page_filter().")

        if not self._pdf_path:
            raise PipelineError("PDF файл отсутствует.")

        if not self._ocr_result:
            raise PipelineError("OCR результат отсутствует.")

        start = time.perf_counter()

        # Собираем OCR текст для fallback
        relevant_set = set(self._filter_result.relevant_pages)
        raw_text_by_page = {
            page.page_number: page.full_text
            for page in self._ocr_result.document.pages
            if page.page_number in relevant_set
        }

        result = await clean_relevant_pages(
            pdf_path=self._pdf_path,
            page_numbers=self._filter_result.relevant_pages,
            raw_text_by_page=raw_text_by_page,
        )

        json_path, txt_path = await save_vlm_result(result, result_dir=self.pipeline_dir)

        duration = time.perf_counter() - start

        self._vlm_result = result

        meta = VLMMetadata(
            processed_pages=result.processed_pages,
            json_path=json_path,
            txt_path=txt_path,
            duration=duration,
        )
        self._vlm_meta = meta

        logger.info(
            "VLM обработал %d страниц за %.2f с",
            result.processed_pages,
            duration,
        )
        return meta

    # -------------------------------------------------------------------------
    # Шаг 5: Извлечение дефектов
    # -------------------------------------------------------------------------

    async def run_defect_extraction(self) -> ExtractionMetadata:
        """Извлекает дефекты из VLM-очищенных страниц."""
        logger.info("=" * 60)
        logger.info("ШАГ 5: ИЗВЛЕЧЕНИЕ ДЕФЕКТОВ")
        logger.info("=" * 60)

        if not self._vlm_result:
            raise PipelineError("VLM результат отсутствует. Выполните run_vlm_cleaning().")

        start = time.perf_counter()

        result = await extract_defects(self._vlm_result)
        json_path = await save_extraction_result(result, result_dir=self.pipeline_dir)

        duration = time.perf_counter() - start

        self._extraction_result = result

        meta = ExtractionMetadata(
            total_defects=result.total_defects,
            pages_processed=result.pages_processed,
            json_path=json_path,
            duration=duration,
        )
        self._extraction_meta = meta

        logger.info(
            "Извлечено %d дефектов с %d страниц за %.2f с",
            result.total_defects,
            result.pages_processed,
            duration,
        )
        return meta

    # -------------------------------------------------------------------------
    # Шаг 6: Дедупликация
    # -------------------------------------------------------------------------

    async def run_deduplication(self) -> DeduplicationMetadata:
        """Помечает дубликаты дефектов."""
        logger.info("=" * 60)
        logger.info("ШАГ 6: ДЕДУПЛИКАЦИЯ")
        logger.info("=" * 60)

        if not self._extraction_result:
            raise PipelineError("Extraction результат отсутствует. Выполните run_defect_extraction().")

        start = time.perf_counter()

        # deduplicate_defects — синхронная
        result = deduplicate_defects(self._extraction_result)
        json_path = await save_dedup_result(result, result_dir=self.pipeline_dir)

        duration = time.perf_counter() - start

        self._dedup_result = result

        meta = DeduplicationMetadata(
            total_defects=result.total_defects,
            unique_defects=result.unique_defects,
            duplicate_groups=result.duplicate_groups,
            json_path=json_path,
            duration=duration,
        )
        self._dedup_meta = meta

        logger.info(
            "Дедупликация: %d всего, %d уникальных, %d групп дубликатов за %.2f с",
            result.total_defects,
            result.unique_defects,
            result.duplicate_groups,
            duration,
        )
        return meta

    # -------------------------------------------------------------------------
    # Шаг 7: Генерация Excel
    # -------------------------------------------------------------------------

    async def run_excel_generation(self) -> ExcelMetadata:
        """Генерирует Excel отчёт."""
        logger.info("=" * 60)
        logger.info("ШАГ 7: ГЕНЕРАЦИЯ EXCEL")
        logger.info("=" * 60)

        if not self._dedup_result:
            raise PipelineError("Dedup результат отсутствует. Выполните run_deduplication().")

        start = time.perf_counter()

        # generate_excel_report — синхронная
        excel_path = await asyncio.to_thread(
            generate_excel_report,
            self._dedup_result,
            output_dir=self.pipeline_dir,
        )

        duration = time.perf_counter() - start

        meta = ExcelMetadata(
            excel_path=Path(excel_path),
            duration=duration,
        )
        self._excel_meta = meta

        logger.info("Excel отчёт создан: %s за %.2f с", excel_path, duration)
        return meta

    # -------------------------------------------------------------------------
    # Запуск полного пайплайна
    # -------------------------------------------------------------------------

    async def run(self) -> PipelineResult:
        """Запускает полный пайплайн анализа дефектов.

        Returns:
            PipelineResult с метаданными всех шагов
        """
        logger.info("=" * 60)
        logger.info("ЗАПУСК ПАЙПЛАЙНА АНАЛИЗА ДЕФЕКТОВ")
        logger.info("=" * 60)
        logger.info("URL: %s", self.source_url)
        logger.info("Артефакты: %s", self.pipeline_dir)

        try:
            await self.download_document()
            await self.run_ocr()
            await self.run_pii_masking()
            await self.run_page_filter()

            # Проверяем что есть релевантные страницы
            if not self._filter_result or not self._filter_result.relevant_pages:
                self._errors.append("Не найдены релевантные страницы с дефектами")
                logger.warning("Не найдены релевантные страницы — пайплайн остановлен")
            else:
                await self.run_vlm_cleaning()
                await self.run_defect_extraction()
                await self.run_deduplication()
                await self.run_excel_generation()

        except PipelineError as e:
            self._errors.append(str(e))
            logger.error("Ошибка пайплайна: %s", e)
        except Exception as e:
            self._errors.append(f"Неожиданная ошибка: {e}")
            logger.exception("Неожиданная ошибка в пайплайне")

        total_duration = time.perf_counter() - self.started_at

        result = PipelineResult(
            pipeline_dir=self.pipeline_dir,
            source_url=self.source_url,
            pdf_path=self._pdf_path,
            excel_path=self._excel_meta.excel_path if self._excel_meta else None,
            total_duration=total_duration,
            download=self._download_meta,
            ocr=self._ocr_meta,
            pii_masking=self._pii_meta,
            filter=self._filter_meta,
            vlm=self._vlm_meta,
            extraction=self._extraction_meta,
            deduplication=self._dedup_meta,
            excel=self._excel_meta,
            errors=self._errors,
        )

        logger.info("=" * 60)
        logger.info("ПАЙПЛАЙН ЗАВЕРШЁН")
        logger.info("=" * 60)
        logger.info("Общее время: %.2f с", total_duration)
        if result.excel_path:
            logger.info("Excel: %s", result.excel_path)
        if result.errors:
            logger.warning("Ошибки: %s", result.errors)

        return result


# =============================================================================
# Публичный API
# =============================================================================


async def run_pipeline(source_url: str, pipeline_dir: Path | None = None) -> PipelineResult:
    """Запускает полный пайплайн анализа дефектов.

    Args:
        source_url: Ссылка на PDF в Google Drive
        pipeline_dir: Папка для артефактов (опционально)

    Returns:
        PipelineResult с результатами и метаданными
    """
    pipeline = DefectAnalysisPipeline(source_url, pipeline_dir)
    return await pipeline.run()
