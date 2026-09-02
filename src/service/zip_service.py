from io import BytesIO
import zipfile
from typing import Dict, AsyncIterator

from src.concurrency import run_blocking


async def collect_selection_sheet_content(pdf_generator: AsyncIterator[bytes]) -> bytes:
    """
    Собирает содержимое PDF из генератора в единый байтовый объект.
    Args:
        pdf_generator: Асинхронный генератор, возвращающий фрагменты PDF
    Returns:
        bytes: Полное содержимое PDF в виде байтов
    """
    content = b""
    async for chunk in pdf_generator:
        content += chunk
    return content


def _build_zip_archive(files_dict: Dict[str, bytes]) -> BytesIO:
    """Синхронная сборка ZIP. Вызывать только из пула потоков."""
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        for filename, content in files_dict.items():
            zip_file.writestr(filename, content)

    zip_buffer.seek(0)
    return zip_buffer


async def create_zip_archive(files_dict: Dict[str, bytes]) -> BytesIO:
    """
    Создает ZIP-архив в памяти из словаря файлов.

    Args:
        files_dict: Словарь вида {имя_файла: содержимое}

    Returns:
        BytesIO: Буфер с ZIP-архивом, готовый к передаче в ответе
    """
    return await run_blocking(_build_zip_archive, files_dict)
