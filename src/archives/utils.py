import zipfile
from io import BytesIO
from typing import List
from fastapi.responses import StreamingResponse

from src.archives.schema import ArchiveDetail
from src.concurrency import run_blocking


def _pack_archives(archives_list: List[ArchiveDetail]) -> BytesIO:
    """Синхронная упаковка архивов в один ZIP"""
    zip_io = BytesIO()
    with zipfile.ZipFile(zip_io, mode="w") as zf:
        for archive in archives_list:
            file_name = archive.name or f"archive_{archive.id}.zip"
            zf.writestr(file_name, archive.archive)

    zip_io.seek(0)
    return zip_io


async def create_archive_response(archives_list: List[ArchiveDetail]) -> StreamingResponse:
    """Create a streaming response with archive data.
    Args:
        archives_list: List of archives to include in the response
    Returns:
        StreamingResponse with appropriate headers
    """
    if len(archives_list) == 1:
        archive = archives_list[0]
        file_name = archive.name or f"archive_{archive.id}.zip"

        return StreamingResponse(
            BytesIO(archive.archive),
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={file_name}"})

    zip_io = await run_blocking(_pack_archives, archives_list)
    return StreamingResponse(
        zip_io,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=archives_package.zip"}
    )