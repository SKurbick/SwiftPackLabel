from typing import Annotated, List

from fastapi import APIRouter, HTTPException, Depends, status, Query
from fastapi.responses import StreamingResponse

from src.auth.dependencies import get_current_user
from src.db import get_db_connection, AsyncGenerator
from src.archives.archives import Archives
from src.archives.schema import ArchiveResponse, ArchiveIdsRequest
from src.archives.utils import create_archive_response

archive = APIRouter(prefix='/archive', tags=['Archives'])


@archive.get(
    "/",
    response_model=ArchiveResponse,
    summary="Get all archives",
    description="Retrieve a list of all available archives"
)
async def get_archives(db: AsyncGenerator = Depends(get_db_connection),
                       user: dict = Depends(get_current_user)) -> ArchiveResponse:
    """Get all archives information."""
    archives_service = Archives(db)
    result = await archives_service.get_all_archives()
    return ArchiveResponse(archives=result)


@archive.get(
    "/download",
    summary="Download archives",
    description="Download one or multiple archives as a zip file",
    response_description="ZIP file containing the requested archives"
)
async def download_archives(
        ids: Annotated[List[int], Query(description="List of archive IDs to download")],
        db: AsyncGenerator = Depends(get_db_connection),
        user: dict = Depends(get_current_user)) -> StreamingResponse:
    """Download one or multiple archives based on provided IDs."""
    archives_service = Archives(db)
    archives_list = await archives_service.get_archives_by_ids(ids)

    if not archives_list:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Архивы не найдены"
        )

    return create_archive_response(archives_list)
