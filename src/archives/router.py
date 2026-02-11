from typing import Annotated, List

from fastapi import APIRouter, HTTPException, Depends, status, Query, Response
from fastapi.responses import StreamingResponse

from src.auth import UserPermissions, get_info_from_token
from src.db import get_db_connection, AsyncGenerator
from src.archives.archives import Archives
from src.archives.schema import ArchiveResponse
from src.archives.utils import create_archive_response

archive = APIRouter(prefix='/archive', tags=['Archives'])


@archive.get(
    "/",
    response_model=ArchiveResponse,
    summary="Get all archives",
    description="Retrieve a list of all available archives"
)
async def get_archives(db: AsyncGenerator = Depends(get_db_connection),
                       user: UserPermissions = Depends(get_info_from_token)) -> ArchiveResponse:
    """Get all archives information."""
    if not user.viewing:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail="permission locked")
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
        user: UserPermissions = Depends(get_info_from_token)) -> StreamingResponse:
    """Download one or multiple archives based on provided IDs."""
    if not user.download_excel_files:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail="permission locked")
    archives_service = Archives(db)
    archives_list = await archives_service.get_archives_by_ids(ids)

    if not archives_list:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Архивы не найдены"
        )

    return create_archive_response(archives_list)


@archive.delete(
    "/{archive_id}",
    summary="Delete archive",
    description="Delete an archive by ID (superuser only)",
    status_code=status.HTTP_204_NO_CONTENT
)
async def delete_archive(
        archive_id: int,
        db: AsyncGenerator = Depends(get_db_connection),
        user: UserPermissions = Depends(get_info_from_token)) -> Response:
    """Delete an archive by ID. Only superusers can perform this action."""
    if not user.ability_to_upload_excel_file_to_fines:
        raise HTTPException(status_code=status.HTTP_423_LOCKED, detail="permission locked")
    archives_service = Archives(db)
    deleted = await archives_service.delete_archive(archive_id)

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Архив с ID {archive_id} не найден"
        )

    return Response(status_code=status.HTTP_204_NO_CONTENT)
