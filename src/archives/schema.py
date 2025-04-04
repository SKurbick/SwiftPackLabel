from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class ArchiveBase(BaseModel):
    name: Optional[str]
    id: int


class ArchiveInfo(ArchiveBase):
    date: datetime
    user: Optional[str] = None


class ArchiveResponse(BaseModel):
    archives: List[ArchiveInfo]


class ArchiveDetail(ArchiveBase):
    archive: bytes
    created_at: datetime
    user_name: Optional[str] = None


class ArchiveIdsRequest(BaseModel):
    ids: List[int] = Field(..., description="List of archive IDs to download")