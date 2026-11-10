from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints


Rid = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=256, strict=True)]


class AssemblyTasksLookupRequest(BaseModel):
    """список rid для поиска сборочных заданий."""

    rids: list[Rid] = Field(
        min_length=1,
        max_length=1000
    )


class AssemblyTaskItem(BaseModel):
    rid: str
    task_id: int


class AssemblyTasksLookupResponse(BaseModel):
    items: list[AssemblyTaskItem]
