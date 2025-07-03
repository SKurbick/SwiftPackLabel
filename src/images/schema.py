from pydantic import BaseModel, Field
from typing import Optional


class ImageUploadResponse(BaseModel):
    """Схема ответа при загрузке изображения"""
    success: bool = Field(..., description="Успешность загрузки")
    message: str = Field(..., description="Сообщение о результате")
    filename: Optional[str] = Field(None, description="Имя сохраненного файла")
    file_path: Optional[str] = Field(None, description="Путь к сохраненному файлу")


class ImageInfoResponse(BaseModel):
    """Схема ответа с информацией об изображении"""
    filename: str = Field(..., description="Имя файла")
    path: str = Field(..., description="Путь к файлу")
    size_bytes: int = Field(..., description="Размер файла в байтах")
    width: int = Field(..., description="Ширина изображения")
    height: int = Field(..., description="Высота изображения")
    format: str = Field(..., description="Формат изображения")
    created_at: float = Field(..., description="Время создания файла")
    modified_at: float = Field(..., description="Время последнего изменения")


class ImageDeleteResponse(BaseModel):
    """Схема ответа при удалении изображения"""
    success: bool = Field(..., description="Успешность удаления")
    message: str = Field(..., description="Сообщение о результате")
    filename: str = Field(..., description="Имя удаленного файла")


class ImageListResponse(BaseModel):
    """Схема ответа со списком изображений"""
    total: int = Field(..., description="Общее количество изображений")
    filenames: list[str] = Field(..., description="Список названий файлов изображений")