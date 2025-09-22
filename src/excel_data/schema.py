from typing import List, Optional
from pydantic import BaseModel, Field


class WildModelPair(BaseModel):
    """Схема для представления пары значений wild-модель."""
    wild: str
    model: str = Field(alias="модель")
    
    class Config:
        allow_population_by_field_name = True


class WildModelRecord(BaseModel):
    """Схема для записи с индексом."""
    index: int
    wild: str
    model: str


class WildModelCreate(BaseModel):
    """Схема для создания новой записи."""
    wild: str
    model: str


class WildModelUpdate(BaseModel):
    """Схема для обновления записи."""
    wild: str
    model: str


class WildModelListResponse(BaseModel):
    """Схема для ответа со списком всех записей."""
    data: List[WildModelRecord] = Field(default_factory=list)
    total: int = 0


class WildModelResponse(BaseModel):
    """Схема для ответа с моделью по wild коду."""
    wild: str
    model: str


class MessageResponse(BaseModel):
    """Схема для простого ответа с сообщением."""
    message: str
