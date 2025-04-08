from typing import List
from pydantic import BaseModel, Field


class WildModelPair(BaseModel):
    """Схема для представления пары значений wild-модель."""
    wild: str
    модель: str


class WildModelResponse(BaseModel):
    """Схема для ответа с моделью по wild коду."""
    wild: str
    модель: str


class WildModelListResponse(BaseModel):
    """Схема для ответа со списком всех пар wild-модель."""
    data: List[WildModelPair] = Field(default_factory=list)


class MessageResponse(BaseModel):
    """Схема для простого ответа с сообщением."""
    message: str
