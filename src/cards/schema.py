from typing import Optional
from pydantic import BaseModel, Field, ConfigDict, field_validator


class DimensionsUpdateRequest(BaseModel):
    """Схема для запроса обновления размеров и веса товара."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        str_strip_whitespace=True
    )

    wild: str = Field(..., description="Артикул продавца (wild)")
    width: Optional[float] = Field(None, description="Ширина товара (см)")
    length: Optional[float] = Field(None, description="Длина товара (см)")
    height: Optional[float] = Field(None, description="Высота товара (см)")
    weight: Optional[float] = Field(None, description="Вес товара (кг)")

    @field_validator('wild')
    def validate_wild(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError('Артикул продавца не может быть пустым')
        return v.strip()
