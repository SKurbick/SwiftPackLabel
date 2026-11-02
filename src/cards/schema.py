from typing import Annotated, Optional
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator
import math


class WildQrPrintRequest(BaseModel):
    """Данные для печати этикеток с QR-кодом вилда."""

    model_config = ConfigDict(str_strip_whitespace=True)

    product_article: Annotated[
        str,
        Field(min_length=1, max_length=16, description="wild"),
    ]
    count: Annotated[
        int,
        Field(ge=1, le=1000, description="Кол-во этикеток"),
    ]


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

    @model_validator(mode='after')
    def round_dimensions_and_weight(self) -> 'DimensionsUpdateRequest':
        """Округляет размеры и вес до верхнего целого числа после валидации."""
        for field_name in ['width', 'length', 'height']:
            value = getattr(self, field_name)
            if value is not None:
                setattr(self, field_name, int(math.ceil(value)))
        return self