"""
Схемы данных для работы с карточками товаров Wildberries.
"""
from pydantic import BaseModel, Field, ConfigDict


class DimensionsUpdateRequest(BaseModel):
    """Схема для запроса обновления размеров и веса товара."""
    
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        str_strip_whitespace=True
    )
    
    wild: str = Field(..., description="Артикул продавца (wild)")
    width: float = Field(..., description="Ширина товара (см)")
    length: float = Field(..., description="Длина товара (см)")
    height: float = Field(..., description="Высота товара (см)")
    weight: float = Field(..., description="Вес товара (кг)")
