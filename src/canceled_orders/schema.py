from typing import List
from pydantic import BaseModel, Field, ConfigDict, field_validator


class SupplyCanceledCheckRequest(BaseModel):
    """Схема для запроса проверки наличия отмененных заказов в поставке."""

    model_config = ConfigDict(str_strip_whitespace=True)

    supply_id: str = Field(..., description="ID поставки")

    @field_validator('supply_id')
    def validate_supply_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError('supply_id не может быть пустым')
        return v.strip()


class SupplyCanceledCheckResponse(BaseModel):
    """Схема для ответа проверки наличия отмененных заказов в поставке."""

    supply_id: str = Field(..., description="ID поставки")
    has_canceled: bool = Field(..., description="Флаг наличия заказов со статусом canceled_by_client")
    canceled_order_ids: List[int] = Field(default_factory=list, description="Список ID заказов со статусом canceled_by_client")


class BulkSupplyCanceledCheckRequest(BaseModel):
    """Схема для массового запроса проверки наличия отмененных заказов в поставках."""

    model_config = ConfigDict(str_strip_whitespace=True)

    supply_ids: List[str] = Field(..., description="Список ID поставок для проверки")

    @field_validator('supply_ids')
    def validate_supply_ids(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError('Список supply_ids не может быть пустым')
        cleaned = [sid.strip() for sid in v if sid and sid.strip()]
        if not cleaned:
            raise ValueError('Все supply_ids пустые')
        return cleaned


class BulkSupplyCanceledCheckResponse(BaseModel):
    """Схема для массового ответа проверки наличия отмененных заказов в поставках."""

    results: List[SupplyCanceledCheckResponse] = Field(..., description="Список результатов проверки для каждой поставки")
