from typing import List
from src.utils import format_date

from pydantic import BaseModel, field_validator, ConfigDict


class BaseSchema(BaseModel):
    """Базовая модель с общими конфигурациями для всех схем."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        str_strip_whitespace=True,
        str_min_length=0,)


class OrderSchema(BaseModel):
    """Схема для представления основной информации о заказе."""

    local_vendor_code: str
    order_id: int
    nm_id: int


class StickerSchema(OrderSchema):
    """Схема для представления информации о стикере, расширяющая OrderSchema."""

    file: str
    partA: int
    partB: int
    barcode: str


class SupplyBase(BaseSchema):
    """Базовый класс для поставок с общими полями."""

    name: str
    createdAt: str
    supply_id: str
    account: str
    count: int
    orders: List[OrderSchema]


class SupplyId(SupplyBase):
    """Схема для входящих данных о поставке."""
    pass


class SupplyIdResult(SupplyBase):
    """Схема для обработанных данных о поставке с датой в нужном формате."""

    @field_validator("createdAt", mode="before")
    def convert_date(cls, v: str) -> str:
        """Преобразует строку даты в требуемый формат."""
        return format_date(v)


class SupplyIdBodySchema(BaseSchema):
    """Схема для тела запроса с списком поставок."""

    supplies: List[SupplyId]


class SupplyIdResponseSchema(BaseSchema):
    """Схема для ответа с списком обработанных поставок."""

    supplies: List[SupplyIdResult]