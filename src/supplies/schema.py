from typing import List
from src.utils import format_date

from pydantic import BaseModel, field_validator, ConfigDict
from src.orders.schema import SupplyInfo


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
        import re
        if not isinstance(v, str):
            return str(v)
        
        # Check if date is already in DD.MM.YYYY format (from cache)
        if re.match(r'^\d{2}\.\d{2}\.\d{4}$', v):
            return v
            
        # If it's ISO format, convert it
        try:
            return format_date(v)
        except ValueError:
            # If format is unexpected, return as-is
            return v


class SupplyIdBodySchema(BaseSchema):
    """Схема для тела запроса с списком поставок."""

    supplies: List[SupplyId]


class SupplyIdResponseSchema(BaseSchema):
    """Схема для ответа с списком обработанных поставок."""

    supplies: List[SupplyIdResult]


class SupplyDeleteItem(BaseModel):
    account: str
    supply_id: str


class SupplyDeleteBody(BaseModel):
    supply: list[SupplyDeleteItem]


class SupplyDeleteResponse(BaseModel):
    deleted: list[SupplyDeleteItem]


class WildOrderItem(BaseModel):
    """Схема для представления заказа при фильтрации по wild."""
    order_id: int


class WildSupplyItem(BaseModel):
    """Схема для представления поставки при фильтрации по wild."""
    account: str
    supply_id: str
    orders: List[WildOrderItem]


class WildFilterRequest(BaseModel):
    """Схема запроса для получения стикеров по определенному wild."""
    wild: str
    supplies: List[WildSupplyItem]


class DeliverySupplyInfo(SupplyInfo):
    """
    Расширенная схема SupplyInfo для использования в API доставки.
    Добавляет валидацию, что список order_ids не должен быть пустым.
    """
    
    @field_validator("order_ids")
    def validate_order_ids(cls, order_ids: List[int]) -> List[int]:
        """
        Проверяет, что order_ids не пустой список.
        """
        if not order_ids:
            raise ValueError("Список order_ids не может быть пустым")
        return order_ids