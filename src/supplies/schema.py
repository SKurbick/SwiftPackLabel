from typing import List, Optional, Dict
from src.utils import format_date
from datetime import datetime

from pydantic import BaseModel, field_validator, ConfigDict, Field
from src.orders.schema import SupplyInfo


class BaseSchema(BaseModel):
    """Базовая модель с общими конфигурациями для всех схем."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        str_strip_whitespace=True,
        str_min_length=0,)


class BaseResponseSchema(BaseSchema):
    """Базовая схема для ответов с автоматическим timestamp."""
    cached_at: datetime = Field(default_factory=datetime.utcnow, description="Время получения данных")


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

    shipped_count: Optional[int] = Field(None, description="Количество отгруженных товаров (только для висячих поставок)")

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


class SupplyIdResponseSchema(BaseResponseSchema):
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


class SupplyIdWithShippedBodySchema(BaseSchema):
    """Схема для тела запроса с списком висячих поставок и фактическим количеством для отгрузки."""
    
    supplies: List[SupplyId]
    shipped_count: int = Field(description="Фактическое количество товаров для отгрузки из висячих поставок")


class SupplyOrderItem(BaseModel):
    """Схема для элемента поставки с заказами."""
    account: str
    supply_id: str
    order_ids: List[int]


class WildOrdersItem(BaseModel):
    """Схема для заказов по wild-коду с количеством для удаления."""
    supplies: List[SupplyOrderItem] = Field(description="Список поставок для данного wild-кода")
    remove_count: int = Field(description="Количество заказов для перемещения для данного wild-кода")


class MoveOrdersRequest(BaseSchema):
    """Схема запроса для перемещения заказов между поставками."""
    orders: Dict[str, WildOrdersItem] = Field(description="Заказы сгруппированные по wild-кодам с индивидуальным remove_count")


class MoveOrdersResponse(BaseSchema):
    """Схема ответа для перемещения заказов."""
    success: bool
    message: str
    removed_order_ids: List[int] = Field(description="ID заказов которые были удалены/перемещены")
    processed_supplies: int = Field(description="Количество обработанных поставок")
    processed_wilds: int = Field(description="Количество обработанных wild-кодов")

