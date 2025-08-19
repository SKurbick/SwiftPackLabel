from typing import List, Optional, Dict, Any
from src.utils import format_date
from datetime import datetime, timedelta

from pydantic import BaseModel, field_validator, ConfigDict, Field
from src.orders.schema import SupplyInfo


class BaseSchema(BaseModel):
    """Базовая модель с общими конфигурациями для всех схем."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        str_strip_whitespace=True,
        str_min_length=0, )


class BaseResponseSchema(BaseSchema):
    """Базовая схема для ответов с автоматическим timestamp."""
    cached_at: datetime = Field(
        default_factory=lambda: datetime.utcnow() + timedelta(hours=3),
        description="Время получения данных"
    )


class OrderSchema(BaseModel):
    """Схема для представления основной информации о заказе."""

    local_vendor_code: str
    order_id: int
    nm_id: int
    createdAt: Optional[str] = None


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

    shipped_count: Optional[int] = Field(None,
                                         description="Количество отгруженных товаров (только для висячих поставок)")
    is_fictitious_delivered: Optional[bool] = Field(None,
                                                    description="Флаг фиктивной доставки (только для висячих поставок)")

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
    orders: Dict[str, WildOrdersItem] = Field(
        description="Заказы сгруппированные по wild-кодам с индивидуальным remove_count")


class MoveOrdersResponse(BaseSchema):
    """Схема ответа для перемещения заказов."""
    success: bool
    message: str
    removed_order_ids: List[int] = Field(description="ID заказов которые были удалены/перемещены")
    processed_supplies: int = Field(description="Количество обработанных поставок")
    processed_wilds: int = Field(description="Количество обработанных wild-кодов")


class SupplyBarcodeListRequest(BaseSchema):
    """Схема запроса для получения штрихкодов списка поставок."""
    supplies: Dict[str, str] = Field(description="Словарь поставок: {supply_id: account_name}")


class FictitiousDeliveryRequest(BaseSchema):
    """Схема запроса для перевода фиктивной поставки в доставку."""
    supplies: Dict[str, str] = Field(description="Объект поставок {supply_id: account} (может содержать одну или несколько поставок)")
    
    @field_validator('supplies')
    @classmethod
    def validate_supplies(cls, v):
        """Валидация объекта поставок."""
        if not v or len(v) == 0:
            raise ValueError("Объект supplies не может быть пустым")
        
        if not isinstance(v, dict):
            raise ValueError("Поле supplies должно быть объектом")
            
        for supply_id_key, account_value in v.items():
            if not isinstance(supply_id_key, str) or not isinstance(account_value, str):
                raise ValueError("Все ключи и значения в supplies должны быть строками")
            if not supply_id_key.strip() or not account_value.strip():
                raise ValueError("supply_id и account не могут быть пустыми строками")
        
        return v


class FictitiousDeliveryResponse(BaseSchema):
    """Схема ответа для перевода фиктивных поставок в доставку."""
    success: bool = Field(description="Успешность операции")
    message: str = Field(description="Сообщение о результате операции")
    operator: str = Field(description="Оператор, выполнивший операцию")
    total_processed: int = Field(description="Общее количество обработанных поставок")
    successful_count: int = Field(description="Количество успешно обработанных")
    failed_count: int = Field(description="Количество неудачных")
    results: List[Dict[str, Any]] = Field(description="Детальные результаты по каждой поставке")
    processing_time_seconds: float = Field(description="Время обработки в секундах")


class FictitiousDeliveryInfo(BaseSchema):
    """Схема информации о фиктивной доставке."""
    supply_id: str = Field(description="ID поставки")
    account: str = Field(description="Аккаунт Wildberries")
    operator: Optional[str] = Field(None, description="Оператор, создавший поставку")
    created_at: datetime = Field(description="Время создания поставки")
    fictitious_delivered_at: Optional[datetime] = Field(None, description="Время перевода в фиктивную доставку")
    fictitious_delivery_operator: Optional[str] = Field(None, description="Оператор фиктивной доставки")
    orders_count: int = Field(description="Количество заказов в поставке")
    shipped_count: int = Field(description="Количество отгруженных заказов")
