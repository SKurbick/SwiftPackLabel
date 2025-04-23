from typing import List, Dict, Any, Optional
from pydantic import BaseModel, ConfigDict, Field


class BaseSchema(BaseModel):
    """Базовая модель с общими конфигурациями для всех схем."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        str_strip_whitespace=True,
        str_min_length=0,
    )


class OrderDetail(BaseSchema):
    """Модель заказа с детальной информацией."""
    id: int
    article: str
    photo: str = "Нет фото"
    subject_name: str = "Нет наименования"
    price: int
    account: str
    created_at: str
    elapsed_time: str = "Н/Д"


class GroupedOrderInfo(BaseSchema):
    """Модель сгруппированной информации о заказах по артикулу wild."""
    wild: str
    stock_quantity: int = 0
    doc_name: str = "Нет наименования в документе"
    api_name: str = "Нет наименования из API"
    orders: List[Dict[str, Any]] = []
    order_count: int = 0
