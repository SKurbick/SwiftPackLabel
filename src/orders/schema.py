from typing import List
from pydantic import BaseModel, ConfigDict


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


class OrdersResponse(BaseSchema):
    """Модель ответа со списком заказов."""
    orders: List[OrderDetail]
