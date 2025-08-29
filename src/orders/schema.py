from typing import List, Dict
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from datetime import datetime, timedelta


class BaseSchema(BaseModel):
    """Базовая модель с общими конфигурациями для всех схем."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        str_strip_whitespace=True,
        str_min_length=0,
    )


class BaseResponseSchema(BaseSchema):
    """Базовая схема для ответов с автоматическим timestamp."""
    cached_at: datetime = Field(
        default_factory=lambda: datetime.utcnow() + timedelta(hours=3),
        description="Время получения данных"
    )


class OrderDetail(BaseSchema):
    """Модель заказа с детальной информацией."""
    id: int
    nm_id:int
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
    orders: List[OrderDetail] = []
    order_count: int = 0
    
    @field_validator('order_count')
    def validate_order_count(cls, v):
        if v < 0:
            raise ValueError("order_count не может быть отрицательным")
        return v
    
    @field_validator('orders')
    def validate_orders_list(cls, v):
        if not v:
            raise ValueError("Список заказов не может быть пустым")
        return v


class GroupedOrderInfoWithFact(GroupedOrderInfo):
    fact_orders: int = 0
    
    @field_validator('fact_orders')
    def validate_fact_orders(cls, v, info):
        values = info.data
        if 'order_count' in values and v > values['order_count']:
            raise ValueError(f"fact_orders ({v}) не может быть больше order_count ({values['order_count']})")
        if v < 0:
            raise ValueError("fact_orders не может быть отрицательным")
        return v


class OrdersWithSupplyNameIn(BaseSchema):
    orders: Dict[str, GroupedOrderInfoWithFact]
    name_supply: str
    is_hanging: bool = False
    
    @field_validator('name_supply')
    def validate_name_supply(cls, v):
        if not v:
            raise ValueError("name_supply не может быть пустым")
        return v
    
    @field_validator('orders')
    def validate_orders(cls, orders_dict):
        if not orders_dict:
            raise ValueError("orders не может быть пустым")
        
        for wild_key, info in orders_dict.items():
            if not info.orders:
                raise ValueError(f"Список заказов для {wild_key} не может быть пустым")
        
        return orders_dict
    
    @model_validator(mode='after')
    def add_tex_suffix_for_non_hanging(self):
        """Добавляет суффикс _TEX к названию поставки, если она не висячая."""
        if not self.is_hanging:
            self.name_supply = f"{self.name_supply}_TEX"
        return self


class WildInfo(BaseSchema):
    wild: str
    count: int

class SupplyInfo(BaseSchema):
    supply_id: str
    account: str
    order_ids: List[int]

class SupplyAccountWildOut(BaseSchema):
    wilds: List[WildInfo]
    supply_ids: List[SupplyInfo]
    order_wild_map: Dict[int, str] = {}


class OrdersResponse(BaseSchema):
    """Схема для ответа с заказами."""
    orders: Dict[str, GroupedOrderInfo]
