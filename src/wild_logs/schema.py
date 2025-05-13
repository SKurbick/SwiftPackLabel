from typing import Optional, Dict, Any
from pydantic import BaseModel


class WildLogCreate(BaseModel):
    """Схема для входных данных о работе с wild."""
    operator_name: str
    wild_code: str
    order_count: int
    processing_time: float
    product_name: str
    additional_data: Optional[Dict[str, Any]] = None
