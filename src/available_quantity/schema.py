from pydantic import BaseModel


class AvailableQuantity(BaseModel):
    product_id: str
    warehouse_id: int
    available_quantity: float