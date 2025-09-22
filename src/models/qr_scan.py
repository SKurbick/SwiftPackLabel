from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class QRScanCreate(BaseModel):
    """Модель для создания новой записи QR-скана"""
    
    order_id: int = Field(..., description="ID заказа (номер сборочного задания)")
    qr_data: str = Field(..., description="Данные из QR-кода в текстовом формате")
    account: str = Field(..., description="Аккаунт Wildberries")
    part_a: Optional[str] = Field(None, description="Часть A из стикера WB (обычно артикул)")
    part_b: Optional[str] = Field(None, description="Часть B из стикера WB (обычно суффикс)")