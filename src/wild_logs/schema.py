from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class WildLogCreate(BaseModel):
    """Схема для входных данных о работе с wild."""
    operator_name: str
    wild_code: str
    order_count: int
    processing_time: float
    product_name: str
    additional_data: Optional[Dict[str, Any]] = None
    session_id: Optional[str] = None


class ShiftSupervisorData(BaseModel):
    """Данные для авторизации и логирования действий старшего смены"""
    session_id: str = Field(..., description="Идентификатор сессии пользователя")
    supervisor_password: str = Field(..., description="Пароль старшего смены, подтверждающего закрытие поставки")