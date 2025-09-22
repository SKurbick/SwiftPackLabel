from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, field_validator



class BaseSchema(BaseModel):
    """Базовая модель с общими конфигурациями для всех схем."""
    
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        str_strip_whitespace=True,
        str_min_length=0,
    )


class WildParserRequest(BaseSchema):
    """Запрос на парсинг строки wild."""
    
    wild_string: str = Field(..., description="Строка формата 'wild123/23'")
    
    @field_validator("wild_string")
    def validate_wild_string(cls, v):
        """Валидация формата строки wild."""
        if not v or '/' not in v:
            raise ValueError("Строка должна быть в формате 'wild123/23'")
        return v


class WildParserResponse(BaseSchema):
    """Ответ с информацией, извлеченной из строки wild."""
    
    wild: str = Field(..., description="Уникальный номер wild")
    quantity: int = Field(..., description="Количество товара")
    name_file: Optional[str] = Field(..., description="Наименование товара в файле")
    name_db: Optional[str] = Field(..., description="Наименование товара в базе данных")
    photos: Optional[str] = Field(..., description="Ссылки на фото товара")
    length: Optional[int] = Field(None, description="Длина товара в мм")
    width: Optional[int] = Field(None, description="Ширина товара в мм")
    height: Optional[int] = Field(None, description="Высота товара в мм")
    volume: Optional[float] = Field(None, description="Объем товара в м³")
    rating: Optional[float] = Field(None, description="Рейтинг товара")
    colors: Optional[List[str]] = Field(None, description="Цвет товара")


class QRLookupRequest(BaseSchema):
    """Запрос на поиск по QR-коду."""
    
    qr_data: str = Field(..., description="QR код стикера, например '*CN+tGIpw'")



class QRLookupResponse(BaseSchema):
    """Ответ с найденными данными по QR-коду."""
    
    found: bool = Field(..., description="Найдены ли данные")
    data: Optional[dict] = Field(None, description="Объединенные данные из qr_scans и orders_wb")
