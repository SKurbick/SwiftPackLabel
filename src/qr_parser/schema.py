from typing import List
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
    name: str = Field(..., description="Наименование товара")
    photos: List[str] = Field(..., description="Ссылки на фото товара")
