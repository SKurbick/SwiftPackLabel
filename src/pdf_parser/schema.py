"""
Pydantic схемы для PDF парсера
"""
from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class OrderItem(BaseModel):
    """Модель отдельного заказа из листа подбора с метаданными"""
    
    order_id: str = Field(..., description="Номер задания (ID заказа)")
    brand: str = Field(..., description="Бренд товара")
    product_name: str = Field(..., description="Наименование товара")
    size: str = Field(..., description="Размер товара")
    color: str = Field(..., description="Цвет товара")
    seller_article: str = Field(..., description="Артикул продавца (wild код)")
    sticker_code: str = Field(..., description="Код стикера (первая часть)")
    sticker_number: str = Field(..., description="Номер стикера (вторая часть)")
    
    # Метаданные PDF парсера (добавляются в каждый заказ)
    source_file: str = Field(..., description="Имя исходного PDF файла")
    parsed_at: str = Field(..., description="Время парсинга (ISO формат)")
    parser_version: str = Field(..., description="Версия парсера")
    supply_id: Optional[str] = Field(None, description="ID поставки из PDF")
    date: Optional[str] = Field(None, description="Дата документа в формате YYYY-MM-DD")
    date_original: Optional[str] = Field(None, description="Оригинальная дата в формате DD.MM.YYYY")
    total_quantity: Optional[int] = Field(None, description="Общее количество товаров")
    
    # Дополнительные поля, добавляемые через API
    parsed_by: Optional[str] = Field(None, description="Пользователь, выполнивший парсинг")
    original_filename: Optional[str] = Field(None, description="Оригинальное имя загруженного файла")
    test_mode: Optional[bool] = Field(None, description="Режим тестирования")
    
    class Config:
        json_schema_extra = {
            "example": {
                "order_id": "3767432495",
                "brand": "Аппликатор Кузнецова",
                "product_name": "Массажный коврик игольчатый подушка для спины шеи ног",
                "size": "0",
                "color": "фиолетовый",
                "seller_article": "wild105d",
                "sticker_code": "4082875",
                "sticker_number": "0025",
                "source_file": "лист_подбора.pdf",
                "parsed_at": "2025-08-29T20:56:59.996074",
                "parser_version": "1.0.0",
                "supply_id": "WB-GI-176731503",
                "date": "2025-08-26",
                "date_original": "26.08.2025",
                "total_quantity": 82,
                "parsed_by": "admin",
                "original_filename": "лист_подбора.pdf"
            }
        }


class ParsingMetadata(BaseModel):
    """Метаданные парсинга"""
    
    source_file: str = Field(..., description="Имя исходного файла")
    parsed_at: str = Field(..., description="Время парсинга (ISO формат)")
    parser_version: str = Field(..., description="Версия парсера")
    parsed_by: Optional[str] = Field(None, description="Пользователь, выполнивший парсинг")
    original_filename: Optional[str] = Field(None, description="Оригинальное имя загруженного файла")
    test_mode: Optional[bool] = Field(False, description="Режим тестирования")
    
    # Данные из заголовка документа
    supply_id: Optional[str] = Field(None, description="ID поставки (например: WB-GI-176731503)")
    date: Optional[str] = Field(None, description="Дата документа в формате YYYY-MM-DD")
    date_original: Optional[str] = Field(None, description="Оригинальная дата в формате DD.MM.YYYY")
    total_quantity: Optional[int] = Field(None, description="Общее количество товаров")


class ParsingStatistics(BaseModel):
    """Статистика парсинга"""
    
    total_orders_found: int = Field(..., description="Количество найденных заказов")
    expected_quantity: int = Field(0, description="Ожидаемое количество из заголовка")
    parsing_success: bool = Field(..., description="Успешность парсинга")
    
    @property
    def quantity_match(self) -> bool:
        """Проверяет, совпадает ли количество найденных заказов с ожидаемым"""
        return self.total_orders_found == self.expected_quantity


class PickingListParseResult(BaseModel):
    """Результат парсинга листа подбора"""
    
    orders: List[OrderItem] = Field(..., description="Список заказов с метаданными")
    statistics: ParsingStatistics = Field(..., description="Статистика парсинга")
    
    class Config:
        json_schema_extra = {
            "example": {
                "orders": [
                    {
                        "order_id": "3767432495",
                        "brand": "Аппликатор Кузнецова",
                        "product_name": "Массажный коврик игольчатый подушка для спины шеи ног",
                        "size": "0",
                        "color": "фиолетовый",
                        "seller_article": "wild105d",
                        "sticker_code": "4082875",
                        "sticker_number": "0025",
                        "source_file": "лист_подбора.pdf",
                        "parsed_at": "2025-08-29T10:30:00.000000",
                        "parser_version": "1.0.0",
                        "supply_id": "WB-GI-176731503",
                        "date": "2025-08-26",
                        "date_original": "26.08.2025",
                        "total_quantity": 82,
                        "parsed_by": "admin",
                        "original_filename": "лист_подбора.pdf"
                    }
                ],
                "statistics": {
                    "total_orders_found": 82,
                    "expected_quantity": 82,
                    "parsing_success": True
                }
            }
        }


class ParseRequest(BaseModel):
    """Запрос на парсинг (для документации)"""
    
    file: str = Field(..., description="PDF файл для парсинга", example="picking_list.pdf")
    
    class Config:
        json_schema_extra = {
            "example": {
                "file": "лист_подбора.pdf"
            }
        }


class ParsingErrorResponse(BaseModel):
    """Ответ при ошибке парсинга"""
    
    detail: str = Field(..., description="Описание ошибки")
    error_type: str = Field("parsing_error", description="Тип ошибки")
    
    class Config:
        json_schema_extra = {
            "example": {
                "detail": "Не удалось извлечь текст из PDF файла",
                "error_type": "parsing_error"
            }
        }