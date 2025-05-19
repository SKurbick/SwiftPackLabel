import re
from typing import List, Dict, Tuple, Optional
from src.logger import app_logger as logger
from src.qr_parser.schema import WildParserResponse


class WildParserService:
    """Сервис для парсинга строк формата 'wild123/23'."""
    
    def __init__(self, db=None):
        """
        Инициализирует сервис для парсинга wild-строк.
        Args:
            db: Соединение с базой данных (опционально)
        """
        self.db = db
    
    async def parse_wild_string(self, wild_string: str) -> WildParserResponse:
        """
        Парсит строку формата 'wild123/23', извлекая wild и количество.
        Args:
            wild_string: Строка в формате 'wild123/23'
        Returns:
            WildParserResponse: Информация о товаре, извлеченная из строки
        """
        wild_code, quantity = self._extract_wild_and_quantity(wild_string)
        name = await self._get_product_name(wild_code)
        photos = await self._get_product_photos(wild_code)
        
        return WildParserResponse(
            wild=wild_code,
            quantity=quantity,
            name=name,
            photos=photos
        )
    @staticmethod
    def _extract_wild_and_quantity(wild_string: str) -> Tuple[str, int]:
        """
        Извлекает wild-код и количество из строки.
        Args:
            wild_string: Строка в формате 'wild123/23'
        Returns:
            Tuple[str, int]: wild-код и количество
        """
        try:
            parts = wild_string.split('/')
            if len(parts) != 2:
                logger.error(f"Неправильный формат строки wild: {wild_string}")
                raise ValueError(f"Строка должна быть в формате 'wild123/23', получено: {wild_string}")
            
            wild_code = parts[0].strip()
            quantity = int(parts[1].strip())
            return wild_code, quantity
        except (ValueError, IndexError) as e:
            logger.error(f"Ошибка при парсинге строки wild '{wild_string}': {str(e)}")
            raise ValueError(f"Ошибка при парсинге строки: {str(e)}")
    
    async def _get_product_name(self, wild_code: str) -> str:
        """
        Получает наименование товара по wild-коду.
        В будущем здесь будет реализация извлечения из базы или API.
        
        Args:
            wild_code: Wild-код товара
            
        Returns:
            str: Наименование товара
        """
        # TODO: Реализовать извлечение наименования из базы или API
        return f"Товар с артикулом {wild_code}"
    
    async def _get_product_photos(self, wild_code: str) -> List[str]:
        """
        Получает URL фотографий товара по wild-коду.
        В будущем здесь будет реализация извлечения из базы или API.
        
        Args:
            wild_code: Wild-код товара
            
        Returns:
            List[str]: Список URL фотографий товара
        """
        # TODO: Реализовать извлечение фото из базы или API
        # Пока используем заглушку
        return [
            f"https://example.com/photos/{wild_code}_1.jpg",
            f"https://example.com/photos/{wild_code}_2.jpg",
            f"https://example.com/photos/{wild_code}_3.jpg",
            f"https://example.com/photos/{wild_code}_4.jpg"
        ]
