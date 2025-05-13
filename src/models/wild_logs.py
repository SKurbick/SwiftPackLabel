import json
from typing import Dict, Any, Optional
from src.db import db
from src.logger import app_logger as logger


class WildLogsDB:
    """Класс для работы с таблицей operator_wild_responsibility в базе данных."""

    def __init__(self, db_connection=None):
        self.db = db_connection or db

    async def insert_log(
        self,
        operator_name: str,
        wild_code: str,
        order_count: int,
        processing_time: float,
        product_name: str,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Вставляет запись о работе с wild в таблицу логов.
        Args:
            operator_name: Имя оператора, работающего с wild
            wild_code: Код wild, с которым работает оператор
            order_count: Количество заказов для данного wild
            processing_time: Время до получения wild в минутах
            product_name: Наименование товара
            additional_data: Дополнительные данные в формате JSON
        Returns:
            bool: True если запись успешна, False в случае ошибки
        """
        try:
            additional_data_json = json.dumps(additional_data) if additional_data else None
            query = """
            INSERT INTO operator_wild_responsibility 
            (operator_name, wild_code, order_count, processing_time, product_name, additional_data) 
            VALUES ($1, $2, $3, $4, $5, $6)
            """
            
            await self.db.execute(
                query,
                operator_name,
                wild_code,
                order_count,
                processing_time,
                product_name,
                additional_data_json)
            
            logger.info(f"Добавлена запись в таблицу логов: {operator_name} - {wild_code}")
            return True
        except Exception as e:
            logger.error(f"Ошибка при записи в таблицу логов: {str(e)}")
            return False
