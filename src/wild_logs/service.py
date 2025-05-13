from src.logger import app_logger as logger
from src.models.wild_logs import WildLogsDB
from src.wild_logs.schema import WildLogCreate


class WildLogService:
    """Сервис для работы с логами операций с wild-кодами."""

    def __init__(self, db=None):
        self.wild_logs_db = WildLogsDB(db)

    async def create_log(self, log_data: WildLogCreate) -> bool:
        """
        Записывает информацию об операции с wild-кодом в базу данных.
        
        Args:
            log_data: Данные для записи в лог
            
        Returns:
            bool: True если запись успешна, False в случае ошибки
        """
        try:
            success = await self.wild_logs_db.insert_log(
                operator_name=log_data.operator_name,
                wild_code=log_data.wild_code,
                order_count=log_data.order_count,
                processing_time=log_data.processing_time,
                product_name=log_data.product_name,
                additional_data=log_data.additional_data)
            if success:
                logger.info(f"Создан лог операции с wild: {log_data.operator_name} - {log_data.wild_code}")

            return success
        except Exception as e:
            logger.error(f"Ошибка при создании лога операции с wild: {str(e)}")
            return False
