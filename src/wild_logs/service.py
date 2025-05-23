from src.logger import app_logger as logger
from src.models.wild_logs import WildLogsDB
from src.wild_logs.schema import WildLogCreate, ShiftSupervisorData


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
                additional_data=log_data.additional_data,
                session_id=log_data.session_id)
            if success:
                logger.info(f"Создан лог операции с wild: {log_data.operator_name} - {log_data.wild_code}")

            return success
        except Exception as e:
            logger.error(f"Ошибка при создании лога операции с wild: {str(e)}")
            return False

    async def update_supervisor_info(self, data: ShiftSupervisorData) -> bool:
        """
        Обновляет информацию о старшем операторе для записей с указанным session_id.
        
        Args:
            data: Данные для обновления информации о старшем операторе
            
        Returns:
            bool: True если обновление успешно, False в случае ошибки
        """
        try:
            success = await self.wild_logs_db.update_supervisor_info(
                session_id=data.session_id,
                supervisor_password=data.supervisor_password)

            if success:
                logger.info(f"Обновлена информация о старшем операторе для сессии: {data.session_id}")

            return success
        except Exception as e:
            logger.error(f"Ошибка при обновлении информации о старшем операторе: {str(e)}")
            return False
