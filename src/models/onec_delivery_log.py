import json
import uuid
from datetime import datetime
from typing import Dict, Any, List
from src.logger import app_logger as logger


class OneCDeliveryLog:
    """
    Работа с таблицей onec_delivery_log для отслеживания интеграции с 1C.
    
    Класс для сохранения данных, отправленных в 1C.
    """

    def __init__(self, db):
        self.db = db

    async def save_delivery_logs(self, logs: List[Dict[str, Any]]) -> bool:
        """
        Сохраняет множественные записи логов доставки в БД.
        
        Args:
            logs: Список записей для сохранения
            
        Returns:
            bool: True если успешно сохранено
        """
        if not logs:
            logger.warning("Пустой список логов для сохранения")
            return True
            
        try:
            query = """
            INSERT INTO public.onec_delivery_log (
                integration_id, account_name, inn, supply_id, wild_code, order_id, 
                nm_id, price, count, status, sent_at, original_order_data, 
                formatted_data, response_data, error_details
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            """
            
            # Подготавливаем данные для batch insert
            values = []
            values.extend(
                (
                    log.get('integration_id'),
                    log.get('account_name'),
                    log.get('inn'),
                    log.get('supply_id'),
                    log.get('wild_code'),
                    log.get('order_id'),
                    log.get('nm_id'),
                    log.get('price'),
                    log.get('count', 1),
                    log.get('status', 'pending'),
                    log.get('sent_at'),
                    (
                        json.dumps(log.get('original_order_data'))
                        if log.get('original_order_data')
                        else None
                    ),
                    (
                        json.dumps(log.get('formatted_data'))
                        if log.get('formatted_data')
                        else None
                    ),
                    (
                        json.dumps(log.get('response_data'))
                        if log.get('response_data')
                        else None
                    ),
                    (
                        json.dumps(log.get('error_details'))
                        if log.get('error_details')
                        else None
                    ),
                )
                for log in logs
            )
            await self.db.executemany(query, values)

            logger.info(f"Сохранено {len(logs)} записей логов доставки в 1C")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения логов доставки: {str(e)}")
            return False

    @staticmethod
    def prepare_delivery_data_for_logging(
        integration_response: Dict[str, Any],
        original_request_data: Dict[str, Any],
        response_from_1c: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Подготавливает данные для логирования в БД на основе ответа интеграции.
        
        Args:
            integration_response: Данные из метода format_delivery_data
            original_request_data: Оригинальные данные запроса (supply_ids, order_wild_map)
            response_from_1c: Ответ от системы 1C
            
        Returns:
            List[Dict]: Подготовленные записи для сохранения
        """
        logs = []
        integration_id = str(uuid.uuid4())
        sent_at = datetime.now()
        
        # Определяем статус на основе ответа 1C
        status = "success"
        error_details = None
        
        if response_from_1c.get("status_code", 200) != 200:
            status = "error" 
            error_details = {
                "status_code": response_from_1c.get("status_code"),
                "message": response_from_1c.get("message"),
                "response": response_from_1c.get("response")
            }
        
        try:
            # Проходим по всем аккаунтам в данных интеграции
            accounts_data = integration_response.get("accounts", [])
            
            for account_data in accounts_data:
                account_name = account_data.get("account", "")
                inn = account_data.get("inn", "")
                
                # Проходим по всем поставкам аккаунта
                for supply_item in account_data.get("data", []):
                    supply_id = supply_item.get("supply_id", "")
                    
                    # Проходим по всем wild-кодам поставки
                    for wild_item in supply_item.get("wilds", []):
                        wild_code = wild_item.get("wild_code", "")
                        
                        # Проходим по всем заказам wild-кода
                        for order in wild_item.get("orders", []):
                            order_id = str(order.get("order_id", ""))
                            
                            log_entry = {
                                "integration_id": integration_id,
                                "account_name": account_name,
                                "inn": inn,
                                "supply_id": supply_id,
                                "wild_code": wild_code,
                                "order_id": order_id,
                                "nm_id": order.get("nm_id"),
                                "price": float(order.get("price", 0)),
                                "count": order.get("count", 1),
                                "status": status,
                                "sent_at": sent_at,
                                "original_order_data": None,  # Не используется
                                "formatted_data": {
                                    "account": account_name,
                                    "inn": inn,
                                    "supply_id": supply_id,
                                    "wild_code": wild_code,
                                    "order": order
                                },
                                "response_data": response_from_1c,
                                "error_details": error_details
                            }
                            
                            logs.append(log_entry)
            
            logger.info(f"Подготовлено {len(logs)} записей для логирования интеграции с 1C")
            return logs
            
        except Exception as e:
            logger.error(f"Ошибка подготовки данных для логирования: {str(e)}")
            return []

    async def save_integration_logs(
        self,
        integration_response: Dict[str, Any],
        original_request_data: Dict[str, Any],
        response_from_1c: Dict[str, Any]
    ) -> bool:
        """
        Агрегирует данные и сохраняет логи интеграции в БД.
        Этот метод вызывается после отправки данных в 1C.
        
        Args:
            integration_response: Данные из format_delivery_data
            original_request_data: Оригинальные данные запроса
            response_from_1c: Ответ от 1C
            
        Returns:
            bool: True если успешно сохранено
        """
        try:
            # Подготавливаем данные для логирования
            logs = self.prepare_delivery_data_for_logging(
                integration_response, 
                original_request_data, 
                response_from_1c
            )
            
            if not logs:
                logger.warning("Нет данных для сохранения в логи интеграции")
                return True
            
            # Сохраняем подготовленные логи
            return await self.save_delivery_logs(logs)
            
        except Exception as e:
            logger.error(f"Ошибка сохранения логов интеграции: {str(e)}")
            return False

    async def log_1c_integration(self, request_data: Dict[str, Any], response_data: Dict[str, Any]) -> None:
        """
        Простой метод для логирования интеграции с 1C.
        
        Args:
            request_data: Данные запроса, отправленного в 1C
            response_data: Ответ от 1C
        """
        try:
            await self.save_integration_logs(
                integration_response=request_data,
                original_request_data={},
                response_from_1c=response_data
            )
        except Exception as e:
            logger.error(f"Ошибка логирования 1C интеграции: {str(e)}")