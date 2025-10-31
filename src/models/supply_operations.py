"""
Модель для работы с операциями создания поставок
"""
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
from collections import defaultdict
from src.db import db
from src.logger import get_logger

logger = get_logger()


class SupplyOperationsDB:
    """Класс для работы с операциями создания поставок в базе данных"""
    
    @staticmethod
    async def save_operation_start(operation_id: str, user_id: int, request_payload: Dict[str, Any], 
                                   supply_name: str = None, supply_date: str = None) -> bool:
        """
        Сохраняет начало операции создания поставки.
        
        Args:
            operation_id: Уникальный идентификатор операции
            user_id: ID пользователя
            request_payload: Входные данные запроса
            supply_name: Наименование поставки
            supply_date: Дата поставки
            
        Returns:
            bool: True если операция успешно сохранена
        """
        try:
            query = """
                    INSERT INTO supply_operations (operation_id, user_id, request_payload, status, supply_name, supply_date)
                VALUES ($1, $2, $3, 'PROCESSING', $4, $5)
                ON CONFLICT (operation_id) DO NOTHING
                RETURNING id
            """
            result = await db.fetchrow(query, operation_id, user_id, json.dumps(request_payload), supply_name, supply_date)
            
            if result:
                logger.info(f"Операция {operation_id} сохранена для пользователя {user_id}")
                return True
            else:
                logger.warning(f"Операция {operation_id} уже существует")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при сохранении начала операции {operation_id}: {e}")
            raise

    @staticmethod
    async def save_operation_success(operation_id: str, response_data: Dict[str, Any]) -> bool:
        """
        Сохраняет успешный результат операции.
        
        Args:
            operation_id: Уникальный идентификатор операции
            response_data: Результат операции
            
        Returns:
            bool: True если результат успешно сохранен
        """
        try:
            query = """
                UPDATE supply_operations 
                SET response_data = $2, status = 'SUCCESS', completed_at = CURRENT_TIMESTAMP
                WHERE operation_id = $1
                RETURNING id
            """
            result = await db.fetchrow(query, operation_id, json.dumps(response_data))
            
            if result:
                logger.info(f"Операция {operation_id} завершена успешно")
                return True
            else:
                logger.warning(f"Операция {operation_id} не найдена для обновления")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при сохранении результата операции {operation_id}: {e}")
            raise

    @staticmethod
    async def save_operation_error(operation_id: str, error_message: str) -> bool:
        """
        Сохраняет ошибку операции.
        
        Args:
            operation_id: Уникальный идентификатор операции
            error_message: Текст ошибки
            
        Returns:
            bool: True если ошибка успешно сохранена
        """
        try:
            query = """
                UPDATE supply_operations 
                SET error_message = $2, status = 'FAILED', completed_at = CURRENT_TIMESTAMP
                WHERE operation_id = $1
                RETURNING id
            """
            result = await db.fetchrow(query, operation_id, error_message)
            
            if result:
                logger.info(f"Операция {operation_id} завершена с ошибкой: {error_message}")
                return True
            else:
                logger.warning(f"Операция {operation_id} не найдена для обновления ошибки")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при сохранении ошибки операции {operation_id}: {e}")
            raise

    @staticmethod
    async def get_operation_by_id(operation_id: str) -> Optional[Dict[str, Any]]:
        """
        Получает операцию по ID.
        
        Args:
            operation_id: Уникальный идентификатор операции
            
        Returns:
            Dict с данными операции или None если не найдена
        """
        try:
            query = """
                SELECT id, operation_id, user_id, request_payload, response_data, 
                       status, error_message, created_at, completed_at, supply_name, supply_date
                FROM supply_operations 
                WHERE operation_id = $1
            """
            result = await db.fetchrow(query, operation_id)
            
            if result:
                operation_data = dict(result)
                # Парсим JSON поля
                if operation_data['request_payload']:
                    operation_data['request_payload'] = json.loads(operation_data['request_payload'])
                if operation_data['response_data']:
                    operation_data['response_data'] = json.loads(operation_data['response_data'])
                
                logger.info(f"Найдена операция {operation_id} со статусом {operation_data['status']}")
                return operation_data
            else:
                logger.info(f"Операция {operation_id} не найдена")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при получении операции {operation_id}: {e}")
            raise

    @staticmethod
    async def get_latest_user_operation(user_id: int, limit: int = 1) -> Optional[List[Dict[str, Any]]]:
        """
        Получает последнюю операцию пользователя.
        
        Args:
            user_id: ID пользователя
            limit: Количество операций для получения (по умолчанию 1)
            
        Returns:
            Dict с данными последней операции или None если не найдена
        """
        try:
            query = """
                SELECT id, operation_id, user_id, request_payload, response_data, 
                       status, error_message, created_at, completed_at, supply_name, supply_date
                FROM supply_operations 
                WHERE user_id = $1 
                ORDER BY created_at DESC 
                LIMIT $2
            """
            result = await db.fetch(query, user_id, limit)
            
            if result:
                operations = []
                for row in result:
                    operation_data = dict(row)
                    # Парсим JSON поля
                    if operation_data['request_payload']:
                        operation_data['request_payload'] = json.loads(operation_data['request_payload'])
                    if operation_data['response_data']:
                        operation_data['response_data'] = json.loads(operation_data['response_data'])
                    operations.append(operation_data)
                
                logger.info(f"Найдено {len(operations)} операций для пользователя {user_id}")
                return operations
            else:
                logger.info(f"Операции для пользователя {user_id} не найдены")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при получении последней операции пользователя {user_id}: {e}")
            raise

    @staticmethod
    async def get_user_operations_history(user_id: int, limit: int = 10, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Получает историю операций пользователя с пагинацией.
        
        Args:
            user_id: ID пользователя
            limit: Максимальное количество операций
            offset: Смещение для пагинации
            
        Returns:
            List с операциями пользователя
        """
        try:
            query = """
                SELECT id, operation_id, user_id, request_payload, response_data, 
                       status, error_message, created_at, completed_at, supply_name, supply_date
                FROM supply_operations 
                WHERE user_id = $1 
                ORDER BY created_at DESC 
                LIMIT $2 OFFSET $3
            """
            result = await db.fetch(query, user_id, limit, offset)
            
            operations = []
            for row in result:
                operation_data = dict(row)
                # Парсим JSON поля
                if operation_data['request_payload']:
                    operation_data['request_payload'] = json.loads(operation_data['request_payload'])
                if operation_data['response_data']:
                    operation_data['response_data'] = json.loads(operation_data['response_data'])
                operations.append(operation_data)
            
            logger.info(f"Получено {len(operations)} операций для пользователя {user_id}")
            return operations
            
        except Exception as e:
            logger.error(f"Ошибка при получении истории операций пользователя {user_id}: {e}")
            raise

    @staticmethod
    async def cleanup_old_operations(days_old: int = 30) -> int:
        """
        Удаляет старые операции для очистки базы данных.
        
        Args:
            days_old: Количество дней для определения старых операций
            
        Returns:
            int: Количество удаленных операций
        """
        try:
            query = """
                DELETE FROM supply_operations 
                WHERE created_at < CURRENT_TIMESTAMP - INTERVAL $1
                AND status IN ('SUCCESS', 'FAILED')
            """
            result = await db.execute(query, f"{days_old} days")
            
            # Извлекаем количество удаленных строк из результата
            deleted_count = int(result.split()[-1]) if result else 0
            
            logger.info(f"Удалено {deleted_count} старых операций (старше {days_old} дней)")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Ошибка при очистке старых операций: {e}")
            raise

    @staticmethod
    async def get_sessions_list(limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Получает общий список сессий с базовой информацией (название, id, время создания).
        Исключает пустые сессии (где wilds = []).

        Args:
            limit: Максимальное количество сессий
            offset: Смещение для пагинации

        Returns:
            List с базовой информацией о сессиях (без пустых)
        """
        try:
            query = """
                SELECT operation_id, supply_name, supply_date, status, created_at, completed_at
                FROM supply_operations
                WHERE response_data::jsonb->'wilds' != '[]'::jsonb
                ORDER BY created_at DESC
                LIMIT $1 OFFSET $2
            """
            result = await db.fetch(query, limit, offset)
            
            sessions = []
            for row in result:
                session_data = {
                    'operation_id': row['operation_id'],
                    'supply_name': row['supply_name'],
                    'supply_date': row['supply_date'],
                    'status': row['status'],
                    'created_at': row['created_at'],
                    'completed_at': row['completed_at']
                }
                sessions.append(session_data)
            
            logger.info(f"Получено {len(sessions)} сессий из базы данных")
            return sessions
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка сессий: {e}")
            raise

    @staticmethod
    async def get_session_full_info(operation_id: str) -> Optional[Dict[str, Any]]:
        """
        Получает полную информацию о сессии по ID.
        
        Args:
            operation_id: Уникальный идентификатор операции
            
        Returns:
            Dict с полной информацией о сессии или None если не найдена
        """
        try:
            query = """
                SELECT id, operation_id, user_id, request_payload, response_data, 
                       status, error_message, created_at, completed_at, supply_name, supply_date
                FROM supply_operations 
                WHERE operation_id = $1
            """
            result = await db.fetchrow(query, operation_id)
            
            if result:
                session_data = dict(result)
                # Парсим JSON поля
                if session_data['request_payload']:
                    session_data['request_payload'] = json.loads(session_data['request_payload'])
                if session_data['response_data']:
                    session_data['response_data'] = json.loads(session_data['response_data'])
                
                logger.info(f"Найдена полная информация для сессии {operation_id}")
                return session_data
            else:
                logger.info(f"Сессия {operation_id} не найдена")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при получении полной информации о сессии {operation_id}: {e}")
            raise

    @staticmethod
    async def update_response_data_after_move(
        operation_id: str,
        removed_order_ids: List[int]
    ) -> bool:
        """
        Обновляет response_data после перемещения заказов.

        Удаляет конкретные заказы из:
        - supply_ids[].order_ids (списки ID заказов в каждой поставке)
        - order_wild_map (маппинг заказов на wild-коды)
        - wilds[].count (обновляет счетчики)

        Args:
            operation_id: ID операции для обновления
            removed_order_ids: Список ID заказов, которые были перемещены

        Returns:
            bool: True если обновление успешно
        """
        try:
            if not removed_order_ids:
                logger.info(f"Нет заказов для удаления из операции {operation_id}")
                return True

            # 1. Получаем response_data
            query_select = """
                SELECT response_data
                FROM supply_operations
                WHERE operation_id = $1
            """
            result = await db.fetchrow(query_select, operation_id)

            if not result:
                logger.warning(f"Операция {operation_id} не найдена для обновления")
                return False

            current_response = json.loads(result['response_data'])

            # 2. Проверка структуры response_data
            if not all(key in current_response for key in ['supply_ids', 'wilds', 'order_wild_map']):
                logger.warning(f"Операция {operation_id} имеет некорректную структуру response_data")
                return False

            removed_ids_set = set(removed_order_ids)
            removed_ids_str_set = {str(oid) for oid in removed_order_ids}

            # 3. Удаляем заказы из supply_ids и считаем удаленные по wild-кодам
            wild_removed_counts = defaultdict(int)
            supplies_to_remove = []

            for idx, supply_item in enumerate(current_response['supply_ids']):
                if 'order_ids' not in supply_item:
                    continue

                original_count = len(supply_item['order_ids'])

                # Фильтруем order_ids, удаляя перемещенные
                supply_item['order_ids'] = [
                    oid for oid in supply_item['order_ids']
                    if oid not in removed_ids_set
                ]

                new_count = len(supply_item['order_ids'])
                removed_count = original_count - new_count

                if removed_count > 0:
                    logger.info(
                        f"Supply {supply_item.get('supply_id', 'unknown')}: "
                        f"удалено {removed_count} заказов ({original_count} -> {new_count})"
                    )

                # Если поставка стала пустой, помечаем для удаления
                if new_count == 0:
                    supplies_to_remove.append(idx)
                    logger.info(f"Supply {supply_item.get('supply_id', 'unknown')} будет удален (order_ids=0)")

            # Удаляем пустые поставки (в обратном порядке чтобы не сбить индексы)
            for idx in reversed(supplies_to_remove):
                del current_response['supply_ids'][idx]

            # 4. Удаляем заказы из order_wild_map и считаем удаления по wild-кодам
            for order_id_str in removed_ids_str_set:
                if order_id_str in current_response['order_wild_map']:
                    wild_code = current_response['order_wild_map'][order_id_str]
                    wild_removed_counts[wild_code] += 1
                    del current_response['order_wild_map'][order_id_str]

            # 5. Обновляем счетчики в wilds
            wilds_to_remove = []
            for idx, wild_item in enumerate(current_response['wilds']):
                wild_code = wild_item.get('wild')
                if wild_code in wild_removed_counts:
                    removed_count = wild_removed_counts[wild_code]
                    wild_item['count'] -= removed_count

                    logger.info(
                        f"Wild {wild_code}: обновлен счетчик "
                        f"(-{removed_count}, новый count={wild_item['count']})"
                    )

                    # Если count стал 0 или отрицательным, помечаем для удаления
                    if wild_item['count'] <= 0:
                        wilds_to_remove.append(idx)
                        logger.info(f"Wild {wild_code} будет удален (count={wild_item['count']})")

            # Удаляем wild-коды с count=0 (в обратном порядке)
            for idx in reversed(wilds_to_remove):
                del current_response['wilds'][idx]

            # 6. Обновляем response_data в БД
            query_update = """
                UPDATE supply_operations
                SET response_data = $2,
                    updated_at = CURRENT_TIMESTAMP
                WHERE operation_id = $1
                RETURNING id
            """

            updated = await db.fetchrow(
                query_update,
                operation_id,
                json.dumps(current_response)
            )

            if updated:
                logger.info(
                    f"Response data обновлён для операции {operation_id}: "
                    f"удалено {len(removed_order_ids)} заказов из {len(wild_removed_counts)} wild-кодов"
                )
                return True
            else:
                logger.error(f"Не удалось обновить response_data для операции {operation_id}")
                return False

        except Exception as e:
            logger.error(f"Ошибка при обновлении response_data операции {operation_id}: {e}")
            raise