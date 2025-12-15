import json
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime, timedelta

from numpy import record

from src.logger import app_logger as logger
from src.supplies.schema import HangingSuppliesWithOverdueOrders, BaseHangingSuppliesData


class HangingSupplies:
    """
    Работа с таблицей hanging_supplies.
    
    Таблица содержит информацию о висячих поставках - поставках, которые создаются 
    и переводятся в статус доставки даже при отсутствии товара.
    
    Columns:
        id (serial, primary key)
        supply_id (varchar): ID поставки
        account (varchar): Аккаунт Wildberries
        order_data (jsonb): Полные данные о заказах поставки
        shipped_orders (jsonb): Отгруженные заказы
        changes_log (jsonb): Лог изменений в составе поставки
        created_at (timestamptz): Время создания записи, по умолчанию CURRENT_TIMESTAMP
        operator (varchar): Оператор, создавший поставку
        is_fictitious_delivered (boolean): Флаг фиктивной доставки
        fictitious_delivered_at (timestamptz): Время перевода в фиктивную доставку
        fictitious_delivery_operator (varchar): Оператор фиктивной доставки
    """

    def __init__(self, db):
        self.db = db

    async def save_hanging_supply(self, supply_id: str, account: str, order_data: str, operator: str = 'unknown') -> bool:
        """
        Сохраняет информацию о висячей поставке в БД.
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            order_data: Данные о заказах в поставке в формате JSON
            operator: Имя пользователя (оператора), создавшего висячую поставку
        Returns:
            bool: True, если запись успешно создана/обновлена, иначе False
        """
        try:
            query = """
            INSERT INTO public.hanging_supplies (supply_id, account, order_data, operator)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (supply_id, account) 
            DO UPDATE SET order_data = $3, created_at = CURRENT_TIMESTAMP, operator = $4
            RETURNING id
            """
            result = await self.db.fetchrow(query, supply_id, account, order_data, operator)
            return result is not None
        except Exception as e:
            logger.error(f"Ошибка при сохранении висячей поставки: {str(e)}")
            return False

    async def get_hanging_supplies(self) -> List[Dict[str, Any]]:
        """
        Получает список всех висячих поставок, опционально фильтруя по аккаунту.
        Args:
            account: Аккаунт Wildberries (опционально)
        Returns:
            List[Dict[str, Any]]: Список висячих поставок
        """
        try:
            query = """
            SELECT * FROM public.hanging_supplies
            ORDER BY created_at DESC
            LIMIT 1000
            """
            result = await self.db.fetch(query)
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Ошибка при получении висячих поставок: {str(e)}")
            return []
    
    async def update_changes_log(self, supply_id: str, account: str, changes_log_entries: List[Dict[str, Any]]) -> bool:
        """
        Обновляет changes_log в БД, добавляя новые записи.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            changes_log_entries: Список новых записей для лога
            
        Returns:
            bool: True если обновление прошло успешно, False иначе
        """
        try:
            query = """
            UPDATE public.hanging_supplies 
            SET changes_log = changes_log || $3::jsonb
            WHERE supply_id = $1 AND account = $2
            RETURNING id
            """
            result = await self.db.fetchrow(query, supply_id, account, json.dumps(changes_log_entries))
            success = result is not None
            if success:
                logger.debug(f"Обновлен changes_log для поставки {supply_id} ({account})")
            return success
        except Exception as e:
            logger.error(f"Ошибка обновления changes_log для поставки {supply_id} ({account}): {str(e)}")
            return False
    
    async def get_changes_log(self, supply_id: str, account: str) -> List[Dict[str, Any]]:
        """
        Получает лог изменений для конкретной поставки.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            
        Returns:
            List[Dict[str, Any]]: Лог изменений
        """
        try:
            query = """
            SELECT changes_log 
            FROM public.hanging_supplies 
            WHERE supply_id = $1 AND account = $2
            """
            result = await self.db.fetchrow(query, supply_id, account)
            if result and result['changes_log']:
                return result['changes_log']
            return []
        except Exception as e:
            logger.error(f"Ошибка получения changes_log для поставки {supply_id} ({account}): {str(e)}")
            return []
    
    async def get_changes_statistics(self) -> Dict[str, Any]:
        """
        Получает статистику изменений по всем висячим поставкам.
        
        Returns:
            Dict[str, Any]: Статистика изменений
        """
        try:
            query = """
            SELECT 
                supply_id,
                account,
                jsonb_array_length(COALESCE(changes_log, '[]'::jsonb)) as changes_count,
                created_at
            FROM public.hanging_supplies 
            WHERE jsonb_array_length(COALESCE(changes_log, '[]'::jsonb)) > 0
            ORDER BY changes_count DESC
            """
            result = await self.db.fetch(query)
            
            statistics = {
                "total_supplies_with_changes": len(result),
                "total_changes": sum(row['changes_count'] for row in result),
                "supplies": [dict(row) for row in result]
            }
            
            return statistics
        except Exception as e:
            logger.error(f"Ошибка получения статистики изменений: {str(e)}")
            return {"error": str(e)}
    
    async def get_hanging_supply_by_id(self, supply_id: str, account: str) -> Optional[Dict[str, Any]]:
        """
        Получает конкретную висячую поставку по ID и аккаунту.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            
        Returns:
            Optional[Dict[str, Any]]: Данные висячей поставки или None
        """
        try:
            query = """
            SELECT * FROM public.hanging_supplies 
            WHERE supply_id = $1 AND account = $2
            """
            result = await self.db.fetchrow(query, supply_id, account)
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Ошибка получения висячей поставки {supply_id} ({account}): {str(e)}")
            return None
    
    async def update_shipped_orders(self, supply_id: str, account: str, shipped_orders: List[Dict[str, Any]]) -> bool:
        """
        Обновляет список отгруженных заказов для поставки.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            shipped_orders: Список отгруженных заказов для добавления
            
        Returns:
            bool: True если обновление прошло успешно, False иначе
        """
        try:
            query = """
            UPDATE public.hanging_supplies 
            SET shipped_orders = shipped_orders || $3::jsonb
            WHERE supply_id = $1 AND account = $2
            RETURNING id
            """
            result = await self.db.fetchrow(query, supply_id, account, json.dumps(shipped_orders))
            success = result is not None
            if success:
                logger.info(f"Обновлен shipped_orders для поставки {supply_id} ({account}): добавлено {len(shipped_orders)} заказов")
            return success
        except Exception as e:
            logger.error(f"Ошибка обновления shipped_orders для поставки {supply_id} ({account}): {str(e)}")
            return False
    
    async def get_shipped_orders(self, supply_id: str, account: str) -> List[Dict[str, Any]]:
        """
        Получает список отгруженных заказов для поставки.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            
        Returns:
            List[Dict[str, Any]]: Список отгруженных заказов
        """
        try:
            query = """
            SELECT shipped_orders 
            FROM public.hanging_supplies 
            WHERE supply_id = $1 AND account = $2
            """
            result = await self.db.fetchrow(query, supply_id, account)
            if result and result['shipped_orders']:
                return result['shipped_orders']
            return []
        except Exception as e:
            logger.error(f"Ошибка получения shipped_orders для поставки {supply_id} ({account}): {str(e)}")
            return []
    
    async def cleanup_old_changes_log(self, days_to_keep: int = 30) -> Dict[str, Any]:
        """
        Очищает старые записи из changes_log для экономии места.
        
        Args:
            days_to_keep: Количество дней для хранения логов
            
        Returns:
            Dict[str, Any]: Результат очистки
        """
        try:
            # Вычисляем дату отсечения
            cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
            cutoff_date = cutoff_date.replace(hour=0, minute=0, second=0, microsecond=0)
            cutoff_timestamp = cutoff_date.isoformat()
            
            # Обновляем changes_log, удаляя старые записи
            query = """
            UPDATE public.hanging_supplies 
            SET changes_log = (
                SELECT jsonb_agg(log_entry)
                FROM jsonb_array_elements(changes_log) AS log_entry
                WHERE (log_entry->>'timestamp')::timestamp > $1::timestamp
            )
            WHERE jsonb_array_length(changes_log) > 0
            """
            
            await self.db.execute(query, cutoff_timestamp)
            
            # Получаем статистику после очистки
            stats_query = """
            SELECT 
                COUNT(*) as total_supplies,
                SUM(jsonb_array_length(COALESCE(changes_log, '[]'::jsonb))) as total_logs_remaining
            FROM public.hanging_supplies
            """
            
            stats = await self.db.fetchrow(stats_query)
            
            result = {
                "cutoff_date": cutoff_timestamp,
                "days_kept": days_to_keep,
                "total_supplies": stats['total_supplies'],
                "total_logs_remaining": stats['total_logs_remaining']
            }
            
            logger.info(f"Очистка changes_log завершена: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при очистке changes_log: {str(e)}")
            return {"error": str(e)}
    
    async def get_supplies_with_changes(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Получает висячие поставки, у которых есть изменения в changes_log.
        
        Args:
            limit: Максимальное количество записей для возврата
            
        Returns:
            List[Dict[str, Any]]: Список поставок с изменениями
        """
        try:
            query = """
            SELECT 
                supply_id,
                account,
                operator,
                created_at,
                jsonb_array_length(COALESCE(changes_log, '[]'::jsonb)) as changes_count,
                jsonb_array_length(COALESCE(shipped_orders, '[]'::jsonb)) as shipped_count
            FROM public.hanging_supplies 
            WHERE jsonb_array_length(COALESCE(changes_log, '[]'::jsonb)) > 0
            ORDER BY created_at DESC
            LIMIT $1
            """
            result = await self.db.fetch(query, limit)
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Ошибка получения поставок с изменениями: {str(e)}")
            return []
    
    async def update_order_data(self, supply_id: str, account: str, order_data: Dict[str, Any]) -> bool:
        """
        Обновляет order_data для висячей поставки.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            order_data: Новые данные заказов
            
        Returns:
            bool: True если обновление прошло успешно, False иначе
        """
        try:
            query = """
            UPDATE public.hanging_supplies 
            SET order_data = $3::jsonb
            WHERE supply_id = $1 AND account = $2
            RETURNING id
            """
            result = await self.db.fetchrow(query, supply_id, account, json.dumps(order_data))
            success = result is not None
            if success:
                logger.info(f"Обновлен order_data для поставки {supply_id} ({account})")
            return success
        except Exception as e:
            logger.error(f"Ошибка обновления order_data для поставки {supply_id} ({account}): {str(e)}")
            return False

    async def delete_hanging_supply(self, supply_id: str, account: str) -> bool:
        """
        Удаляет висячую поставку из БД.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            
        Returns:
            bool: True если удаление прошло успешно, False иначе
        """
        try:
            query = """
            DELETE FROM public.hanging_supplies 
            WHERE supply_id = $1 AND account = $2
            RETURNING id
            """
            result = await self.db.fetchrow(query, supply_id, account)
            success = result is not None
            if success:
                logger.info(f"Удалена висячая поставка {supply_id} ({account})")
            return success
        except Exception as e:
            logger.error(f"Ошибка удаления висячей поставки {supply_id} ({account}): {str(e)}")
            return False

    async def get_order_data_by_supplies(self, supply_ids: List[str]) -> Dict[str, dict]:
        """
        Получает order_data и shipped_orders для списка поставок оптимизированным запросом.
        Args:
            supply_ids: Список ID поставок
        Returns:
            Dict[str, dict]: Словарь {supply_id: {"order_data": ..., "shipped_orders": ..., "account": ...}}
        """
        if not supply_ids:
            return {}
            
        try:
            placeholders = ','.join(f"${i+1}" for i in range(len(supply_ids)))
            query = f"""
                SELECT supply_id, account, order_data, shipped_orders 
                FROM public.hanging_supplies 
                WHERE supply_id IN ({placeholders})
            """
            
            rows = await self.db.fetch(query, *supply_ids)
            logger.info(f"Получено {len(rows)} записей order_data и shipped_orders из таблицы hanging_supplies")
            
            return {
                row['supply_id']: {
                    "order_data": row['order_data'],
                    "shipped_orders": row['shipped_orders'] or [],
                    "account": row['account']
                } 
                for row in rows
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении order_data для поставок: {str(e)}")
            return {}

    async def mark_as_fictitious_delivered(self, supply_id: str, account: str, operator: str = 'unknown') -> bool:
        """
        Помечает висячую поставку как переведенную в фиктивную доставку.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            operator: Оператор, переводивший в фиктивную доставку
            
        Returns:
            bool: True если обновление прошло успешно, False иначе
        """
        try:
            query = """
            UPDATE public.hanging_supplies 
            SET is_fictitious_delivered = true,
                fictitious_delivered_at = CURRENT_TIMESTAMP,
                fictitious_delivery_operator = $3
            WHERE supply_id = $1 AND account = $2 AND is_fictitious_delivered = false
            RETURNING id
            """
            result = await self.db.fetchrow(query, supply_id, account, operator)
            success = result is not None
            if success:
                logger.info(f"Поставка {supply_id} ({account}) помечена как фиктивно доставленная оператором {operator}")
            else:
                logger.warning(f"Поставка {supply_id} ({account}) не найдена или уже помечена как фиктивно доставленная")
            return success
        except Exception as e:
            logger.error(f"Ошибка пометки поставки {supply_id} ({account}) как фиктивно доставленной: {str(e)}")
            return False

    async def get_fictitious_delivered_supplies(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Получает список фиктивно доставленных висячих поставок.
        
        Args:
            limit: Максимальное количество записей для возврата
            
        Returns:
            List[Dict[str, Any]]: Список фиктивно доставленных поставок
        """
        try:
            query = """
            SELECT 
                supply_id,
                account,
                operator,
                created_at,
                fictitious_delivered_at,
                fictitious_delivery_operator,
                jsonb_array_length(COALESCE(order_data->'orders', '[]'::jsonb)) as orders_count,
                jsonb_array_length(COALESCE(shipped_orders, '[]'::jsonb)) as shipped_count
            FROM public.hanging_supplies 
            WHERE is_fictitious_delivered = true
            ORDER BY fictitious_delivered_at DESC
            LIMIT $1
            """
            result = await self.db.fetch(query, limit)
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Ошибка получения фиктивно доставленных поставок: {str(e)}")
            return []

    async def is_fictitious_delivered(self, supply_id: str, account: str) -> bool:
        """
        Проверяет, помечена ли поставка как фиктивно доставленная.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            
        Returns:
            bool: True если поставка помечена как фиктивно доставленная, False иначе
        """
        try:
            query = """
            SELECT is_fictitious_delivered 
            FROM public.hanging_supplies 
            WHERE supply_id = $1 AND account = $2
            """
            result = await self.db.fetchrow(query, supply_id, account)
            return result['is_fictitious_delivered'] if result else False
        except Exception as e:
            logger.error(f"Ошибка проверки фиктивной доставки для поставки {supply_id} ({account}): {str(e)}")
            return False

    async def get_fictitious_delivery_info(self, supply_id: str, account: str) -> Optional[Dict[str, Any]]:
        """
        Получает информацию о фиктивной доставке поставки.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            
        Returns:
            Optional[Dict[str, Any]]: Информация о фиктивной доставке или None
        """
        try:
            query = """
            SELECT 
                is_fictitious_delivered,
                fictitious_delivered_at,
                fictitious_delivery_operator
            FROM public.hanging_supplies 
            WHERE supply_id = $1 AND account = $2 AND is_fictitious_delivered = true
            """
            result = await self.db.fetchrow(query, supply_id, account)
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Ошибка получения информации о фиктивной доставке для поставки {supply_id} ({account}): {str(e)}")
            return None

    async def get_weekly_fictitious_supplies_ids(self, is_fictitious_delivered: bool = True) -> List[Dict[str, str]]:
        """
        Получает уникальные supply_id из фиктивно доставленных висячих поставок за 3 дня.
        Возвращает данные в формате, совместимом с get_information_to_supplies().
        
        Args:
            is_fictitious_delivered: Фильтр по статусу фиктивной доставки
        
        Returns:
            List[Dict[str, str]]: Список словарей с supply_id и account для каждой поставки
        """
        query = """
        SELECT DISTINCT supply_id, account
        FROM public.hanging_supplies
        WHERE created_at >= CURRENT_DATE - INTERVAL '21 day'
        AND created_at < CURRENT_DATE + INTERVAL '1 day'
        AND is_fictitious_delivered = $1
        ORDER BY supply_id;
        """
        
        try:
            result = await self.db.fetch(query, is_fictitious_delivered)
            logger.info(f"Получено {len(result)} уникальных висячих поставок с is_fictitious_delivered={is_fictitious_delivered} за 3 дня")

            grouped_by_account = {}
            for row in result:
                account = row['account']
                supply_id = row['supply_id']
                
                if account not in grouped_by_account:
                    grouped_by_account[account] = []

                grouped_by_account[account].append({
                    'id': supply_id
                })

            return [grouped_by_account] if grouped_by_account else []
            
        except Exception as e:
            logger.error(f"Ошибка при получении висячих поставок с is_fictitious_delivered={is_fictitious_delivered}: {str(e)}")
            return []

    async def get_fictitious_shipped_order_ids(self, supply_id: str, account: str) -> List[int]:
        """
        Получает список фиктивно отгруженных order_id для поставки.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            
        Returns:
            List[int]: Список фиктивно отгруженных order_id
        """
        try:
            query = """
            SELECT fictitious_shipped_order_ids 
            FROM public.hanging_supplies 
            WHERE supply_id = $1 AND account = $2
            """
            result = await self.db.fetchrow(query, supply_id, account)
            
            if result:
                shipped_data = json.loads(result['fictitious_shipped_order_ids'])
                
                if shipped_data:
                    try:
                        order_ids = [item['order_id'] for item in shipped_data]
                        return order_ids
                    except (KeyError, TypeError) as parse_error:
                        logger.error(f"Ошибка парсинга fictitious_shipped_order_ids: {parse_error}, данные: {shipped_data}")
                        return []
            
            return []
        except Exception as e:
            logger.error(f"Ошибка получения фиктивно отгруженных order_id для поставки {supply_id} ({account}): {str(e)}")
            return []

    async def add_fictitious_shipped_order_ids(self, supply_id: str, account: str,
                                              order_ids: List[int], operator: Optional[str] = None) -> bool:
        """
        Добавляет новые фиктивно отгруженные order_id в поле fictitious_shipped_order_ids.

        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            order_ids: Список order_id для добавления
            operator: Оператор, выполняющий операцию (может быть None)

        Returns:
            bool: True если обновление прошло успешно, False иначе
        """
        try:
            timestamp = datetime.utcnow().isoformat()
            new_entries = [
                {"order_id": order_id,"shipped_at": timestamp,"operator": operator} for order_id in order_ids]
            
            query = """
            UPDATE public.hanging_supplies 
            SET fictitious_shipped_order_ids = fictitious_shipped_order_ids || $3::jsonb
            WHERE supply_id = $1 AND account = $2
            RETURNING id
            """
            result = await self.db.fetchrow(query, supply_id, account, json.dumps(new_entries))
            success = result is not None
            if success:
                logger.info(f"Добавлено {len(order_ids)} фиктивно отгруженных order_id для поставки {supply_id} ({account})")
            else:
                logger.warning(f"Поставка {supply_id} ({account}) не найдена для добавления фиктивно отгруженных order_id")
            return success
        except Exception as e:
            logger.error(f"Ошибка добавления фиктивно отгруженных order_id для поставки {supply_id} ({account}): {str(e)}")
            return False

    async def get_fictitious_shipped_order_ids_batch(self, supplies: Dict[str, str]) -> Dict[Tuple[str, str], List[int]]:
        """
        Получает фиктивно отгруженные order_id для группы поставок.
        
        Args:
            supplies: Словарь поставок {supply_id: account}
            
        Returns:
            Dict[Tuple[str, str], List[int]]: Словарь {(supply_id, account): [order_id1, order_id2, ...]}
        """
        result = {}
        for supply_id, account in supplies.items():
            shipped_ids = await self.get_fictitious_shipped_order_ids(supply_id, account)
            result[(supply_id, account)] = shipped_ids
        return result

    async def _sync_get_hanging_supplies_by_status(self) -> list[HangingSuppliesWithOverdueOrders]:
        """
        Получеие висячих поставок из hanging_supplies по активному статусу
        (assembly_task_status_mode.wb_status = 'waiting' /
        assembly_task_status_mode.supplier_status = 'confirm')
        """
        query = """
        with filtered_orders as (
            select
                hs.id, 
                hs.supply_id,
                min((elem->>'createdAt')::timestamptz) as earliest_order_date
            from hanging_supplies hs 
            cross join lateral jsonb_array_elements(
                case
                    when hs.order_data ? 'orders'
                    then hs.order_data->'orders'
                    else '[]'::jsonb
                end
            ) as elem
            where hs.order_data->'orders' != '[]'::jsonb
            group by hs.id, hs.supply_id
        ),
        filtered_hanging_supplies as (
            select distinct hs.id, hs.supply_id, hs.account
            from hanging_supplies hs
            join filtered_orders fo on hs.id = fo.id
            join supplies_and_orders sao on hs.supply_id = sao.supply_id
            join assembly_task_status_model atsm on sao.id = atsm.id
            where atsm.wb_status = 'waiting' 
                and atsm.supplier_status = 'confirm' 
                and hs.is_fictitious_delivered is false 
                and fo.earliest_order_date < now() - interval '60 hours'
        )
        select 
            fhs.supply_id,
            fhs.account,
            elem->>'id' as order_id
        from filtered_hanging_supplies fhs
        join hanging_supplies hs on fhs.id = hs.id
        cross join lateral jsonb_array_elements(
            case
                when hs.order_data ? 'orders'
                then hs.order_data->'orders'
                else '[]'::jsonb
            end
        ) as elem
        order by fhs.supply_id, order_id;
        """

        result = await self.db.fetch(query)

        return [HangingSuppliesWithOverdueOrders(
            supply_id=record.get('supply_id'),
            account=record.get('account'),
            order_id=record.get('order_id')
        ) for record in result]

    async def _sync_conversion_supply_into_fictitious_shipment(self, supplies_ids: set[str]):
        """Автоперевод висячих поставок в фиктивную доставку."""
        update_query = """
        UPDATE hanging_supplies
        SET is_fictitious_delivered = TRUE,
            fictitious_delivered_at = now(),
            fictitious_delivery_operator = 'auto_conversion'
        WHERE supply_id = ANY($1);
        """

        await self.db.execute(update_query, supplies_ids)

    async def _sync_update_orders_status(self, orders: list[tuple]) -> None:
        """
        Автоперевод сборочных заданий в статус IN_HANGING_SUPPLY, просроченных на > 60 часов
        в рамках перевода висячих поставок.
        """
        update_query = """
        INSERT INTO order_status_log 
            (order_id, status, supply_id, account)
        VALUES($1, 'IN_HANGING_SUPPLY', $2, $3)
        """

        await self.db.executemany(update_query, orders)


    async def _sync_get_hanging_supplies_with_invalid_status(self) -> list[BaseHangingSuppliesData]:
        """
        Получение ID висячих поставок, в которых ХОТЯ БЫ ОДНО сборочное задание было отправлено
        (status_model_view."Статус_ВБ" != 'waiting'), а остальные имеют статус waiting
        :return: список ID висячих поставок
        """
        query = """
        WITH get_hanging_supply AS (
            SELECT id, supply_id, created_at
            FROM hanging_supplies hs
            WHERE hs.is_fictitious_delivered = false
        ),
        get_statuses AS (
            SELECT wild, 
                   "Номер_СЗ" AS order_id, 
                   "Наш_статус" AS our_status, 
                   "Статус_поставщика" AS supply_status, 
                   "Статус_ВБ" AS wb_status, 
                   "Номер_поставки" AS supply_id
            FROM status_model_view smv 
            WHERE smv."Наш_статус" = 'IN_HANGING_SUPPLY' 
              AND smv."Статус_поставщика" = 'complete'
        ),
        supply_with_waiting AS (
            SELECT DISTINCT supply_id
            FROM get_statuses
            WHERE wb_status = 'waiting'
        )
        SELECT distinct ghs.supply_id
        FROM get_hanging_supply ghs
        JOIN get_statuses gs ON ghs.supply_id = gs.supply_id
        WHERE ghs.supply_id IN (SELECT supply_id FROM supply_with_waiting)
        """

        result = await self.db.fetch(query)

        return [BaseHangingSuppliesData(
            supply_id=record.get('supply_id')
        ) for record in result]