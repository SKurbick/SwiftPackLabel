from typing import List, Dict, Any, Optional

from src.logger import app_logger as logger


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
        created_at (timestamptz): Время создания записи, по умолчанию CURRENT_TIMESTAMP
    """

    def __init__(self, db):
        self.db = db

    async def save_hanging_supply(self, supply_id: str, account: str, order_data: Dict[str, Any]) -> bool:
        """
        Сохраняет информацию о висячей поставке в БД.
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            order_data: Данные о заказах в поставке в формате JSON
        Returns:
            bool: True, если запись успешно создана/обновлена, иначе False
        """
        try:
            query = """
            INSERT INTO public.hanging_supplies (supply_id, account, order_data)
            VALUES ($1, $2, $3)
            ON CONFLICT (supply_id, account) 
            DO UPDATE SET order_data = $3, created_at = CURRENT_TIMESTAMP
            RETURNING id
            """
            result = await self.db.fetchrow(query, supply_id, account, order_data)
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
            """
            result = await self.db.fetch(query)
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Ошибка при получении висячих поставок: {str(e)}")
            return []
