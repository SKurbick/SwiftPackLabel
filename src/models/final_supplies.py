from typing import Optional, Dict, Any
from src.logger import app_logger as logger


class FinalSupplies:
    """
    Работа с таблицей final_supplies для отслеживания финальных поставок.
    
    Простой класс для сохранения и получения информации о финальных поставках.
    
    Columns:
        id (serial, primary key)
        supply_id (varchar): ID поставки
        account (varchar): Аккаунт Wildberries
        supply_name (varchar): Название поставки
        created_at (timestamptz): Время создания записи
    """

    def __init__(self, db):
        self.db = db

    async def get_latest_final_supply(self, account: str) -> Optional[Dict[str, Any]]:
        """
        Получает последнюю финальную поставку для аккаунта.
        
        Args:
            account: Аккаунт Wildberries
            
        Returns:
            Dict с данными поставки или None если не найдена
        """
        try:
            query = """
            SELECT supply_id, account, supply_name, created_at
            FROM public.final_supplies 
            WHERE account = $1
            ORDER BY created_at DESC
            LIMIT 1
            """
            result = await self.db.fetchrow(query, account)
            
            if result:
                supply_data = dict(result)
                logger.info(f"Найдена финальная поставка {supply_data['supply_id']} для аккаунта {account}")
                return supply_data
            else:
                logger.info(f"Нет финальных поставок для аккаунта {account}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка получения финальной поставки для {account}: {str(e)}")
            return None

    async def save_final_supply(self, supply_id: str, account: str, supply_name: str) -> bool:
        """
        Сохраняет информацию о финальной поставке.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт WB  
            supply_name: Название поставки
            
        Returns:
            bool: True если успешно сохранено
        """
        try:
            query = """
            INSERT INTO public.final_supplies (supply_id, account, supply_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (supply_id, account) 
            DO UPDATE SET 
                supply_name = $3
            RETURNING id
            """
            result = await self.db.fetchrow(query, supply_id, account, supply_name)
            success = result is not None
            
            if success:
                logger.info(f"Сохранена финальная поставка {supply_id} ({supply_name}) для аккаунта {account}")
            
            return success
            
        except Exception as e:
            logger.error(f"Ошибка сохранения финальной поставки {supply_id}: {str(e)}")
            return False