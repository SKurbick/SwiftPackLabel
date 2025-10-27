from typing import List, Dict, Any, Tuple
import json
from datetime import datetime
from src.logger import app_logger as logger


class DeliveredSupplies:
    """
    Постоянное хранилище доставленных поставок.

    Используется ТОЛЬКО для поставок в статусе доставки (is_delivery=True).
    Данные неизменны после доставки и хранятся постоянно.

    Структура данных:
    - supply_id: ID поставки WB
    - account: Аккаунт WB
    - supply_name: Название поставки
    - created_at_supply: Дата создания поставки
    - supply_data: Полная структура поставки (name, createdAt, supply_id, account, count, orders)
    """

    def __init__(self, db):
        self.db = db

    async def get_supplies_from_storage(
        self,
        supply_ids: List[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """
        Получает полные данные поставок из БД хранилища.

        Args:
            supply_ids: Список кортежей (supply_id, account)

        Returns:
            Dict: {(supply_id, account): {полная структура поставки}}

        Example:
            >>> storage = DeliveredSupplies(db)
            >>> supplies = await storage.get_supplies_from_storage([
            ...     ("WB-GI-190533692", "Вектор"),
            ...     ("WB-GI-190343559", "Оганесян")
            ... ])
            >>> print(supplies[("WB-GI-190533692", "Вектор")])
            {
                "name": "3 круг 26.10._TEX",
                "createdAt": "26.10.2025",
                "supply_id": "WB-GI-190533692",
                "account": "Вектор",
                "count": 3,
                "orders": [...]
            }
        """
        if not supply_ids:
            return {}

        query = """
        SELECT supply_id, account, supply_data
        FROM public.delivered_supplies
        WHERE (supply_id, account) IN (
            SELECT * FROM unnest($1::text[], $2::text[])
        )
        """

        supply_ids_list = [sid for sid, _ in supply_ids]
        accounts_list = [acc for _, acc in supply_ids]

        result = await self.db.fetch(query, supply_ids_list, accounts_list)

        stored_data = {}
        for row in result:
            key = (row['supply_id'], row['account'])
            # supply_data - это JSONB, десериализуем если нужно
            supply_data = row['supply_data']
            if isinstance(supply_data, str):
                supply_data = json.loads(supply_data)
            stored_data[key] = supply_data

        logger.info(
            f"Получено {len(stored_data)} доставленных поставок из БД хранилища "
            f"(запрошено {len(supply_ids)})"
        )
        return stored_data

    async def save_supplies_to_storage(
        self,
        supplies_data: List[Dict[str, Any]]
    ) -> int:
        """
        Сохраняет доставленные поставки в БД хранилище.

        Args:
            supplies_data: Список словарей с полной структурой поставок:
                {
                    'name': str,
                    'createdAt': str (DD.MM.YYYY),
                    'supply_id': str,
                    'account': str,
                    'count': int,
                    'orders': [...],
                    'shipped_count': int | None,
                    'is_fictitious_delivered': bool | None
                }

        Returns:
            int: Количество сохраненных записей

        Example:
            >>> supplies = [
            ...     {
            ...         "name": "3 круг 26.10._TEX",
            ...         "createdAt": "26.10.2025",
            ...         "supply_id": "WB-GI-190533692",
            ...         "account": "Вектор",
            ...         "count": 3,
            ...         "orders": [...]
            ...     }
            ... ]
            >>> saved = await storage.save_supplies_to_storage(supplies)
        """
        if not supplies_data:
            return 0

        query = """
        INSERT INTO public.delivered_supplies
            (supply_id, account, supply_name, created_at_supply, supply_data)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (supply_id, account)
        DO NOTHING
        """

        saved_count = 0
        for supply in supplies_data:
            try:
                # Извлекаем данные до конвертации
                supply_id = supply.get('supply_id') if isinstance(supply, dict) else supply.supply_id
                account = supply.get('account') if isinstance(supply, dict) else supply.account
                name = supply.get('name', '') if isinstance(supply, dict) else getattr(supply, 'name', '')
                created_at_str = supply.get('createdAt') if isinstance(supply, dict) else supply.createdAt

                # Парсим дату из формата ISO или DD.MM.YYYY в datetime
                created_at_parsed = self._parse_date(created_at_str)

                # Конвертируем Pydantic модели в словари для JSON
                supply_dict = self._convert_to_dict(supply)

                result = await self.db.execute(
                    query,
                    supply_id,
                    account,
                    name,
                    created_at_parsed,
                    json.dumps(supply_dict)  # Сохраняем всю структуру
                )

                # Проверяем, была ли вставка (не конфликт)
                if result == "INSERT 0 1":
                    saved_count += 1

            except Exception as e:
                logger.error(
                    f"Ошибка сохранения доставленной поставки "
                    f"{supply.get('supply_id') if isinstance(supply, dict) else getattr(supply, 'supply_id', 'UNKNOWN')} "
                    f"({supply.get('account') if isinstance(supply, dict) else getattr(supply, 'account', 'UNKNOWN')}): {str(e)}"
                )

        logger.info(f"Сохранено {saved_count} новых доставленных поставок в БД хранилище")
        return saved_count

    @staticmethod
    def _convert_to_dict(obj: Any) -> Dict[str, Any]:
        """
        Рекурсивно конвертирует объект в словарь, обрабатывая Pydantic модели.

        Args:
            obj: Объект для конвертации

        Returns:
            Dict или примитивный тип
        """
        if hasattr(obj, 'model_dump'):
            # Pydantic v2
            return obj.model_dump()
        elif hasattr(obj, 'dict'):
            # Pydantic v1
            return obj.dict()
        elif isinstance(obj, dict):
            return {k: DeliveredSupplies._convert_to_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [DeliveredSupplies._convert_to_dict(item) for item in obj]
        else:
            return obj

    @staticmethod
    def _parse_date(date_str: str) -> datetime:
        """
        Парсит дату из формата DD.MM.YYYY или ISO в datetime объект для timestamptz.

        Args:
            date_str: Дата в формате "26.10.2025" или "2025-10-26T00:00:00Z"

        Returns:
            datetime: Объект datetime или None
        """
        if not date_str:
            return None

        try:
            # Сначала пробуем ISO формат (2025-10-26T00:00:00Z)
            if 'T' in date_str:
                # Парсим ISO формат
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return dt
            else:
                # Парсим DD.MM.YYYY
                dt = datetime.strptime(date_str, "%d.%m.%Y")
                return dt
        except Exception as e:
            logger.warning(f"Не удалось распарсить дату {date_str}: {e}")
            return None

    async def get_missing_supplies(
        self,
        requested_supplies: List[Tuple[str, str]]
    ) -> List[Tuple[str, str]]:
        """
        Определяет, каких поставок нет в БД хранилище.

        Args:
            requested_supplies: Список (supply_id, account)

        Returns:
            List: Поставки, которых нет в хранилище
        """
        stored = await self.get_supplies_from_storage(requested_supplies)
        stored_keys = set(stored.keys())
        requested_keys = set(requested_supplies)

        missing = list(requested_keys - stored_keys)
        logger.info(
            f"Отсутствует в БД хранилище: {len(missing)} из {len(requested_supplies)}"
        )
        return missing

    async def check_supply_exists(
        self,
        supply_id: str,
        account: str
    ) -> bool:
        """
        Проверяет, существует ли поставка в БД хранилище.

        Args:
            supply_id: ID поставки
            account: Аккаунт

        Returns:
            bool: True если существует
        """
        query = """
        SELECT 1 FROM public.delivered_supplies
        WHERE supply_id = $1 AND account = $2
        LIMIT 1
        """

        result = await self.db.fetchrow(query, supply_id, account)
        return result is not None

    async def get_storage_statistics(self) -> Dict[str, Any]:
        """
        Получает статистику по БД хранилищу.

        Returns:
            Dict: Статистика с метриками
        """
        query = """
        SELECT
            COUNT(*) as total_supplies,
            COUNT(DISTINCT account) as unique_accounts,
            SUM((supply_data->>'count')::int) as total_orders,
            MIN(saved_at) as oldest_entry,
            MAX(saved_at) as newest_entry,
            pg_size_pretty(pg_total_relation_size('public.delivered_supplies')) as table_size
        FROM public.delivered_supplies
        """

        result = await self.db.fetchrow(query)

        stats = {
            'total_supplies': result['total_supplies'],
            'unique_accounts': result['unique_accounts'],
            'total_orders': result['total_orders'],
            'oldest_entry': result['oldest_entry'],
            'newest_entry': result['newest_entry'],
            'table_size': result['table_size']
        }

        logger.info(f"Статистика БД хранилища доставленных поставок: {stats}")
        return stats

    async def cleanup_old_supplies(self, days_to_keep: int = 90) -> int:
        """
        ОПЦИОНАЛЬНО: Очищает очень старые поставки (если нужно экономить место).

        По умолчанию доставленные поставки хранятся постоянно.

        Args:
            days_to_keep: Количество дней для хранения (по умолчанию 90)

        Returns:
            int: Количество удаленных записей
        """
        query = """
        DELETE FROM public.delivered_supplies
        WHERE saved_at < CURRENT_TIMESTAMP - INTERVAL '%s days'
        RETURNING id
        """

        result = await self.db.fetch(query % days_to_keep)
        deleted_count = len(result)

        if deleted_count > 0:
            logger.info(
                f"Очищено {deleted_count} старых доставленных поставок "
                f"(старше {days_to_keep} дней)"
            )

        return deleted_count
