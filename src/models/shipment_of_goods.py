from typing import List, Dict, Any, Optional
from decimal import Decimal
from datetime import datetime

from src.logger import app_logger as logger


class ShipmentOfGoods:
    """Таблица shipment_of_goods
    Содержит информацию об отгрузке товаров

    Columns:
        id (serial4, primary key)
        author (varchar): Автор отгрузки
        supply_id (varchar): ID поставки
        product_id (varchar): ID продукта, внешний ключ к products.id это wild
        warehouse_id (int4): ID склада, внешний ключ к warehouses.id по умолчанию 1
        delivery_type (varchar): Тип доставки по умолчанию ФБС
        wb_warehouse (varchar): Склад Wildberries
        account (varchar): Аккаунт это кабинет WB
        quantity (numeric): Количество товара (больше 0)
        created_at (timestamptz): Дата и время создания записи
    """

    def __init__(self, db):
        self.db = db

    async def create_all(self, items: List[Dict[str, Any]]) -> bool:
        """
        Вставляет все записи одним запросом.
        Args:
            items: Список подготовленных словарей с данными для вставки.
        Returns:
            bool: True если вставка успешна, False в противном случае
        """
        if not items:
            return False

        columns = ["author", "supply_id", "product_id", "warehouse_id",
                   "delivery_type", "wb_warehouse", "account", "quantity"]

        placeholders = []
        params = []

        for item in items:
            item_placeholders = []
            for col in columns:
                params.append(item.get(col))
                item_placeholders.append(f"${len(params)}")

            placeholders.append(f"({', '.join(item_placeholders)})")

        query = f"""
        INSERT INTO public.shipment_of_goods ({', '.join(columns)})
        VALUES {', '.join(placeholders)}
        """

        try:
            await self.db.execute(query, *params)
            return True
        except Exception as e:
            logger.error(f"Ошибка при вставке данных: {str(e)}")
            return False