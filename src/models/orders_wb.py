"""
Модель для работы с заказами Wildberries
"""
import json
from datetime import datetime
from typing import List, Dict, Any
from src.db import db
from src.logger import get_logger

logger = get_logger()


class OrdersDB:
    """Класс для работы с заказами в базе данных"""
    
    @staticmethod
    async def add_orders(orders: List[Dict[str, Any]]) -> int:
        """
        Добавляет новые заказы в базу данных.
        
        Args:
            orders: Список словарей с данными заказов
            
        Returns:
            Количество добавленных заказов
        """
        if not orders:
            return 0

        current_time = datetime.utcnow()

        try:
            async with db.connection() as conn:
                async with conn.transaction():
                    # Подготавливаем все данные для bulk insert
                    orders_data = [
                        OrdersDB._prepare_order_data(order, current_time) 
                        for order in orders
                    ]

                    # Bulk insert всех заказов одним запросом
                    query = """
                        INSERT INTO orders_wb (
                            id, order_uid, rid, article, nm_id, chrt_id, color_code, skus,
                            price, sale_price, converted_price, currency_code, converted_currency_code, scan_price,
                            delivery_type, cargo_type, warehouse_id, supply_id, offices, address,
                            comment, is_zero_order, options, required_meta, user_id,
                            created_at, updated_at, processed_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8,
                            $9, $10, $11, $12, $13, $14,
                            $15, $16, $17, $18, $19, $20,
                            $21, $22, $23, $24, $25,
                            $26, $27, $28
                        )
                        ON CONFLICT (order_uid) DO NOTHING
                    """
                    
                    await conn.executemany(query, orders_data)
                    processed_count = len(orders)

            logger.info(f"Добавлено заказов: {processed_count}")
            return processed_count

        except Exception as e:
            logger.error(f"Ошибка при добавлении заказов: {e}")
            raise
    
    @staticmethod
    async def update_orders(orders: List[Dict[str, Any]]) -> int:
        """
        Обновляет существующие заказы в базе данных.
        
        Args:
            orders: Список словарей с данными заказов
            
        Returns:
            Количество обновленных заказов
        """
        if not orders:
            return 0

        current_time = datetime.utcnow()

        try:
            async with db.connection() as conn:
                async with conn.transaction():
                    # Подготавливаем все данные для bulk update
                    orders_data = [
                        OrdersDB._prepare_order_data(order, current_time) 
                        for order in orders
                    ]

                    # Bulk update всех заказов одним запросом
                    query = """
                        INSERT INTO orders_wb (
                            id, order_uid, rid, article, nm_id, chrt_id, color_code, skus,
                            price, sale_price, converted_price, currency_code, converted_currency_code, scan_price,
                            delivery_type, cargo_type, warehouse_id, supply_id, offices, address,
                            comment, is_zero_order, options, required_meta, user_id,
                            created_at, updated_at, processed_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8,
                            $9, $10, $11, $12, $13, $14,
                            $15, $16, $17, $18, $19, $20,
                            $21, $22, $23, $24, $25,
                            $26, $27, $28
                        )
                        ON CONFLICT (order_uid) DO UPDATE SET
                            price = EXCLUDED.price,
                            sale_price = EXCLUDED.sale_price,
                            converted_price = EXCLUDED.converted_price,
                            scan_price = EXCLUDED.scan_price,
                            delivery_type = EXCLUDED.delivery_type,
                            cargo_type = EXCLUDED.cargo_type,
                            warehouse_id = EXCLUDED.warehouse_id,
                            supply_id = EXCLUDED.supply_id,
                            offices = EXCLUDED.offices,
                            address = EXCLUDED.address,
                            comment = EXCLUDED.comment,
                            is_zero_order = EXCLUDED.is_zero_order,
                            options = EXCLUDED.options,
                            required_meta = EXCLUDED.required_meta,
                            user_id = EXCLUDED.user_id,
                            updated_at = EXCLUDED.updated_at
                    """
                    
                    await conn.executemany(query, orders_data)
                    processed_count = len(orders)

            logger.info(f"Обновлено заказов: {processed_count}")
            return processed_count

        except Exception as e:
            logger.error(f"Ошибка при обновлении заказов: {e}")
            raise
    
    @staticmethod
    def _prepare_order_data(order: Dict[str, Any], current_time: datetime) -> tuple:
        """
        Подготавливает данные заказа для вставки в БД.
        
        Args:
            order: Словарь с данными заказа
            current_time: Текущее время для processed_at
            
        Returns:
            Кортеж с данными для SQL запроса
        """
        # Парсинг даты создания
        created_at = datetime.fromisoformat(order['createdAt'].replace('Z', '+00:00'))
        
        # Подготовка JSON полей
        skus_json = json.dumps(order.get('skus', []))
        offices_json = json.dumps(order.get('offices', []))
        options_json = json.dumps(order.get('options', {}))
        required_meta_json = json.dumps(order.get('requiredMeta', []))
        
        return (
            order['id'],                                    # id
            order['orderUid'],                              # order_uid
            order['rid'],                                   # rid
            order['article'],                               # article
            order['nmId'],                                  # nm_id
            order['chrtId'],                                # chrt_id
            order.get('colorCode', ''),                     # color_code
            skus_json,                                      # skus
            order['price'],                                 # price
            order.get('salePrice'),                         # sale_price
            order['convertedPrice'],                        # converted_price
            order['currencyCode'],                          # currency_code
            order['convertedCurrencyCode'],                 # converted_currency_code
            order.get('scanPrice'),                         # scan_price
            order['deliveryType'],                          # delivery_type
            order['cargoType'],                             # cargo_type
            order['warehouseId'],                           # warehouse_id
            order.get('supplyId'),                          # supply_id
            offices_json,                                   # offices
            order.get('address'),                           # address
            order.get('comment', ''),                       # comment
            order.get('isZeroOrder', False),                # is_zero_order
            options_json,                                   # options
            required_meta_json,                             # required_meta
            order.get('userId'),                            # user_id
            created_at,                                     # created_at
            current_time,                                   # updated_at
            current_time                                    # processed_at
        )