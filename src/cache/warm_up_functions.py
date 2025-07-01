"""
Функции для прогрева глобального кэша.
"""

async def warm_up_supplies():
    """Функция для прогрева кэша поставок."""
    try:
        from src.supplies.supplies import SuppliesService
        supplies_service = SuppliesService()
        return await supplies_service.get_list_supplies()
    except Exception as e:
        raise Exception(f"Failed to warm up supplies cache: {e}")


async def warm_up_orders():
    """Функция для прогрева кэша заказов."""
    try:
        from src.db import get_db_connection
        from src.orders.orders import OrdersService
        from src.orders.schema import OrderDetail
        
        # Получаем DB connection
        async for db in get_db_connection():
            orders_service = OrdersService(db)
            
            # Параметры по умолчанию
            time_delta = 1.0
            wild = None
            
            # Получаем и группируем заказы
            filtered_orders = await orders_service.get_filtered_orders(time_delta=time_delta, article=wild)
            order_details = [OrderDetail(**order) for order in filtered_orders]
            grouped_orders = await orders_service.group_orders_by_wild(order_details)
            
            return grouped_orders
            
    except Exception as e:
        raise Exception(f"Failed to warm up orders cache: {e}")