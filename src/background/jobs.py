from src.available_quantity.repository import AvailableQuantityRepository
from src.available_quantity.service import AvailableQuantityService
from src.background.database import background_db as db
from src.background.hanging_supplies import HangingSuppliesService
from src.background.scheduler import Job
from src.diagnostics import safe_error
from src.logger import app_logger as logger
from src.models.hanging_supplies import HangingSupplies
from src.models.orders_wb import OrdersDB
from src.settings import settings
from src.utils import get_wb_tokens
from src.wildberries_api.orders import Orders


async def sync_orders():
    """Обновляет заказы WB в БД пакетами по 1000 записей."""
    failed = []
    for account, tokens in get_wb_tokens().items():
        try:
            orders = await Orders(account, tokens).get_orders()
            for start in range(0, len(orders), 1000):
                async with db.connection() as connection:
                    await OrdersDB._update_orders_with_connection(connection, orders[start:start + 1000])
        except Exception as exc:
            failed.append(account)
            logger.error(f"Не удалось обновить заказы кабинета {account}: {safe_error(exc)}")
    if failed:
        raise RuntimeError(f"Не обновлены заказы {len(failed)} кабинетов")


async def sync_hanging_supplies(supplies_data):
    """Сверяет висячие поставки с уже полученными данными WB."""
    return await HangingSuppliesService(db).sync_hanging_supplies_with_current_data(supplies_data)


async def cleanup_changes_log():
    """Оставляет историю изменений за последние 30 дней."""
    result = await HangingSupplies(db).cleanup_old_changes_log(30)
    if "error" in result:
        raise RuntimeError("Не удалось очистить историю висячих поставок")


async def update_available_quantity():
    """Сохраняет свободные остатки по прежнему расчёту."""
    await AvailableQuantityService(AvailableQuantityRepository(db)).update_available_quantity()


async def auto_conversion_hanging_supplies():
    """Переводит просроченные висячие поставки в фиктивную доставку."""
    await HangingSuppliesService(db)._sync_conversion_supply_into_fictitious_shipment()


def periodic_jobs():
    """Сохраняет расписание, которое раньше выполнял Celery beat."""
    return (
        Job("sync_orders", sync_orders, timeout=3900, interval=3900),
        Job("cleanup_changes_log", cleanup_changes_log, interval=604800),
        Job("available_quantity", update_available_quantity, daily_at=(20, 59),
            timezone=settings.BACKGROUND_JOBS_TIMEZONE),
        Job("auto_conversion_hanging_supplies", auto_conversion_hanging_supplies, interval=216000),
    )
