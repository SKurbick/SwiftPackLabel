from typing import Any, Dict, Iterable, Optional, Set

from src.logger import app_logger as logger
from src.utils import get_wb_tokens
from src.wildberries_api.orders import Orders


SUPPLY_NAME_STAGE_SUFFIXES = ("_ТЕХ", "_TEX", "_ФИНАЛ")

WAREHOUSE_ID_PREFIX = "СК"


async def load_warehouse_names(db, accounts: Optional[Iterable[str]] = None) -> Dict[int, str]:
    """Загружает названия складов из справочника `warehouses_fbs`."""
    if db is None:
        return {}

    try:
        if accounts:
            rows = await db.fetch(
                "SELECT wb_warehouse_id, warehouse_name FROM warehouses_fbs WHERE account = ANY($1)",
                list(accounts),
            )
        else:
            rows = await db.fetch("SELECT wb_warehouse_id, warehouse_name FROM warehouses_fbs")

        return {
            row["wb_warehouse_id"]: _short_warehouse_name(row["warehouse_name"])
            for row in rows
            if row["wb_warehouse_id"] is not None and row["warehouse_name"]
        }
    except Exception as e:
        logger.warning(f"Справочник складов недоступен ({e}), в наименованиях будут номера складов")
        return {}


def _short_warehouse_name(warehouse_name: str) -> str:
    """Оставляет от названия склада только город: «Наш склад: Казань» -> «Казань»."""
    _, separator, tail = warehouse_name.partition(":")
    return (tail if separator else warehouse_name).strip()


async def resolve_order_warehouses(orders_by_account: Dict[str, Iterable[int]], db=None) -> Dict[int, int]:
    """
    Определяет склад каждого заказа.
    """
    wanted: Dict[str, Set[int]] = {
        account: set(order_ids) for account, order_ids in orders_by_account.items() if order_ids
    }
    if not wanted:
        return {}

    warehouses: Dict[int, int] = {}
    all_ids = {order_id for ids in wanted.values() for order_id in ids}

    if db is not None:
        warehouses.update(await _warehouses_from_db(db, all_ids))
        if warehouses:
            logger.info(f"Склад известен из БД для {len(warehouses)} из {len(all_ids)} заказов")

    tokens = get_wb_tokens()
    for account, order_ids in wanted.items():
        missing = order_ids - set(warehouses)
        if not missing:
            continue

        try:
            new_orders = await Orders(account, tokens.get(account)).get_new_orders()
        except Exception as e:
            logger.error(f"Кабинет {account}: не удалось получить склады {len(missing)} заказов ({e})")
            continue

        found = {
            order["id"]: order["warehouseId"]
            for order in new_orders
            if order.get("id") in missing and order.get("warehouseId") is not None
        }
        warehouses.update(found)

        still_missing = missing - set(found)
        if still_missing:
            logger.warning(
                f"Кабинет {account}: склад не определён у {len(still_missing)} заказов, "
                f"в поставку они не попадут: {sorted(still_missing)[:20]}"
            )

    _log_distribution(wanted, warehouses)
    return warehouses


async def _warehouses_from_db(db, order_ids: Set[int]) -> Dict[int, int]:
    """Читает склады заказов из orders_wb."""
    try:
        rows = await db.fetch(
            "SELECT id, warehouse_id FROM orders_wb WHERE id = ANY($1) AND warehouse_id IS NOT NULL",
            list(order_ids),
        )
        return {row["id"]: row["warehouse_id"] for row in rows}
    except Exception as e:
        logger.warning(f"Не удалось прочитать склады заказов из БД ({e}), спрашиваем WB API")
        return {}


def _log_distribution(wanted: Dict[str, Set[int]], warehouses: Dict[int, int]) -> None:
    """Пишет в лог, как заказы разложились по складам — по одной строке на кабинет."""
    for account, order_ids in wanted.items():
        by_warehouse: Dict[int, int] = {}
        for order_id in order_ids:
            warehouse_id = warehouses.get(order_id)
            if warehouse_id is not None:
                by_warehouse[warehouse_id] = by_warehouse.get(warehouse_id, 0) + 1

        if len(by_warehouse) > 1:
            logger.info(
                f"Кабинет {account}: заказы на {len(by_warehouse)} складах {by_warehouse} — "
                f"будет создано столько же поставок"
            )
        elif by_warehouse:
            logger.info(f"Кабинет {account}: все заказы на одном складе {list(by_warehouse)[0]}")


def supply_name_with_warehouse(supply_name: str, warehouse_id: int,
                               warehouse_names: Optional[Dict[int, str]] = None) -> str:
    """
    Добавляет в наименование поставки метку склада.

    Метка ставится перед служебным окончанием (`_ТЕХ`, `_TEX`, `_ФИНАЛ`), а не
    в самый конец: по этому окончанию определяется стадия поставки, и метка
    в хвосте сломала бы перевод в финальную.
    """
    label = (warehouse_names or {}).get(warehouse_id) or f"{WAREHOUSE_ID_PREFIX}{warehouse_id}"
    marker = f"_{label}"

    for suffix in SUPPLY_NAME_STAGE_SUFFIXES:
        if supply_name.endswith(suffix):
            return f"{supply_name[:-len(suffix)]}{marker}{suffix}"
    return f"{supply_name}{marker}"


def group_by_account_and_warehouse(order_ids_by_account: Dict[str, Iterable[int]],
                                   order_warehouses: Dict[int, int]) -> Dict[tuple, list]:
    """
    Группирует заказы по паре «кабинет + склад» — по одной поставке на пару.
    """
    groups: Dict[tuple, list] = {}
    for account, order_ids in order_ids_by_account.items():
        for order_id in order_ids:
            warehouse_id = order_warehouses.get(order_id)
            if warehouse_id is None:
                continue
            groups.setdefault((account, warehouse_id), []).append(order_id)
    return groups


def needs_warehouse_marker(groups: Dict[tuple, Any]) -> Dict[str, bool]:
    """Определяет кабинеты, у которых складов больше одного — только им нужна метка."""
    warehouses_per_account: Dict[str, Set[int]] = {}
    for account, warehouse_id in groups:
        warehouses_per_account.setdefault(account, set()).add(warehouse_id)
    return {account: len(ids) > 1 for account, ids in warehouses_per_account.items()}
