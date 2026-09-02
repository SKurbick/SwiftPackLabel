import os

from loguru import logger as _loguru

STICKER_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logging", "stickers")
os.makedirs(STICKER_LOG_DIR, exist_ok=True)
STICKER_LOG_FILE = os.path.join(STICKER_LOG_DIR, "stickers.txt")

_MARK = "sticker_issue"

_loguru.add(
    STICKER_LOG_FILE,
    format="{time:YYYY-MM-DD HH:mm:ss} | {message}",
    rotation="10 MB",
    compression="zip",
    level="INFO",
    enqueue=True,
    filter=lambda record: record["extra"].get(_MARK) is True,
)


def log_stickers_issued(supply_id: str, account: str, orders, operator: str = None) -> None:
    """список выданных стикеров в лог с проблемами и без"""
    try:
        records = list(orders)
        marked = _loguru.bind(**{_MARK: True})
        ctx = f"supply_id={supply_id} account={account} operator={operator or '-'}"
        given = 0
        for order_id, wild, issued in records:
            if issued:
                given += 1
            state = "стикер получен" if issued else "стикер не получен"
            marked.info(f"{state:<17} | order_id={order_id} wild={wild or '-'} {ctx}")
        total = len(records)
        marked.info(
            f"итого | {ctx} "
            f"запрошено={total} выдано={given} не_выдано={total - given}"
        )
    except Exception as e:
        pass
