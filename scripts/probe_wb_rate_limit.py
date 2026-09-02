"""
Измеряет реальный лимит запросов WB для одного кабинета.

Пока лимит неизвестен, настройки клиента (сколько запросов в минуту допускать,
как долго ждать после 429) подбираются наугад. Скрипт отвечает на два вопроса:

    1. Сколько запросов проходит, прежде чем WB отвечает 429.
    2. Сколько секунд нужно ждать, чтобы лимит разошёлся.

Только чтение: используется GET /api/v3/supplies?limit=1 — самый дешёвый
запрос, ничего не меняющий в кабинете. Клиент проекта намеренно не используется,
чтобы его семафор и паузы не искажали замер.

ВНИМАНИЕ: скрипт расходует лимит того кабинета, который измеряет. Запускать,
когда по этому кабинету не идут круги и не обновляется кэш.

Запуск:
    python scripts/probe_wb_rate_limit.py "Вектор"
    python scripts/probe_wb_rate_limit.py "Вектор" --max-requests 400
"""
import argparse
import asyncio
import sys
import time
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.settings import get_wb_tokens  # noqa: E402

PROBE_URL = "https://marketplace-api.wildberries.ru/api/v3/supplies"
RECOVERY_PROBE_INTERVAL_SEC = 5
RECOVERY_TIMEOUT_SEC = 180


async def _probe(session: aiohttp.ClientSession, headers: dict) -> int:
    """Делает один дешёвый запрос и возвращает код ответа."""
    async with session.get(PROBE_URL, params={"limit": 1}, headers=headers) as response:
        await response.read()
        return response.status


async def find_limit(session: aiohttp.ClientSession, headers: dict, max_requests: int) -> tuple[int, float]:
    """Шлёт запросы подряд, пока не прилетит 429.

    Returns:
        (сколько запросов прошло успешно, за сколько секунд)
    """
    started = time.monotonic()
    for sent in range(1, max_requests + 1):
        status = await _probe(session, headers)
        elapsed = time.monotonic() - started

        if status == 429:
            print(f"  429 на запросе №{sent} через {elapsed:.1f}с")
            return sent - 1, elapsed

        if status >= 400:
            print(f"  Неожиданный код {status} на запросе №{sent} — прерываю замер")
            return sent - 1, elapsed

        if sent % 25 == 0:
            print(f"  прошло {sent} запросов за {elapsed:.1f}с ({sent / max(elapsed, 0.001):.0f}/сек)")

    elapsed = time.monotonic() - started
    print(f"  Лимит не достигнут за {max_requests} запросов ({elapsed:.1f}с)")
    return max_requests, elapsed


async def measure_recovery(session: aiohttp.ClientSession, headers: dict) -> float | None:
    """Ждёт, пока лимит разойдётся, проверяя раз в несколько секунд."""
    started = time.monotonic()
    while time.monotonic() - started < RECOVERY_TIMEOUT_SEC:
        await asyncio.sleep(RECOVERY_PROBE_INTERVAL_SEC)
        waited = time.monotonic() - started
        status = await _probe(session, headers)
        if status != 429:
            return waited
        print(f"  ещё 429 после {waited:.0f}с ожидания")
    return None


async def main(account: str, max_requests: int) -> None:
    tokens = get_wb_tokens()
    if account not in tokens:
        print(f"Кабинет '{account}' не найден. Доступные: {', '.join(sorted(tokens))}")
        raise SystemExit(1)

    headers = {"Authorization": tokens[account], "Content-Type": "application/json"}

    print(f"Кабинет: {account}")
    print(f"Запрос:  GET {PROBE_URL}?limit=1 (только чтение)\n")

    async with aiohttp.ClientSession() as session:
        print("Шаг 1: ищем, на каком запросе включается лимит")
        passed, elapsed = await find_limit(session, headers, max_requests)

        if passed >= max_requests:
            print("\nЛимит не нащупан — повторите с большим --max-requests")
            return

        rate = passed / max(elapsed, 0.001)
        print(f"\n  прошло запросов: {passed}")
        print(f"  за время:        {elapsed:.1f}с")
        print(f"  темп:            {rate:.0f}/сек ({rate * 60:.0f}/мин)")

        print("\nШаг 2: ждём, пока лимит разойдётся")
        recovery = await measure_recovery(session, headers)

    print("\n" + "=" * 60)
    print(f"ИТОГ по кабинету {account}")
    print(f"  запросов до 429:        {passed}")
    print(f"  восстановление:         "
          f"{f'{recovery:.0f}с' if recovery else f'дольше {RECOVERY_TIMEOUT_SEC}с'}")
    if recovery:
        safe_per_min = int(passed / max(recovery, 1) * 60 * 0.8)
        print(f"\n  Безопасный темп (80% от измеренного): ~{safe_per_min} запросов/мин")
        print(f"  Пауза после 429 (HTTP_RATE_LIMIT_BACKOFF_BASE_SEC): {max(int(recovery), 5)}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Замер лимита запросов WB для кабинета")
    parser.add_argument("account", help="Имя кабинета из tokens.json, например: Вектор")
    parser.add_argument("--max-requests", type=int, default=400,
                        help="Потолок числа запросов в замере (по умолчанию 400)")
    args = parser.parse_args()

    asyncio.run(main(args.account, args.max_requests))
