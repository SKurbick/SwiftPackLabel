import asyncio
from typing import List, Set

from src.response import ExternalApiError, ensure_response, parse_json
from src.users.account import Account
from src.logger import app_logger as logger
from src.wildberries_api.orders import Orders
from src.orders.constants_to_block import FBS2_SUPPLY_NAME_PREFIXES

WB_SUPPLY_ORDERS_BATCH_SIZE = 100


class Supplies(Account):

    def __init__(self, account, token):
        super().__init__(account, token)
        self.url: str = "https://marketplace-api.wildberries.ru/api/v3/supplies"

    async def get_supplies_filter_done(self):
        supplies = await self.get_supplies()
        return {self.account: [sup for sup in supplies if not sup.get('done')]}

    async def get_supplies(self):
        """Постранично забирает поставки откидываю для фбс2."""
        supplies = []
        next_value = 0
        seen_cursors = set()
        while True:
            params = {"limit": 1000, "next": next_value}
            response = await self.async_client.get(self.url, params=params, headers=self.headers)
            data = parse_json(ensure_response(response, f"Список поставок кабинета {self.account}"))

            for sup in data.get("supplies") or []:
                sup_name: str = sup.get("name") or ""
                if not sup_name.startswith(FBS2_SUPPLY_NAME_PREFIXES):
                    supplies.append(sup)

            next_value = data.get("next")
            logger.info(f"Получены {len(supplies)} поставок and next {next_value}, account {self.account}")
            if not next_value:
                break
            if next_value in seen_cursors:
                logger.error(
                    f"WB повторил курсор {next_value} для кабинета {self.account}, "
                    f"останавливаем пагинацию на {len(supplies)} поставках"
                )
                break
            seen_cursors.add(next_value)

        return supplies

    async def get_supply_order_ids(self, supply_id: str) -> list[int]:
        url = f"https://marketplace-api.wildberries.ru/api/marketplace/v3/supplies/{supply_id}/order-ids"
        response = await self.async_client.get(url, headers=self.headers)
        response_json = parse_json(ensure_response(response, f"Состав поставки {supply_id} ({self.account})"))
        order_ids = response_json.get('orderIds', response_json.get('orders', []))
        logger.info(f"Получены {len(order_ids)} order-ids для поставки {supply_id}, account {self.account}")
        return order_ids

    def _log_resolved_orders(self, supply_id: str, resolved: int, expected: int, source: str) -> None:
        """Сообщает, сколько заказов поставки удалось раскрыть в детали."""
        message = (f"Получены детали для {resolved}/{expected} заказов поставки {supply_id} "
                   f"{source}, account {self.account}")
        if resolved < expected:
            logger.warning(
                f"{message}. неполный: {expected - resolved} заказов не раскрыты "
                f"(нет ни в бд ни в wb api) — в интерфейсе поставка покажет {resolved}"
            )
        else:
            logger.info(message)

    async def get_supply_orders(self, supply_id: str, db=None):
        """
        Получает заказы поставки с деталями.

        Оптимизированный подход:
        1. Получаем order_ids из WB API (быстро)
        2. Если db передан - получаем детали из БД (быстро)
        3. Fallback на WB API для отсутствующих в БД
        """
        order_ids = await self.get_supply_order_ids(supply_id)

        if not order_ids:
            logger.info(f"Поставка {supply_id} пуста (нет заказов), account {self.account}")
            return {self.account: {supply_id: {"orders": []}}}

        # Оптимизация: используем БД если доступна
        if db:
            from src.models.assembly_task_status import AssemblyTaskStatus
            from src.db import db as db_pool
            # Используем пул соединений для параллельных запросов
            async with db_pool.connection() as conn:
                assembly_task = AssemblyTaskStatus(conn)
                orders_from_db = await assembly_task.get_orders_details_by_ids(order_ids)

            # Проверяем что все заказы найдены
            missing_ids = set(order_ids) - set(orders_from_db.keys())

            if not missing_ids:
                # Все заказы найдены в БД
                orders_list = list(orders_from_db.values())
                self._log_resolved_orders(supply_id, len(orders_list), len(order_ids), "из БД")
                return {self.account: {supply_id: {"orders": orders_list}}}

            # Есть недостающие - получаем их из WB API
            logger.info(f"Поставка {supply_id}: {len(missing_ids)} заказов не найдено в БД, получаем из WB API")
            orders_api = Orders(self.account, self.token)

            # Пробуем сначала new_orders (быстрее)
            new_orders = await orders_api.get_new_orders()
            for order in new_orders:
                oid = order.get('id')
                if oid in missing_ids:
                    orders_from_db[oid] = {
                        "id": oid,
                        "article": order.get("article", ""),
                        "nmId": order.get("nmId"),
                        "createdAt": order.get("createdAt", ""),
                        "convertedPrice": order.get("convertedPrice", 0),
                    }
                    missing_ids.discard(oid)

            # Если всё ещё есть недостающие - полный запрос
            if missing_ids:
                all_orders = await orders_api.get_orders()
                for order in all_orders:
                    oid = order.get('id')
                    if oid in missing_ids:
                        orders_from_db[oid] = {
                            "id": oid,
                            "article": order.get("article", ""),
                            "nmId": order.get("nmId"),
                            "createdAt": order.get("createdAt", ""),
                            "convertedPrice": order.get("convertedPrice", 0),
                        }

            orders_list = list(orders_from_db.values())
            self._log_resolved_orders(supply_id, len(orders_list), len(order_ids), "(гибрид БД+API)")
            return {self.account: {supply_id: {"orders": orders_list}}}

        # Fallback: старый метод через полный get_orders()
        orders_api = Orders(self.account, self.token)
        all_orders = await orders_api.get_orders()

        order_ids_set = set(order_ids)
        filtered_orders = [order for order in all_orders if order.get('id') in order_ids_set]

        self._log_resolved_orders(supply_id, len(filtered_orders), len(order_ids), "(только WB API)")

        return {self.account: {supply_id: {"orders": filtered_orders}}}

    async def create_supply(self, name: str) -> dict:
        """
        Создаёт новую поставку в кабинете по наименованию.
        :param name: Наименование поставки
        :return: Ответ от WB API (id поставки или ошибка)
        """
        response = await self.async_client.post(self.url, json={"name": name}, headers=self.headers)
        logger.info(f"Создана поставка с именем '{name}' для аккаунта {self.account}. Ответ: {response}")
        return parse_json(ensure_response(response, f"Создание поставки '{name}' ({self.account})"))

    async def add_order_to_supply(self, supply_id: str, order_id: int, check_status: bool = True) -> dict:
        """
        Добавляет сборочное задание (orderId) к поставке (supplyId) через PATCH-запрос к WB API.
        :param supply_id: ID поставки (например, WB-GI-1234567)
        :param order_id: ID сборочного задания (orderId)
        :param check_status: Проверять статус перед добавлением (по умолчанию True)
        :return: Ответ от WB API или ошибка
        """
        # Проверяем статус заказа если требуется
        if check_status:
            orders_api = Orders(self.account, self.token)
            
            can_add = await orders_api.can_add_to_supply(order_id)
            if not can_add:
                error_msg = f"Заказ {order_id} нельзя добавить в поставку - проверьте статус"
                logger.warning(error_msg)
                return {"error": error_msg, "success": False}
        
        # Добавляем заказ в поставку
        url = f"https://marketplace-api.wildberries.ru/api/marketplace/v3/supplies/{supply_id}/orders"
        response = await self.async_client.patch(url, json={"orders": [order_id]}, headers=self.headers)
        ensure_response(response, f"Добавление заказа {order_id} в поставку {supply_id} ({self.account})")
        logger.info(f"Добавлен заказ {order_id} в поставку {supply_id} для аккаунта {self.account}. Ответ: {response}")
        return response

    async def add_orders_to_supply(self, supply_id: str, order_ids: List[int]) -> Set[int]:
        """Добавляет сборочные задания в поставку батчамии."""
        if not order_ids:
            return set()

        batches = [order_ids[i:i + WB_SUPPLY_ORDERS_BATCH_SIZE]
                   for i in range(0, len(order_ids), WB_SUPPLY_ORDERS_BATCH_SIZE)]
        logger.info(
            f"Добавление {len(order_ids)} заданий в поставку {supply_id} "
            f"для аккаунта {self.account}: запросов {len(batches)}"
        )

        results = await asyncio.gather(*(self._add_orders_batch(supply_id, batch) for batch in batches))

        added: Set[int] = set()
        for result in results:
            added.update(result)

        if len(added) < len(order_ids):
            logger.error(
                f"Поставка {supply_id} ({self.account}): принято {len(added)} из {len(order_ids)} заданий"
            )
        return added

    async def _add_orders_batch(self, supply_id: str, order_ids: List[int]) -> Set[int]:
        """
        Отправляет один пакет заданий.
        """
        url = f"https://marketplace-api.wildberries.ru/api/marketplace/v3/supplies/{supply_id}/orders"
        try:
            response = await self.async_client.patch(url, json={"orders": order_ids}, headers=self.headers)
            ensure_response(
                response,
                f"Добавление {len(order_ids)} заданий в поставку {supply_id} ({self.account})"
            )
        except ExternalApiError as e:
            if len(order_ids) == 1:
                logger.error(
                    f"Задание {order_ids[0]} не добавлено в поставку {supply_id} ({self.account}): {e}"
                )
                return set()

            half = len(order_ids) // 2
            logger.warning(
                f"Пакет из {len(order_ids)} заданий не принят поставкой {supply_id} "
                f"({self.account}): {e}. Делим пополам и пробуем снова"
            )
            retried = await asyncio.gather(
                self._add_orders_batch(supply_id, order_ids[:half]),
                self._add_orders_batch(supply_id, order_ids[half:]),
            )
            return set().union(*retried)

        logger.info(
            f"Добавлено {len(order_ids)} заданий в поставку {supply_id} для аккаунта {self.account}"
        )
        return set(order_ids)

    async def delete_supply(self, supply_id: str) -> dict:
        """
        Удаляет поставку, если она активна и за ней не закреплено ни одно сборочное задание.
        :param supply_id: ID поставки (например, WB-GI-1234567)
        :return: Ответ от WB API
        Метод удаляет поставку через DELETE запрос к WB API.
        Поставка может быть удалена только если она активна и за ней не закреплено ни одно сборочное задание.
        """
        response = await self.async_client.delete(f"{self.url}/{supply_id}", headers=self.headers)
        logger.info(f"Удаление поставки {supply_id} для аккаунта {self.account}. Ответ: {response}")
        return response

    async def deliver_supply(self, supply_id: str):
        """
        Переводит поставку в статус доставки.
        Метод закрывает поставку и переводит все сборочные задания в ней в статус complete (в доставке).
        Поставка может быть передана в доставку, только если в ней:
        - есть хотя бы одно сборочное задание
        - отсутствуют пустые короба

        :param supply_id: ID поставки (например, WB-GI-1234567)
        :return: Ответ от WB API
        """
        response = await self.async_client.patch(f"{self.url}/{supply_id}/deliver", headers=self.headers)
        ensure_response(response, f"Перевод поставки {supply_id} в доставку ({self.account})")
        logger.info(
            f"Перевод поставки {supply_id} в статус доставки для аккаунта {self.account}. Код ответа: {response}")
        return response

    async def get_information_to_supply(self, supply_id):
        """Возвращает карточку поставки.

        Raises:
            ExternalApiError: Если запрос не удался. Пустой ответ вызывающий код
                трактует как «поставки нет в этом кабинете» — сбой сети под такой
                вывод маскировать нельзя.
        """
        response = await self.async_client.get(f"{self.url}/{supply_id}", headers=self.headers)
        logger.info(f"Получение информации о поставке {supply_id} : account {self.account}")
        return parse_json(ensure_response(response, f"Информация о поставке {supply_id} ({self.account})"))

    async def get_sticker_by_supply_ids(self, supply_id):
        """Возвращает штрихкод поставки в PNG.

        Raises:
            ExternalApiError: Если запрос не удался — иначе стикер молча пропадёт
                из общего листа печати.
        """
        response = await self.async_client.get(f"{self.url}/{supply_id}/barcode?type=png", headers=self.headers)
        logger.info(f"Получение информации о поставке {supply_id} : account {self.account}")
        return parse_json(ensure_response(response, f"Штрихкод поставки {supply_id} ({self.account})"))

    async def get_supply_orders_batch(self, supply_ids: list[str], db=None) -> dict:
        """
        Пакетное получение заказов для нескольких поставок.

        Оптимизированный подход:
        1. Получаем order_ids из WB API для всех поставок (параллельно)
        2. Если db передан - получаем детали из БД одним запросом
        3. Fallback на WB API для отсутствующих в БД
        """
        import asyncio

        if not supply_ids:
            return {self.account: {}}

        logger.info(f"Пакетное получение заказов для {len(supply_ids)} поставок, account {self.account}")

        tasks = [self.get_supply_order_ids(supply_id) for supply_id in supply_ids]
        order_ids_results = await asyncio.gather(*tasks)

        supply_order_ids_map = {}
        all_order_ids = set()
        for supply_id, order_ids in zip(supply_ids, order_ids_results):
            supply_order_ids_map[supply_id] = set(order_ids)
            all_order_ids.update(order_ids)

        logger.info(f"Получены order-ids для {len(supply_ids)} поставок, всего уникальных заказов: {len(all_order_ids)}, account {self.account}")

        if not all_order_ids:
            result = {self.account: {}}
            for supply_id in supply_ids:
                result[self.account][supply_id] = {"orders": []}
            return result

        # Оптимизация: используем БД если доступна
        orders_by_id = {}
        if db:
            from src.models.assembly_task_status import AssemblyTaskStatus
            from src.db import db as db_pool
            # Используем пул соединений для избежания конфликтов при параллельных запросах
            async with db_pool.connection() as conn:
                assembly_task = AssemblyTaskStatus(conn)
                orders_from_db = await assembly_task.get_orders_details_by_ids(list(all_order_ids))
            orders_by_id = orders_from_db

            missing_ids = all_order_ids - set(orders_by_id.keys())
            logger.info(f"Получено {len(orders_by_id)} заказов из БД, отсутствует {len(missing_ids)}, account {self.account}")

            if missing_ids:
                # Fallback для недостающих
                orders_api = Orders(self.account, self.token)

                # Сначала new_orders
                new_orders = await orders_api.get_new_orders()
                for order in new_orders:
                    oid = order.get('id')
                    if oid in missing_ids:
                        orders_by_id[oid] = {
                            "id": oid,
                            "article": order.get("article", ""),
                            "nmId": order.get("nmId"),
                            "createdAt": order.get("createdAt", ""),
                            "convertedPrice": order.get("convertedPrice", 0),
                        }
                        missing_ids.discard(oid)

                # Если всё ещё есть недостающие
                if missing_ids:
                    all_orders = await orders_api.get_orders()
                    for order in all_orders:
                        oid = order.get('id')
                        if oid in missing_ids:
                            orders_by_id[oid] = {
                                "id": oid,
                                "article": order.get("article", ""),
                                "nmId": order.get("nmId"),
                                "createdAt": order.get("createdAt", ""),
                                "convertedPrice": order.get("convertedPrice", 0),
                            }
        else:
            # Fallback: старый метод
            orders_api = Orders(self.account, self.token)
            all_orders = await orders_api.get_orders()
            logger.info(f"Получено {len(all_orders)} заказов из API, account {self.account}")
            orders_by_id = {order.get('id'): order for order in all_orders}

        result = {self.account: {}}
        for supply_id in supply_ids:
            order_ids_for_supply = supply_order_ids_map.get(supply_id, set())
            orders_for_supply = [
                orders_by_id[oid] for oid in order_ids_for_supply
                if oid in orders_by_id
            ]
            result[self.account][supply_id] = {"orders": orders_for_supply}

        logger.info(f"Пакетная обработка завершена для {len(supply_ids)} поставок, account {self.account}")
        return result