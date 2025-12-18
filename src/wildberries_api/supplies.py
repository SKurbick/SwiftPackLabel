from src.response import parse_json
from src.users.account import Account
from src.logger import app_logger as logger
from src.wildberries_api.orders import Orders


class Supplies(Account):

    def __init__(self, account, token):
        super().__init__(account, token)
        self.url: str = "https://marketplace-api.wildberries.ru/api/v3/supplies"

    async def get_supplies_filter_done(self):
        supplies = await self.get_supplies()
        return {self.account: [sup for sup in supplies if not sup.get('done')]}

    async def get_supplies(self):
        supplies = []
        next_value = 0
        while True:
            params = {"limit": 1000, "next": next_value}
            response = await self.async_client.get(self.url, params=params, headers=self.headers)
            data = parse_json(response)
            supplies.extend(data.get("supplies", []))
            next_value = data.get("next")
            logger.info(f"Получены {len(supplies)} поставок and next {next_value}, account {self.account}")
            if not next_value:
                break

        return supplies

    async def get_supply_order_ids(self, supply_id: str) -> list[int]:
        url = f"https://marketplace-api.wildberries.ru/api/marketplace/v3/supplies/{supply_id}/order-ids"
        response = await self.async_client.get(url, headers=self.headers)

        if response is None:
            logger.warning(f"Не удалось получить order-ids для поставки {supply_id}, account {self.account}")
            return []

        response_json = parse_json(response)
        order_ids = response_json.get('orderIds', response_json.get('orders', []))
        logger.info(f"Получены {len(order_ids)} order-ids для поставки {supply_id}, account {self.account}")
        return order_ids

    async def get_supply_orders(self, supply_id: str):
        order_ids = await self.get_supply_order_ids(supply_id)

        if not order_ids:
            logger.info(f"Поставка {supply_id} пуста (нет заказов), account {self.account}")
            return {self.account: {supply_id: {"orders": []}}}

        orders_api = Orders(self.account, self.token)
        all_orders = await orders_api.get_orders()

        order_ids_set = set(order_ids)
        filtered_orders = [order for order in all_orders if order.get('id') in order_ids_set]

        logger.info(f"Получены детали для {len(filtered_orders)}/{len(order_ids)} заказов поставки {supply_id}, account {self.account}")

        return {self.account: {supply_id: {"orders": filtered_orders}}}

    async def create_supply(self, name: str) -> dict:
        """
        Создаёт новую поставку в кабинете по наименованию.
        :param name: Наименование поставки
        :return: Ответ от WB API (id поставки или ошибка)
        """
        response = await self.async_client.post(self.url, json={"name": name}, headers=self.headers)
        logger.info(f"Создана поставка с именем '{name}' для аккаунта {self.account}. Ответ: {response}")
        return parse_json(response)

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
        self.async_client.retries = 90
        self.async_client.delay = 61
        url = f"https://marketplace-api.wildberries.ru/api/marketplace/v3/supplies/{supply_id}/orders"
        response = await self.async_client.patch(url, json={"orders": [order_id]}, headers=self.headers)
        logger.info(f"Добавлен заказ {order_id} в поставку {supply_id} для аккаунта {self.account}. Ответ: {response}")
        return response

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
        logger.info(
            f"Перевод поставки {supply_id} в статус доставки для аккаунта {self.account}. Код ответа: {response}")
        return response

    async def get_information_to_supply(self, supply_id):
        response = await self.async_client.get(f"{self.url}/{supply_id}", headers=self.headers)
        logger.info(f"Получение информации о поставке {supply_id} : account {self.account}")
        return parse_json(response)

    async def get_sticker_by_supply_ids(self,supply_id):
        response = await self.async_client.get(f"{self.url}/{supply_id}/barcode?type=png", headers=self.headers)
        logger.info(f"Получение информации о поставке {supply_id} : account {self.account}")
        return parse_json(response)

    async def get_supply_orders_batch(self, supply_ids: list[str]) -> dict:
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

        if all_order_ids:
            orders_api = Orders(self.account, self.token)
            all_orders = await orders_api.get_orders()
            logger.info(f"Получено {len(all_orders)} заказов из API, account {self.account}")
        else:
            all_orders = []

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