import asyncio
import json
import urllib.parse
from src.logger import app_logger as logger
from src.users.account import Account
from src.response import parse_json


class Orders(Account):

    def __init__(self, account, token):
        super().__init__(account, token)
        self.url: str = "https://marketplace-api.wildberries.ru/api/v3/orders"

    async def get_status_orders(self, supply_id, orders_ids: list[int]):
        orders = await self.get_orders_statuses(orders_ids)
        return {self.account: {supply_id: [order for order in orders.get("orders", [])]}}

    async def get_orders_statuses(self, order_ids: list[int]):
        """
        Получает статусы заказов. Обрабатывает списки любой длины, 
        разбивая их на пакеты по 1000 элементов согласно ограничениям API.
        
        Args:
            order_ids: Список идентификаторов заказов
        
        Returns:
            dict: Ответ от API с статусами заказов
        """

        if not order_ids:
            return {"orders": []}

        if len(order_ids) <= 1000:
            response = await self.async_client.post(
                f"{self.url}/status", 
                headers=self.headers, 
                json={"orders": order_ids}
            )
            return parse_json(response)

        batches = [order_ids[i:i+1000] for i in range(0, len(order_ids), 1000)]
        logger.info(f"Разбиваем запрос статусов на {len(batches)} пакетов по <= 1000 элементов для аккаунта {self.account}")

        tasks = []
        tasks.extend(
            self.async_client.post(
                f"{self.url}/status", headers=self.headers, json={"orders": batch})
            for batch in batches)
        responses = await asyncio.gather(*tasks)

        all_orders = []
        for response in responses:
            result = parse_json(response)
            all_orders.extend(result.get("orders", []))

        return {"orders": all_orders}

    async def get_stickers_to_orders(self, supply, order_ids: list[int]):
        url_with_params = f"{self.url}/stickers?type=png&width=58&height=40"
        batches = [order_ids[i:i + 99] for i in range(0, len(order_ids), 99)]

        sticker_batches = await asyncio.gather(
            *[self.async_client.post(url_with_params, headers=self.headers, data=json.dumps({"orders": batch}))
              for batch in batches])

        result = {}
        for response in sticker_batches:
            if not response:
                continue

            response_json = parse_json(response)

            if not result:
                result = {self.account: {supply: response_json}}
            else:
                result[self.account][supply]['stickers'].extend(response_json['stickers'])

        return result


    async def get_new_orders(self):
        orders = []
        next_value = 0
        while True:
            params = {"limit": 1000, "next": next_value}
            response = await self.async_client.get(f"{self.url}/new", params=params, headers=self.headers)
            data = parse_json(response)
            orders.extend(data.get("orders", []))
            next_value = data.get("next")
            logger.info(f"Получены {len(orders)} поставок and next {next_value}, account {self.account}")
            if not next_value:
                break

        return orders

    async def get_orders(self):
        orders = []
        next_value = 0
        while True:
            params = {"limit": 1000, "next": next_value}
            response = await self.async_client.get(f"{self.url}", params=params, headers=self.headers)
            data = parse_json(response)
            orders.extend(data.get("orders", []))
            next_value = data.get("next")
            logger.info(f"Получены {len(orders)} поставок and next {next_value}, account {self.account}")
            if not next_value:
                break

        return orders