import asyncio
import json
import urllib.parse

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
        response = await self.async_client.post(f"{self.url}/status", headers=self.headers, json={"orders": order_ids})
        return parse_json(response)

    async def get_stickers_to_orders(self, supply, order_ids: list[int]):
        query_params = {'type': 'svg', 'width': 58, 'height': 40}
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

            # Обновляем существующий словарь результатов
            if not result:
                result = {self.account: {supply: response_json}}
            else:
                result[self.account][supply]['stickers'].extend(response_json['stickers'])

        return result
