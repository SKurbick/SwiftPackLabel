import asyncio
import json
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
        response = await self.async_client.post(f"{self.url}/status", headers=self.headers, json={"orders": order_ids})
        return parse_json(response)

    async def get_stickers_to_orders(self, supply, order_ids: list[int]):
        logger.info(f"Начинаем получение стикеров для поставки {supply}, аккаунт {self.account}, заказов: {len(order_ids)}")
        logger.debug(f"ID заказов: {order_ids}")
        
        url_with_params = f"{self.url}/stickers?type=png&width=58&height=40"
        batches = [order_ids[i:i + 99] for i in range(0, len(order_ids), 99)]
        logger.info(f"Разделено на {len(batches)} батчей по 99 заказов")

        sticker_batches = await asyncio.gather(
            *[self.async_client.post(url_with_params, headers=self.headers, data=json.dumps({"orders": batch}))
              for batch in batches])
        
        logger.info(f"Получены ответы от {len(sticker_batches)} батчей")

        result = {}
        for i, response in enumerate(sticker_batches):
            if not response:
                logger.warning(f"Пустой ответ от батча {i+1}")
                continue

            try:
                response_json = parse_json(response)
                logger.debug(f"Батч {i+1}: получено стикеров - {len(response_json.get('stickers', []))}")
            except Exception as e:
                logger.error(f"Ошибка при парсинге ответа батча {i+1}: {str(e)}")
                logger.error(f"Сырой ответ: {response}")
                continue

            if not result:
                result = {self.account: {supply: response_json}}
                logger.debug(f"Инициализирован результат для аккаунта {self.account}, поставки {supply}")
            else:
                result[self.account][supply]['stickers'].extend(response_json['stickers'])
                logger.debug(f"Добавлено {len(response_json['stickers'])} стикеров к существующему результату")

        total_stickers = len(result.get(self.account, {}).get(supply, {}).get('stickers', []))
        logger.info(f"Завершено получение стикеров. Итого стикеров: {total_stickers}")
        return result

    async def get_new_orders(self):
        """Получает новые заказы от WB API."""
        orders = []
        next_value = 0
        while True:
            params = {"limit": 1000, "next": next_value}
            response = await self.async_client.get(f"{self.url}/new", params=params, headers=self.headers)
            data = parse_json(response)
            orders.extend(data.get("orders", []))
            next_value = data.get("next")
            logger.info(f"Получены {len(orders)} новых заказов and next {next_value}, account {self.account}")
            if not next_value:
                break

        return orders

    async def get_orders(self):
        """Получает все заказы от WB API."""
        orders = []
        next_value = 0
        while True:
            params = {"limit": 1000, "next": next_value}
            response = await self.async_client.get(f"{self.url}", params=params, headers=self.headers)
            data = parse_json(response)
            orders.extend(data.get("orders", []))
            next_value = data.get("next")
            logger.info(f"Получены {len(orders)} заказов and next {next_value}, account {self.account}")
            if not next_value:
                break

        return orders
