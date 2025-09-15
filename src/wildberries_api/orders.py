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

    async def can_add_to_supply(self, order_id: int) -> bool:
        """
        Проверяет, можно ли добавить сборочное задание в поставку.
        
        :param order_id: ID сборочного задания
        :return: True если можно добавить, False если нельзя
        """
        self.async_client.retries = 90
        self.async_client.delay = 61
        try:
            # Получаем статус заказа
            orders_response = await self.get_orders_statuses([order_id])
            orders_data = orders_response.get("orders", [])
            
            if not orders_data:
                logger.warning(f"Не найден статус для заказа {order_id}")
                return False
            
            order_status = orders_data[0]
            supplier_status = order_status.get("supplierStatus")
            
            # Проверяем supplier_status - можно добавлять "new" и "confirm"
            allowed_statuses = ["new", "confirm"]
            if supplier_status not in allowed_statuses:
                logger.info(f"Заказ {order_id} нельзя добавить в поставку: статус '{supplier_status}' (нужен {allowed_statuses})")
                return False
            
            logger.info(f"Заказ {order_id} можно добавить в поставку (статус: supplier='{supplier_status}')")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка проверки статуса заказа {order_id}: {e}")
            return False

    async def get_stickers_to_orders(self, supply, order_ids: list[int]):
        logger.info(f"Getting stickers for supply {supply}, account {self.account}, orders count: {len(order_ids)}")
        logger.debug(f"Order IDs: {order_ids}")
        
        url_with_params = f"{self.url}/stickers?type=png&width=58&height=40"
        batches = [order_ids[i:i + 99] for i in range(0, len(order_ids), 99)]
        logger.info(f"Split into {len(batches)} batches of 99 orders each")

        sticker_batches = await asyncio.gather(
            *[self.async_client.post(url_with_params, headers=self.headers, data=json.dumps({"orders": batch}))
              for batch in batches])
        
        logger.info(f"Received responses from {len(sticker_batches)} batches")

        result = {}
        for i, response in enumerate(sticker_batches):
            if not response:
                logger.warning(f"Empty response from batch {i+1}")
                continue

            try:
                response_json = parse_json(response)
                logger.debug(f"Batch {i+1}: received stickers - {len(response_json.get('stickers', []))}")
            except Exception as e:
                logger.error(f"Error parsing response from batch {i+1}: {str(e)}")
                logger.error(f"Raw response: {response}")
                continue

            if not result:
                result = {self.account: {supply: response_json}}
                logger.debug(f"Initialized result for account {self.account}, supply {supply}")
            else:
                result[self.account][supply]['stickers'].extend(response_json['stickers'])
                logger.debug(f"Added {len(response_json['stickers'])} stickers to existing result")

        total_stickers = len(result.get(self.account, {}).get(supply, {}).get('stickers', []))
        logger.info(f"Completed getting stickers. Total stickers: {total_stickers}")
        return result

    async def get_new_orders(self):
        """Gets new orders from WB API."""
        orders = []
        next_value = 0
        while True:
            params = {"limit": 1000, "next": next_value}
            response = await self.async_client.get(f"{self.url}/new", params=params, headers=self.headers)
            data = parse_json(response)
            orders.extend(data.get("orders", []))
            next_value = data.get("next")
            logger.info(f"Got {len(orders)} new orders and next {next_value}, account {self.account}")
            if not next_value:
                break

        return orders

    async def get_orders(self):
        """Gets all orders from WB API."""
        orders = []
        next_value = 0
        while True:
            params = {"limit": 1000, "next": next_value}
            response = await self.async_client.get(f"{self.url}", params=params, headers=self.headers)
            data = parse_json(response)
            orders.extend(data.get("orders", []))
            next_value = data.get("next")
            logger.info(f"Got {len(orders)} orders and next {next_value}, account {self.account}")
            if not next_value:
                break

        return orders
