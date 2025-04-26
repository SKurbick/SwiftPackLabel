from src.response import parse_json
from src.users.account import Account
from src.logger import app_logger as logger


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

    async def get_supply_orders(self, supply_id: str):
        response = await self.async_client.get(f"{self.url}/{supply_id}/orders", headers=self.headers)
        response_json = parse_json(response)
        return {self.account: {supply_id: {"orders": response_json.get('orders', [])}}}

    async def create_supply(self, name: str) -> dict:
        """
        Создаёт новую поставку в кабинете по наименованию.
        :param name: Наименование поставки
        :return: Ответ от WB API (id поставки или ошибка)
        """
        response = await self.async_client.post(self.url, json={"name": name}, headers=self.headers)
        logger.info(f"Создана поставка с именем '{name}' для аккаунта {self.account}. Ответ: {response}")
        return parse_json(response)

    async def add_order_to_supply(self, supply_id: str, order_id: int) -> dict:
        """
        Добавляет сборочное задание (orderId) к поставке (supplyId) через PATCH-запрос к WB API.
        :param supply_id: ID поставки (например, WB-GI-1234567)
        :param order_id: ID сборочного задания (orderId)
        :return: Ответ от WB API
        """
        self.async_client.retries = 3
        self.async_client.delay = 60
        url = f"{self.url}/{supply_id}/orders/{order_id}"
        response = await self.async_client.patch(url, headers=self.headers)
        logger.info(f"Добавлен заказ {order_id} в поставку {supply_id} для аккаунта {self.account}. Ответ: {response}")
        return parse_json(response)

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
        return parse_json(response)
