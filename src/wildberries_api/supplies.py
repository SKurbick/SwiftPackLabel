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
