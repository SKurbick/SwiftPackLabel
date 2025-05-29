"""
Модуль для форматирования данных в формат, необходимый для интеграции с 1C.
"""
import asyncio
import json

from aiohttp import BasicAuth
from typing import List, Dict, Any, Optional, Tuple

from starlette import status

from src.logger import app_logger as logger
from src.__init__ import account_inn_map
from src.utils import get_wb_tokens
from src.wildberries_api.orders import Orders
from src.response import AsyncHttpClient
from src.settings import settings


class OneCIntegration:
    """
    Класс для подготовки и форматирования данных о поставках для интеграции с 1C.
    Преобразует информацию о заказах и поставках в структурированный формат.
    """

    def __init__(self):
        """Инициализация класса интеграции с 1C."""
        self.async_client = AsyncHttpClient(timeout=240)

    @staticmethod
    def convert_price(price) -> float:
        """
        Конвертирует цену из копеек в рубли с округлением до 3 знаков.
        Args:
            price: Цена в копейках
        Returns:
            float: Цена в рублях, округленная до 3 знаков
        """
        try:
            return round(price / 100, 3)
        except Exception:
            return 0

    @staticmethod
    def initialize_data_structures(supply_ids: List[Any]) -> Tuple[
        Dict[str, Dict], Dict[str, List[int]], Dict[int, str]]:
        """
        Инициализирует структуры данных, необходимые для обработки поставок.
        Args:
            supply_ids: Список объектов с информацией о поставках
        Returns:
            Tuple, содержащий:
            - result_structure: Структура для накопления результатов
            - accounts_orders: Словарь с заказами, сгруппированными по аккаунтам
            - order_supply_map: Словарь соответствия ID заказов и ID поставок
        """
        result_structure = {}
        accounts_orders = {}

        for supply_info in supply_ids:
            if supply_info.account not in accounts_orders:
                accounts_orders[supply_info.account] = []
                if supply_info.account not in result_structure:
                    result_structure[supply_info.account] = {
                        "data": [],
                        "wild_supply_orders": {}
                    }
            accounts_orders[supply_info.account].extend(supply_info.order_ids)

        order_supply_map = {}
        for supply in supply_ids:
            for order_id in supply.order_ids:
                order_supply_map[order_id] = supply.supply_id

        return result_structure, accounts_orders, order_supply_map

    @staticmethod
    async def get_filtered_orders(account: str, order_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Получает и фильтрует заказы по указанным ID.
        Args:
            account: Имя аккаунта
            order_ids: Список ID заказов для фильтрации
        Returns:
            List[Dict[str, Any]]: Отфильтрованный список заказов
        """
        wb_tokens = get_wb_tokens()
        if account not in wb_tokens:
            logger.error(f"Не найден токен для аккаунта {account}")
            return []

        try:
            orders_api = Orders(account, wb_tokens[account])
            all_orders = await orders_api.get_orders()

            order_ids_set = set(order_ids)
            filtered_orders = [order for order in all_orders if order.get('id') in order_ids_set]

            logger.info(
                f"Получено {len(filtered_orders)} заказов из {len(order_ids)} запрошенных для аккаунта {account}")

            return filtered_orders
        except Exception as e:
            logger.error(f"Ошибка при получении заказов для аккаунта {account}: {str(e)}")
            return []

    def create_order_data(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Создает структуру данных заказа на основе полученных данных от API.
        Args:
            order: Данные заказа от API
        Returns:
            Dict[str, Any]: Структурированные данные заказа
        """
        order_data = {"id": order.get("id")}
        if "convertedPrice" in order:
            order_data["price"] = self.convert_price(order["convertedPrice"])

        for key, value in order.items():
            if key not in ["id"] and all(price_key not in key.lower() for price_key in ["price", "cost"]):
                order_data[key] = value

        return order_data

    @staticmethod
    def find_or_create_supply_item(wild_supply_orders: Dict, wild_code: str, supply_id: str) -> Dict[str, Any]:
        """
        Находит или создает элемент поставки в структуре данных.
        Args:
            wild_supply_orders: Структура wild_supply_orders для аккаунта
            wild_code: Код wild
            supply_id: ID поставки
        Returns:
            Dict[str, Any]: Элемент поставки (существующий или новый)
        """

        if wild_code not in wild_supply_orders:
            wild_supply_orders[wild_code] = {"wild_code": wild_code, "supplies": []}

        for supply_item in wild_supply_orders[wild_code]["supplies"]:
            if supply_item.get("supply_id") == supply_id:
                return supply_item

        supply_item = {"supply_id": supply_id, "orders": []}
        wild_supply_orders[wild_code]["supplies"].append(supply_item)
        return supply_item

    async def process_account_orders(
            self,
            account: str,
            order_ids: List[int],
            order_wild_map: Dict[str, str],
            order_supply_map: Dict[int, str],
            result_structure: Dict[str, Dict]
    ) -> None:
        """
        Обрабатывает заказы для одного аккаунта.
        Args:
            account: Имя аккаунта
            order_ids: Список ID заказов для обработки
            order_wild_map: Словарь соответствия ID заказов и wild-кодов
            order_supply_map: Словарь соответствия ID заказов и ID поставок
            result_structure: Структура для накопления результатов
        """
        filtered_orders = await self.get_filtered_orders(account, order_ids)
        for order in filtered_orders:
            order_id = order.get("id")
            if not order_id:
                continue

            order_id_str = str(order_id)
            wild_code = order_wild_map.get(order_id_str)
            if not wild_code:
                logger.warning(f"Не найден wild-код для заказа {order_id}")
                continue

            supply_id = order_supply_map.get(order_id)
            if not supply_id:
                logger.warning(f"Не найден supply_id для заказа {order_id}")
                continue

            order_data = self.create_order_data(order)

            wild_supply_orders = result_structure[account]["wild_supply_orders"]

            supply_item = self.find_or_create_supply_item(
                wild_supply_orders, wild_code, supply_id
            )

            supply_item["orders"].append(order_data)

    @staticmethod
    def format_wild_item(wild_code: str, supplies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Форматирует данные о wild-коде в итоговую структуру.
        Args:
            wild_code: Код wild
            supplies: Список поставок для данного wild-кода
        Returns:
            Dict[str, Any]: Структурированные данные о wild-коде
        """
        wild_item = {"wild_code": wild_code, "supplies": []}

        for supply_item in supplies:
            supply_id = supply_item.get("supply_id")
            orders = supply_item.get("orders", [])

            supply_data = {"supply_id": supply_id, "orders": [
                {
                    "order_id": str(order.get("id")),
                    "price": float(order.get("price", 0)),
                    "nm_id": order.get("nmId"),
                    "count": 1
                } for order in orders]}
            wild_item["supplies"].append(supply_data)

        return wild_item

    def format_account_data(self, account: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Форматирует данные аккаунта в итоговую структуру.
        Args:
            account: Имя аккаунта
            data: Данные аккаунта
        Returns:
            Optional[Dict[str, Any]]: Структурированные данные аккаунта
        """
        if "wild_supply_orders" not in data or not data["wild_supply_orders"]:
            return None

        account_data = {"account": account, "inn": account_inn_map.get(account, ""), "data": []}

        for wild_code, wild_data in data["wild_supply_orders"].items():
            if "supplies" not in wild_data or not wild_data["supplies"]:
                continue

            wild_item = self.format_wild_item(wild_code, wild_data["supplies"])
            account_data["data"].append(wild_item)

        return account_data if account_data["data"] else None

    def build_final_structure(self, result_structure: Dict[str, Dict]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Строит окончательную структуру данных для 1C на основе накопленных результатов.
        Args:
            result_structure: Накопленная структура результатов по аккаунтам
        Returns:
            Dict[str, List[Dict[str, Any]]]: Итоговая структура для 1C
        """
        accounts = []

        for account, data in result_structure.items():
            if account_data := self.format_account_data(account, data):
                accounts.append(account_data)

        return {"accounts": accounts}

    async def send_to_1c(self, request_body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Отправляет POST запрос к 1C с указанным телом запроса.
        Args:
            request_body: Тело запроса для отправки в 1C
        Returns:
            Dict[str, Any]: Ответ от 1C или информация об ошибке
        """

        logger.info(f"Отправка данных в 1C: {len(str(request_body))} байт")

        if not settings.ONEC_USER or not settings.ONEC_PASSWORD:
            return {"status_code": 500, "message": "Отсутствуют учетные данные для 1C"}

        try:
            auth = BasicAuth(settings.ONEC_USER, settings.ONEC_PASSWORD)
            headers = {"Content-Type": "application/json", "Accept": "application/json"}

            response_text = await self.async_client._make_request(
                "POST", settings.ONEC_HOST, json=request_body,
                headers=headers, auth=auth
            )

            try:
                result = json.loads(response_text) if isinstance(response_text, str) else response_text
                logger.info(f"Успешный ответ от 1C: {result}")
                return result
            except json.JSONDecodeError:
                logger.error(f"Ошибка при чтении JSON ответа от 1C: {response_text}")
                return {"status_code": 500,"message": "Ошибка при чтении JSON ответа","response": response_text}

        except Exception as e:
            logger.error(f"Ошибка при отправке данных в 1C: {str(e)}")
            return {"status_code": 500, "message": f"Ошибка при отправке данных в 1C: {str(e)}"}

    async def format_delivery_data(self, supply_ids: List[Any], order_wild_map: Dict[str, str]) -> Dict[
        str, List[Dict[str, Any]]]:
        """
        Форматирует данные о доставке поставок в структуру для 1C.
        Args:
            supply_ids: Список объектов с информацией о поставках
            order_wild_map: Словарь соответствия ID заказов и wild-кодов
        Returns:
            Dict[str, List[Dict[str, Any]]]: Структурированные данные для 1C
        """
        logger.info(f"Начало форматирования данных для 1C: {len(supply_ids)} поставок и {len(order_wild_map)} заказов")

        try:
            result_structure, accounts_orders, order_supply_map = self.initialize_data_structures(supply_ids)
            tasks = [
                self.process_account_orders(
                    account, order_ids, order_wild_map, order_supply_map, result_structure
                ) for account, order_ids in accounts_orders.items()
            ]
            await asyncio.gather(*tasks)
            result =  await self.send_to_1c(self.build_final_structure(result_structure))
            return result

        except Exception as e:
            logger.error(f"Ошибка при форматировании данных для 1C: {str(e)}")
            raise
