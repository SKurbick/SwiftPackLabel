import asyncio
import json
import base64
import io
from typing import List, Dict, Any, Set, Optional, Tuple
from datetime import datetime
from collections import defaultdict
from PIL import Image

from src.settings import settings
from src.logger import app_logger as logger
from src.supplies.integration_1c import OneCIntegration
from src.utils import get_wb_tokens, process_local_vendor_code
from src.wildberries_api.supplies import Supplies
from src.wildberries_api.orders import Orders
from src.db import AsyncGenerator
from src.models.card_data import CardData
from src.models.shipment_of_goods import ShipmentOfGoods
from src.models.hanging_supplies import HangingSupplies
from src.response import AsyncHttpClient, parse_json
from fastapi import HTTPException

from src.supplies.schema import (
    SupplyIdResponseSchema, SupplyIdBodySchema, OrderSchema, StickerSchema, SupplyId,
    SupplyDeleteBody, SupplyDeleteResponse, SupplyDeleteItem, WildFilterRequest, DeliverySupplyInfo,
    SupplyIdWithShippedBodySchema
)


class SuppliesService:

    def __init__(self, db: AsyncGenerator = None):
        self.db = db
        self.async_client = AsyncHttpClient(timeout=120, retries=3, delay=5)

    @staticmethod
    def format_data_to_result(supply: SupplyId, order: StickerSchema, name_and_photo: Dict[int, Dict[str, Any]]) -> \
            Dict[str, Any]:
        return {"order_id": order.order_id,
                "account": supply.account,
                "article": order.local_vendor_code,
                "supply_id": supply.supply_id,
                "nm_id": order.nm_id,
                "file": order.file,
                "partA": order.partA,
                "partB": order.partB,
                "category": name_and_photo.get(order.nm_id, {"category": "НЕТ Категории"})["category"],
                "subject_name": name_and_photo.get(order.nm_id, {"subject_name": "НЕТ Наименования"})["subject_name"],
                "photo_link": name_and_photo.get(order.nm_id, {"photo_link": "НЕТ ФОТО"})["photo_link"]}

    @staticmethod
    def _change_category_name(result: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        logger.info("Изменение наименования категории если есть различия")
        for items in result.values():
            if categories := {item['subject_name'] for item in items}:
                if len(categories) > 1:
                    max_category = min(categories)
                    for item in items:
                        item['subject_name'] = max_category
        return result

    async def group_orders_to_wild(self, supply_ids: SupplyIdBodySchema) -> Dict[str, List[Dict[str, Any]]]:
        logger.info("Получение недостающих данных о заказе и группировка с сортировкой всех данных по wild")
        result = {}
        name_and_photo = await CardData(self.db).get_subject_name_category_and_photo_to_article(
            [order.nm_id for orders in supply_ids.supplies for order in orders.orders])
        name_and_photo: Dict[int, Dict[str, Any]] = \
            {data["article_id"]: {"subject_name": data["subject_name"], "photo_link": data["photo_link"],
                                  "category": data["parent_name"]}
             for data in name_and_photo}
        order: StickerSchema
        for supply in supply_ids.supplies:
            for order in supply.orders:
                if order.local_vendor_code not in result:
                    result[order.local_vendor_code] = [self.format_data_to_result(supply, order, name_and_photo)]
                else:
                    result[order.local_vendor_code].append(self.format_data_to_result(supply, order, name_and_photo))
        # self._change_category_name(result)
        return dict(sorted(result.items(), key=lambda x: (min(item['subject_name'] for item in x[1]), x[0]), ))

    @staticmethod
    async def get_information_to_supplies() -> List[Dict]:
        logger.info("Получение данных по всем кабинетам о поставках")
        tasks: List = []
        for account, token in get_wb_tokens().items():
            tasks.append(Supplies(account, token).get_supplies_filter_done())
        return await asyncio.gather(*tasks)

    @staticmethod
    async def get_information_orders_to_supplies(supply_ids: List[dict]) -> List[Dict[str, Dict]]:
        logger.info(f'Получение информации о заказах по конкретным поставкам,количество поставок : {len(supply_ids)}')
        tasks = []
        for supplies in supply_ids:
            for account, supply in supplies.items():
                for sup in supply:
                    tasks.append(Supplies(account, get_wb_tokens()[account]).get_supply_orders(sup.get("id")))
        return await asyncio.gather(*tasks)

    @staticmethod
    def group_result(result: List[dict]) -> Dict[str, Dict]:
        logger.info("Формирование данных в стандартной форме аккаунт : значения")
        finished_orders = {}
        for order in result:
            for account, value in order.items():
                if account not in finished_orders:
                    finished_orders[account] = value
                else:
                    finished_orders[account].update(value)
        return finished_orders

    @staticmethod
    async def get_stickers(supplies_ids: SupplyIdBodySchema):
        tasks = []
        for supply in supplies_ids.supplies:
            tasks.append(
                Orders(supply.account, settings.tokens[supply.account]).get_stickers_to_orders(supply.supply_id,
                                                                                               [v.order_id for v in
                                                                                                supply.orders]))
        return await asyncio.gather(*tasks)

    @staticmethod
    def union_results_stickers(supply_orders: SupplyIdBodySchema, stickers: Dict[str, Dict]):
        logger.info("Формирование данных c полученными qr кодами в общий словарь")
        for supply in supply_orders.supplies:
            orders: List[OrderSchema] = sorted(supply.orders, key=lambda x: x.order_id)
            sticker: List[Dict[str, Any]] = sorted(stickers[supply.account][supply.supply_id]['stickers'],
                                                   key=lambda x: x['orderId'])
            for n, v in enumerate(orders):
                if v.order_id == sticker[n].get('orderId'):
                    order_dict: Dict[str, Any] = v.dict()
                    combined_data: Dict[str, Any] = {**order_dict, **sticker[n]}
                    supply.orders[n]: List[StickerSchema] = StickerSchema(**combined_data)

    @staticmethod
    def create_supply_result(supply: Dict[str, Dict[str, Any]], supply_id: str, account: str,
                             orders: Dict[str, List[Dict]]):
        return {"name": supply[supply_id].get("name"),
                "createdAt": supply[supply_id].get("createdAt"),
                "supply_id": supply_id,
                "account": account,
                "count": len(orders['orders']),
                "orders": [
                    OrderSchema(order_id=data["id"], nm_id=data["nmId"],
                                local_vendor_code=process_local_vendor_code(data["article"]))
                    for data in orders["orders"]]}

    async def filter_supplies_by_hanging(self, supplies_data: List, hanging_only: bool = False) -> List:
        """
        Фильтрует список поставок по признаку "висячая".
        Args:
            supplies_data: Список поставок для фильтрации
            hanging_only: Если True - оставить только висячие поставки, если False - только обычные (не висячие)
        Returns:
            List: Отфильтрованный список поставок
        """
        hanging_supplies_list = await HangingSupplies(self.db).get_hanging_supplies()
        hanging_supplies_map = {(hs['supply_id'], hs['account']): hs for hs in hanging_supplies_list}

        # target_wilds = {'wild1512', 'wild355', 'wild354', 'wild102', 'wild659', 'wild399'}
        target_wilds = {}
        filtered_supplies = []
        for supply in supplies_data:
            is_hanging = (supply['supply_id'], supply['account']) in hanging_supplies_map

            if hanging_only == is_hanging:
                if hanging_only:
                    supply["is_hanging"] = True

                    # Добавляем количество отгруженных товаров
                    hanging_supply_data = hanging_supplies_map[(supply['supply_id'], supply['account'])]
                    shipped_orders = hanging_supply_data.get('shipped_orders', [])

                    # Десериализуем shipped_orders если это строка JSON
                    if isinstance(shipped_orders, str):
                        try:
                            shipped_orders = json.loads(shipped_orders)
                        except json.JSONDecodeError:
                            shipped_orders = []

                    if shipped_orders and isinstance(shipped_orders, list):
                        # Подсчитываем уникальные ID заказов
                        unique_shipped_ids = set(
                            order.get('order_id') for order in shipped_orders
                            if isinstance(order, dict) and order.get('order_id')
                        )
                        supply["shipped_count"] = len(unique_shipped_ids)
                    else:
                        supply["shipped_count"] = 0

                    has_target_wild = any(
                        order.local_vendor_code in target_wilds
                        for order in supply.get('orders', [])
                    )
                    if not has_target_wild:
                        filtered_supplies.append(supply)
                else:
                    filtered_supplies.append(supply)

        return filtered_supplies

    async def get_list_supplies(self, hanging_only: bool = False) -> SupplyIdResponseSchema:
        """
        Получить список поставок с фильтрацией по висячим.
        Args:
            hanging_only: Если True - вернуть только висячие поставки, если False - только обычные (не висячие)
        Returns:
            SupplyIdResponseSchema: Список поставок с их деталями
        """
        logger.info(f"Получение данных о поставках, hanging_only={hanging_only}")
        supplies_ids: List[Any] = await self.get_information_to_supplies()
        supplies: Dict[str, Dict] = self.group_result(await self.get_information_orders_to_supplies(supplies_ids))
        result: List = []
        supplies_ids: Dict[str, List] = {key: value for d in supplies_ids for key, value in d.items()}
        for account, value in supplies.items():
            for supply_id, orders in value.items():
                supply: Dict[str, Dict[str, Any]] = {data["id"]: {"name": data["name"], "createdAt": data['createdAt']}
                                                     for data in supplies_ids[account] if not data['done']}
                result.append(self.create_supply_result(supply, supply_id, account, orders))

        filtered_result = await self.filter_supplies_by_hanging(result, hanging_only)
        return SupplyIdResponseSchema(supplies=filtered_result)

    async def check_current_orders(self, supply_ids: SupplyIdBodySchema, allow_partial: bool = False):
        logger.info("Проверка поставок на соответствие наличия заказов (сверка заказов по поставкам)")
        tasks: List = []
        for supply in supply_ids.supplies:
            tasks.append(Supplies(supply.account, get_wb_tokens()[supply.account]).get_supply_orders(supply.supply_id))
        result: Dict[str, Dict] = self.group_result(await asyncio.gather(*tasks))
        for supply in supply_ids.supplies:
            supply_orders: Set[int] = {order.order_id for order in supply.orders}
            check_orders: Set[int] = {order.get("id") for order in
                                      result[supply.account][supply.supply_id].get("orders", [])}

            if allow_partial:
                # Для частичной отгрузки: проверяем, что заказы из запроса существуют в поставке
                missing_orders = supply_orders - check_orders
                if missing_orders:
                    raise HTTPException(status_code=409,
                                        detail=f'Заказы {missing_orders} не найдены в поставке {supply.supply_id} '
                                               f'в кабинете {supply.account}')
            else:
                # Для полной печати: проверяем точное соответствие (текущая логика)
                diff: Set[int] = supply_orders.symmetric_difference(check_orders)
                if diff:
                    raise HTTPException(status_code=409,
                                        detail=f'Есть различия между поставками {diff} в кабинете {supply.account}'
                                               f' Номер поставки : {supply.supply_id}')

    async def filter_and_fetch_stickers(self, supply_ids: SupplyIdBodySchema) -> Dict[str, List[Dict[str, Any]]]:
        logger.info('Инициализация получение документов (Стикеры и Лист подбора)')
        await self.check_current_orders(supply_ids)
        stickers: Dict[str, Dict] = self.group_result(await self.get_stickers(supply_ids))
        self.union_results_stickers(supply_ids, stickers)
        return await self.group_orders_to_wild(supply_ids)

    @staticmethod
    async def delete_single_supply(account: str, supply_id: str, token: str) -> Optional[SupplyDeleteItem]:
        """Удаляет одну поставку и возвращает информацию об удалённой поставке или None в случае ошибки"""
        try:
            supply = Supplies(account, token)
            resp = await supply.delete_supply(supply_id)
            if resp.get("errors"):
                logger.error(f"Ошибка при удалении {supply_id} для {account}: {resp['errors']}")
                return
            logger.info(f"Поставка {supply_id} для {account} успешно удалена")
            return SupplyDeleteItem(account=account, supply_id=supply_id)
        except Exception as e:
            logger.error(f"Ошибка при удалении {supply_id} для {account}: {str(e)}")
            return

    async def delete_supplies(self, body: SupplyDeleteBody) -> SupplyDeleteResponse:
        """Удаляет несколько поставок и возвращает список успешно удалённых"""
        logger.info(f"Удаление поставок: {body.supply}")
        tokens = get_wb_tokens()
        tasks = []
        for item in body.supply:
            token = tokens.get(item.account)
            tasks.append(self.delete_single_supply(item.account, item.supply_id, token))

        results = await asyncio.gather(*tasks)
        deleted_ids = [item for item in results if item is not None]

        return SupplyDeleteResponse(deleted=deleted_ids)

    async def filter_and_fetch_stickers_by_wild(self, wild_filter: WildFilterRequest) -> Dict[
        str, List[Dict[str, Any]]]:
        """
        Фильтрует заказы по указанному wild и получает для них стикеры.
        Args:
            wild_filter: Данные о wild, поставках и заказах для фильтрации
        Returns:
            Dict[str, List[Dict[str, Any]]]: Сгруппированные данные о заказах со стикерами
        """
        logger.info(f'Инициализация получения стикеров для wild: {wild_filter.wild}')

        supplies_list = []

        for supply_item in wild_filter.supplies:
            orders_details = await self._get_orders_details(
                supply_item.account,
                supply_item.supply_id,
                [order.order_id for order in supply_item.orders]
            )

            orders_list = []
            orders_list.extend(
                OrderSchema(order_id=order_detail.get('id'), nm_id=order_detail.get('nmId'),
                            local_vendor_code=wild_filter.wild)
                for order_detail in orders_details if order_detail.get('id') in [order.order_id
                                                                                 for order in supply_item.orders])
            if not orders_list:
                continue

            supplies_list.append(
                SupplyId(
                    name="",  # Имя не важно для генерации стикеров
                    createdAt="",  # Дата создания не важна для генерации стикеров
                    supply_id=supply_item.supply_id,
                    account=supply_item.account,
                    count=len(orders_list),
                    orders=orders_list)
            )

        if not supplies_list:
            logger.warning(f"Не найдено заказов для wild: {wild_filter.wild}")
            return {wild_filter.wild: []}

        supply_ids_body = SupplyIdBodySchema(supplies=supplies_list)

        stickers: Dict[str, Dict] = self.group_result(await self.get_stickers(supply_ids_body))
        self.union_results_stickers(supply_ids_body, stickers)

        result = await self.group_orders_to_wild(supply_ids_body)

        if wild_filter.wild not in result and len(result) > 0:
            first_key = next(iter(result))
            result[wild_filter.wild] = result.pop(first_key)

        return result

    async def _get_orders_details(self, account: str, supply_id: str, order_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Получает детали заказов для указанной поставки.
        Args:
            account: Аккаунт WB
            supply_id: ID поставки
            order_ids: Список ID заказов
        Returns:
            List[Dict[str, Any]]: Список с деталями заказов
        """
        try:
            supply = Supplies(account, get_wb_tokens()[account])
            supply_data = await supply.get_supply_orders(supply_id)

            if not supply_data or account not in supply_data or supply_id not in supply_data[account]:
                logger.error(f"Не удалось получить данные о поставке {supply_id} для аккаунта {account}")
                return []

            all_orders = supply_data[account][supply_id].get("orders", [])

            filtered_orders = [order for order in all_orders if order.get("id") in order_ids]

            return filtered_orders
        except Exception as e:
            logger.error(f"Ошибка при получении деталей заказов для {supply_id}, {account}: {str(e)}")
            return []

    @staticmethod
    async def process_delivery_supplies(supply_ids: List[DeliverySupplyInfo]):
        """
        Отправляет запросы на перевод поставок в статус доставки в Wildberries API.

        Args:
            supply_ids: Список поставок для перевода в статус доставки
        """
        wb_tokens = get_wb_tokens()
        tasks = [Supplies(supply.account, wb_tokens.get(supply.account, "")).deliver_supply(supply.supply_id)
                 for supply in supply_ids]
        await asyncio.gather(*tasks, return_exceptions=True)

    @staticmethod
    async def prepare_shipment_data(supply_ids: List[DeliverySupplyInfo], order_wild_map: Dict[str, str],
                                    author: str, warehouse_id: int = 1, delivery_type: str = "ФБС") -> List[
        Dict[str, Any]]:
        """
        Подготавливает данные для отправки в API shipment_of_goods.

        Args:
            supply_ids: Список поставок для перевода в статус доставки
            order_wild_map: Соответствие заказов и артикулов wild
            author: Имя автора отгрузки
            warehouse_id: ID склада (по умолчанию 1)
            delivery_type: Тип доставки (по умолчанию "ФБС")

        Returns:
            List[Dict[str, Any]]: Список данных для отправки в API shipment_of_goods
        """
        logger.info(f"Подготовка данных для записи в таблицу shipment_of_goods: {len(supply_ids)} поставок")

        result = []

        for supply_info in supply_ids:
            supply_orders = [str(order_id) for order_id in supply_info.order_ids]

            wild_orders = {}
            for order_id in supply_orders:
                if order_id in order_wild_map:
                    wild_code = order_wild_map[order_id]
                    if wild_code not in wild_orders:
                        wild_orders[wild_code] = 0
                    wild_orders[wild_code] += 1

            if not wild_orders:
                logger.warning(f"Для поставки {supply_info.supply_id} не найдено соответствий wild-кодов")
                continue

            for wild_code, quantity in wild_orders.items():
                shipment_data = {
                    "author": author,
                    "supply_id": supply_info.supply_id,
                    "product_id": wild_code,
                    "warehouse_id": warehouse_id,
                    "delivery_type": delivery_type,
                    "shipment_date": datetime.now().strftime("%Y-%m-%d"),
                    "wb_warehouse": "",
                    "account": supply_info.account,
                    "quantity": quantity
                }

                result.append(shipment_data)
                logger.info(f"Подготовлены данные для отгрузки: {supply_info.supply_id}, {wild_code}, {quantity}")

        logger.info(f"Всего подготовлено {len(result)} записей для таблицы shipment_of_goods")
        return result

    async def save_shipments(self,
                             supply_ids: List[DeliverySupplyInfo],
                             order_wild_map: Dict[str, str],
                             author: str,
                             warehouse_id: int = 1,
                             delivery_type: str = "ФБС") -> bool:
        """
        Отправляет данные об отгрузках в API shipment_of_goods.
        """
        logger.info(f"Отправка данных об отгрузках: {len(supply_ids)} поставок")

        shipment_data = await self.prepare_shipment_data(
            supply_ids, order_wild_map, author, warehouse_id, delivery_type
        )

        # Получаем список доступных wild-кодов для фильтрации
        shipment_repository = ShipmentOfGoods(self.db)
        filter_wild = await shipment_repository.filter_wilds()

        filtered_shipment_data = [item for item in shipment_data if item['product_id'] in filter_wild]
        logger.info(f"Отфильтровано записей: {len(shipment_data)} -> {len(filtered_shipment_data)}")

        if not filtered_shipment_data:
            logger.warning("Нет данных для отправки в API")
            return False

        # Отправляем данные в API
        return await self._send_shipment_data_to_api(filtered_shipment_data)

    async def _send_shipment_data_to_api(self, shipment_data: List[Dict[str, Any]]) -> bool:
        """
        Отправляет данные об отгрузках в API /api/shipment_of_goods/update
        
        Args:
            shipment_data: Список данных для отправки
            
        Returns:
            bool: True если отправка успешна, False в противном случае
        """
        logger.info(f'Входные данные : {shipment_data}')
        response_text = await self.async_client.post(
            settings.SHIPMENT_API_URL, json=shipment_data)

        if response_text:
            try:
                response_data = parse_json(response_text)
                logger.info(f"Данные успешно отправлены в API: {response_data}")
                return True
            except ValueError as e:
                logger.error(f"Ошибка парсинга ответа API: {e}")
                logger.error(f"Сырой ответ: {response_text}")
                return False
        else:
            logger.error("Не получен ответ от API")
            return False

    def validate_unique_vendor_code(self, supplies: List[SupplyId]) -> str:
        """
        Проверяет, что все заказы имеют одинаковый local_vendor_code.
        Args:
            supplies: Список поставок для проверки
        Returns:
            str: Уникальный vendor_code
        Raises:
            HTTPException: Если найдено несколько разных vendor_code
        """
        vendor_codes = set()
        for supply in supplies:
            for order in supply.orders:
                vendor_codes.add(order.local_vendor_code)

        if len(vendor_codes) != 1:
            raise HTTPException(
                status_code=400,
                detail=f"Все заказы должны иметь одинаковый local_vendor_code. Найдено: {vendor_codes}"
            )

        return vendor_codes.pop()

    async def get_hanging_supplies_order_data_optimized(self, supplies: List[SupplyId]) -> Dict[str, dict]:
        """
        Получает данные о заказах из базы данных оптимизированным способом.
        Args:
            supplies: Список поставок
        Returns:
            Dict[str, dict]: Данные о заказах по ключу supply_id
        """
        supply_ids = [supply.supply_id for supply in supplies]
        hanging_supplies_model = HangingSupplies(self.db)
        return await hanging_supplies_model.get_order_data_by_supplies(supply_ids)

    def _get_shipped_order_ids(self, shipped_orders) -> set:
        """Извлекает множество ID уже отгруженных заказов."""
        if isinstance(shipped_orders, str):
            try:
                shipped_orders = json.loads(shipped_orders)
            except json.JSONDecodeError:
                shipped_orders = []

        shipped_order_ids = set()
        if shipped_orders and isinstance(shipped_orders, list):
            for shipped_order in shipped_orders:
                if isinstance(shipped_order, dict) and "order_id" in shipped_order:
                    shipped_order_ids.add(shipped_order["order_id"])
        return shipped_order_ids

    def _filter_available_orders(self, orders_list: List[dict], shipped_order_ids: set, supply_id: str, account: str) -> \
            List[dict]:
        """Фильтрует доступные (не отгруженные) заказы для одной поставки."""
        result = []
        for order in orders_list:
            if order["id"] not in shipped_order_ids:
                # Безопасное получение полей с правильными названиями из БД
                created_at = order.get("created_at", order.get("createdAt", ""))  # Пробуем оба варианта
                created_at_ts = 0

                if created_at:
                    try:
                        created_at_ts = datetime.fromisoformat(created_at.replace('Z', '+00:00')).timestamp()
                    except (ValueError, AttributeError):
                        logger.warning(f"Не удалось обработать created_at для заказа {order.get('id')}: {created_at}")
                        created_at_ts = 0

                order_data = {
                    "supply_id": supply_id,
                    "account": account,
                    "order_id": order["id"],
                    "created_at_ts": created_at_ts,
                    "created_at": created_at,
                    "article": order.get("article", ""),
                    "nm_id": order.get("nmId", order.get("nm_id", 0)),  # Пробуем оба варианта
                    "price": order.get("price", order.get("convertedPrice", 0))  # Пробуем оба варианта
                }
                result.append(order_data)

        return result

    def _deserialize_order_data(self, order_data_raw: Any, supply_id: str) -> dict:
        """Десериализует order_data из БД."""
        if isinstance(order_data_raw, str):
            try:
                return json.loads(order_data_raw)
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка десериализации order_data для поставки {supply_id}: {e}")
                raise HTTPException(status_code=500, detail=f"Ошибка данных поставки {supply_id}")
        return order_data_raw

    def _validate_request_orders(self, request_orders: dict, db_orders_map: dict, supply_id: str) -> None:
        """Валидирует, что заказы из запроса существуют в БД."""
        for order_id in request_orders.keys():
            if order_id not in db_orders_map:
                raise HTTPException(
                    status_code=400,
                    detail=f"Заказ {order_id} не найден в БД для поставки {supply_id}"
                )

    def _enrich_order_with_request_data(self, db_order: dict, request_order) -> dict:
        """Обогащает данные заказа из БД данными из запроса."""
        return {
            **db_order,  # Данные из БД (createdAt, convertedPrice, article, id)
            "nmId": request_order.nm_id,  # nm_id из запроса
            "local_vendor_code": request_order.local_vendor_code  # На всякий случай
        }

    def _process_request_orders(self, request_orders: dict, orders_list: List[dict],
                                shipped_order_ids: set, supply_id: str, account: str) -> Tuple[List[dict], int]:
        """Обрабатывает заказы из запроса."""
        db_orders_map = {order["id"]: order for order in orders_list}
        self._validate_request_orders(request_orders, db_orders_map, supply_id)

        filtered_orders = []
        for order_id, request_order in request_orders.items():
            if order_id not in shipped_order_ids:
                db_order = db_orders_map[order_id]
                enriched_order = self._enrich_order_with_request_data(db_order, request_order)
                filtered_orders.append(enriched_order)

        available_orders = self._filter_available_orders(filtered_orders, shipped_order_ids, supply_id, account)
        shipped_count = len(request_orders) - len(available_orders)

        logger.info(
            f"Поставка {supply_id}: {len(request_orders)} заказов в запросе, {shipped_count} уже отгружено, {len(available_orders)} доступно")
        return available_orders, shipped_count

    def _process_db_orders(self, orders_list: List[dict], shipped_order_ids: set,
                           supply_id: str, account: str) -> Tuple[List[dict], int]:
        """Обрабатывает заказы только из БД (старая логика)."""
        available_orders = self._filter_available_orders(orders_list, shipped_order_ids, supply_id, account)
        shipped_count = len(orders_list) - len(available_orders)

        logger.info(
            f"Поставка {supply_id}: {len(orders_list)} заказов, {shipped_count} уже отгружено, {len(available_orders)} доступно")
        return available_orders, shipped_count

    def _process_supply_orders(self, supply_id: str, data: dict, request_orders: dict = None) -> Tuple[List[dict], int]:
        """Координирует обработку заказов поставки."""
        # Десериализуем данные из БД
        order_data = self._deserialize_order_data(data["order_data"], supply_id)
        shipped_order_ids = self._get_shipped_order_ids(data["shipped_orders"])
        orders_list = order_data["orders"]
        account = data["account"]

        # Выбираем стратегию обработки
        if request_orders:
            return self._process_request_orders(request_orders, orders_list, shipped_order_ids, supply_id, account)
        else:
            return self._process_db_orders(orders_list, shipped_order_ids, supply_id, account)

    def extract_available_orders(self, hanging_data: Dict[str, dict], request_supplies: List[SupplyId] = None) -> List[
        dict]:
        """
        Извлечение доступных (не отгруженных) заказов на основе данных запроса с проверкой по БД.
        
        Args:
            hanging_data: Данные висячих поставок из БД по ключу supply_id
            request_supplies: Данные поставок из запроса (если None, используется старая логика)
            
        Returns:
            List[dict]: Отсортированный список доступных заказов (исключая уже отгруженные)
        """
        all_orders = []
        total_shipped = 0

        if request_supplies:
            request_orders_map = {}
            for supply in request_supplies:
                request_orders_map[supply.supply_id] = {order.order_id: order for order in supply.orders}

            for supply_id, data in hanging_data.items():
                if supply_id not in request_orders_map:
                    logger.warning(f"Поставка {supply_id} не найдена в запросе")
                    continue

                available_orders, shipped_count = self._process_supply_orders(supply_id, data,
                                                                              request_orders_map[supply_id])
                all_orders.extend(available_orders)
                total_shipped += shipped_count
        else:
            for supply_id, data in hanging_data.items():
                available_orders, shipped_count = self._process_supply_orders(supply_id, data)
                all_orders.extend(available_orders)
                total_shipped += shipped_count

        # FIFO сортировка: сначала по времени создания, затем по order_id
        all_orders.sort(key=lambda x: (x["created_at_ts"], x["order_id"]))
        logger.info(
            f"Обработано заказов из {len(hanging_data)} поставок: {total_shipped} уже отгружено, {len(all_orders)} доступно для отгрузки")

        return all_orders

    def group_selected_orders_by_supply(self, selected_orders: List[dict]) -> Dict[str, List[dict]]:
        """
        Группирует отобранные заказы по поставкам.
        Args:
            selected_orders: Отобранные заказы для отгрузки
        Returns:
            Dict[str, List[dict]]: Заказы, сгруппированные по supply_id
        """
        grouped = defaultdict(list)

        for order in selected_orders:
            supply_id = order["supply_id"]
            grouped[supply_id].append(order)

        logger.info(f"Заказы сгруппированы по {len(grouped)} поставкам")
        return dict(grouped)

    def _prepare_shipment_data(self, grouped_orders: Dict[str, List[dict]], timestamp: str) -> List[Tuple[str, str]]:
        """Подготавливает данные для batch обновления shipped_orders."""
        update_data = []
        for supply_id, orders in grouped_orders.items():
            shipped_orders_data = [
                {
                    "order_id": order["order_id"],
                    "supply_id": order["supply_id"],
                    "account": order["account"],
                    "article": order["article"],
                    "nm_id": order["nm_id"],
                    "price": order["price"],
                    "created_at": order["created_at"],
                    "shipped_at": timestamp
                }
                for order in orders
            ]
            update_data.append((supply_id, json.dumps(shipped_orders_data)))
        return update_data

    async def _execute_batch_update(self, update_data: List[Tuple[str, str]]):
        """Выполняет batch обновление hanging_supplies."""
        query = """
            UPDATE hanging_supplies 
            SET shipped_orders = shipped_orders || $2::jsonb
            WHERE supply_id = $1
        """

        try:
            for supply_id, shipped_data in update_data:
                await self.db.execute(query, supply_id, shipped_data)
            logger.info(f"Обновлено {len(update_data)} записей в hanging_supplies")
        except Exception as e:
            logger.error(f"Ошибка при обновлении hanging_supplies: {str(e)}")
            raise HTTPException(status_code=500, detail="Ошибка при обновлении БД")

    async def update_hanging_supplies_shipped_orders_batch(self, grouped_orders: Dict[str, List[dict]]):
        """
        Batch обновление поля shipped_orders в таблице hanging_supplies.
        
        Args:
            grouped_orders: Заказы, сгруппированные по поставкам
        """
        timestamp = datetime.utcnow().isoformat()
        update_data = self._prepare_shipment_data(grouped_orders, timestamp)
        await self._execute_batch_update(update_data)

    def _group_orders_by_supply(self, selected_orders: List[dict]) -> Tuple[Dict[str, dict], Dict[str, str]]:
        """Группирует заказы по поставкам и создает маппинг заказов."""
        supply_orders = defaultdict(lambda: {"order_ids": [], "account": None})
        order_wild_map = {}

        for order in selected_orders:
            supply_id = order["supply_id"]
            supply_orders[supply_id]["order_ids"].append(order["order_id"])
            supply_orders[supply_id]["account"] = order["account"]
            order_wild_map[str(order["order_id"])] = order["article"]

        return dict(supply_orders), order_wild_map

    def _build_delivery_supplies(self, supply_orders: Dict[str, dict]) -> List[DeliverySupplyInfo]:
        """Создает объекты DeliverySupplyInfo из группированных заказов."""
        return [
            DeliverySupplyInfo(
                supply_id=supply_id,
                account=supply_data["account"],
                order_ids=supply_data["order_ids"]
            )
            for supply_id, supply_data in supply_orders.items()
        ]

    def prepare_data_for_delivery_optimized(self, selected_orders: List[dict]) -> Tuple[
        List[DeliverySupplyInfo], Dict[str, str]]:
        """
        Оптимизированная подготовка данных для 1C и отгрузки.
        Args:
            selected_orders: Список отобранных заказов
        Returns:
            Tuple[List[DeliverySupplyInfo], Dict[str, str]]: Данные для доставки и маппинг заказов
        """
        supply_orders, order_wild_map = self._group_orders_by_supply(selected_orders)
        delivery_supplies = self._build_delivery_supplies(supply_orders)

        logger.info(f"Подготовлено {len(delivery_supplies)} поставок для доставки")
        return delivery_supplies, order_wild_map

    def _build_supplies_list(self, grouped_orders: Dict[str, List[dict]]) -> List[SupplyId]:
        """Создает список поставок для генерации QR-кодов."""
        supplies_list = []

        for supply_id, orders in grouped_orders.items():
            if not orders:
                continue

            account = orders[0]["account"]

            orders_list = [
                OrderSchema(
                    order_id=order["order_id"],
                    local_vendor_code=order["article"],
                    nm_id=order["nm_id"]
                )
                for order in orders
            ]

            supplies_list.append(
                SupplyId(
                    name="Тестовая поставка",
                    createdAt="2025-07-19T16:00:00Z",
                    supply_id=supply_id,
                    account=account,
                    count=len(orders_list),
                    orders=orders_list
                )
            )
        return supplies_list

    async def _generate_stickers(self, supplies_list: List[SupplyId]) -> Dict[str, Any]:
        """Генерирует стикеры для списка поставок с частичной проверкой."""
        supply_ids_body = SupplyIdBodySchema(supplies=supplies_list)

        try:
            # Для частичной отгрузки используем allow_partial=True
            await self.check_current_orders(supply_ids_body, allow_partial=True)
            stickers: Dict[str, Dict] = self.group_result(await self.get_stickers(supply_ids_body))
            self.union_results_stickers(supply_ids_body, stickers)
            result = await self.group_orders_to_wild(supply_ids_body)
            logger.info(f"Успешно сгенерированы QR-коды для {len(supplies_list)} поставок")
            return result
        except Exception as e:
            logger.error(f"Ошибка при генерации QR-кодов: {str(e)}")
            raise HTTPException(status_code=500, detail="Ошибка при генерации QR-кодов")

    async def generate_qr_codes_for_selected_orders(self, grouped_orders: Dict[str, List[dict]]) -> Dict[str, Any]:
        """
        Генерирует QR-коды для отобранных заказов.
        Args:
            grouped_orders: Заказы, сгруппированные по поставкам
        Returns:
            Dict[str, Any]: Сгруппированные данные со стикерами для печати
        """
        supplies_list = self._build_supplies_list(grouped_orders)

        if not supplies_list:
            logger.warning("Нет данных для генерации QR-кодов")
            return {}

        return await self._generate_stickers(supplies_list)

    async def _validate_and_get_data(self, supply_data: SupplyIdWithShippedBodySchema) -> Tuple[str, List[dict]]:
        """Валидирует входные данные и получает доступные заказы из БД."""
        target_article = self.validate_unique_vendor_code(supply_data.supplies)
        logger.info(f"Валидация пройдена, артикул: {target_article}")

        hanging_data = await self.get_hanging_supplies_order_data_optimized(supply_data.supplies)
        if not hanging_data:
            raise HTTPException(status_code=404, detail="Не найдено данных о висячих поставках")

        all_orders = self.extract_available_orders(hanging_data, supply_data.supplies)
        if len(all_orders) < supply_data.shipped_count:
            raise HTTPException(
                status_code=400,
                detail=f"Недостаточно доступных заказов для отгрузки. Доступно: {len(all_orders)}, запрошено: {supply_data.shipped_count}"
            )

        return target_article, all_orders

    def _select_and_group_orders(self, all_orders: List[dict], shipped_count: int) -> Tuple[
        List[dict], Dict[str, List[dict]]]:
        """Выбирает N заказов и группирует их по поставкам."""
        selected_orders = all_orders[:shipped_count]
        logger.info(f"Отобрано {len(selected_orders)} заказов для отгрузки")

        grouped_orders = self.group_selected_orders_by_supply(selected_orders)
        return selected_orders, grouped_orders

    async def _process_shipment(self, grouped_orders: Dict[str, List[dict]],
                                delivery_supplies: List[DeliverySupplyInfo],
                                order_wild_map: Dict[str, str],
                                user: dict) -> Tuple[Dict, bool]:
        """Обрабатывает интеграцию с 1C и сохранение отгрузок."""
        await self.update_hanging_supplies_shipped_orders_batch(grouped_orders)

        integration = OneCIntegration()
        integration_result = await integration.format_delivery_data(delivery_supplies, order_wild_map)
        integration_success = isinstance(integration_result, dict) and integration_result.get("status_code") == 200

        if not integration_success:
            logger.error(f"Ошибка интеграции с 1C: {integration_result}")

        shipment_result = await self.save_shipments(delivery_supplies, order_wild_map, user.get('username', 'unknown'))

        return integration_result, integration_success and shipment_result

    def _build_response(self, selected_orders: List[dict], grouped_orders: Dict[str, List[dict]],
                        target_article: str, shipped_count: int, user: dict,
                        qr_codes: List[Any], integration_result: Dict, success: bool) -> Dict[str, Any]:
        """Формирует итоговый ответ."""
        return {
            "success": success,
            "message": "Отгрузка фактического количества выполнена успешно" if success else "Операция выполнена с ошибками",
            "processed_orders": len(selected_orders),
            "processed_supplies": len(grouped_orders),
            "target_article": target_article,
            "shipped_count": shipped_count,
            "operator": user.get('username', 'unknown'),
            "qr_codes": qr_codes,
            "integration_result": integration_result,
            "shipment_result": success
        }

    @staticmethod
    def _get_images(qr_codes: Dict[str, Any]) -> str:
        """Вертикальное объединение QR-кодов с разделителем 5мм."""
        individual_files = [item["file"] for items in qr_codes.values() for item in items if "file" in item]
        if not individual_files:
            return ""
        
        try:
            # Конвертируем все в байты
            image_bytes = []
            for img_data in individual_files:
                if isinstance(img_data, str):
                    # base64 строка - декодируем
                    image_bytes.append(base64.b64decode(img_data))
                else:
                    # уже байты
                    image_bytes.append(img_data)
            
            # Открываем изображения
            images = [Image.open(io.BytesIO(img_byte)) for img_byte in image_bytes]
            
            # Размеры (предполагаем что все изображения одинакового размера)
            width = images[0].width
            height = images[0].height
            
            # Конвертируем 5мм в пиксели (используем стандартное разрешение 72 DPI)
            # 5мм = 5 * 72 / 25.4 ≈ 14.17 пикселей
            separator_height = int(5 * 72 / 25.4)
            
            # Создаем объединенное изображение с учетом разделителей
            total_height = height * len(images) + separator_height * (len(images) - 1)
            combined = Image.new('RGB', (width, total_height), 'white')
            
            # Размещаем изображения друг за другом вертикально с разделителями
            current_y = 0
            for i, img in enumerate(images):
                combined.paste(img, (0, current_y))
                current_y += height
                # Добавляем разделитель после каждого изображения кроме последнего
                if i < len(images) - 1:
                    current_y += separator_height
            
            # Сохраняем в байты и конвертируем в base64
            output = io.BytesIO()
            combined.save(output, format='PNG')
            result_bytes = output.getvalue()
            result_base64 = base64.b64encode(result_bytes).decode('utf-8')
            
            # Очищаем память
            for img in images:
                img.close()
            combined.close()
            output.close()
            
            return result_base64
            
        except Exception as e:
            logger.error(f"Ошибка объединения QR-кодов: {e}")
            return ""

    async def shipment_hanging_actual_quantity_implementation(self,
                                                              supply_data: SupplyIdWithShippedBodySchema,
                                                              user: dict) -> Dict[str, Any]:
        """
        Оптимизированная отгрузка фактического количества из висячих поставок.
        Args:
            supply_data: Данные о поставках с количеством для отгрузки
            user: Данные пользователя
        Returns:
            Dict[str, Any]: Результат операции со статистикой и QR-кодами
        """
        logger.info(f"Начало обработки отгрузки фактического количества: {supply_data.shipped_count} заказов")

        try:
            target_article, all_orders = await self._validate_and_get_data(supply_data)
            selected_orders, grouped_orders = self._select_and_group_orders(all_orders, supply_data.shipped_count)
            delivery_supplies, order_wild_map = self.prepare_data_for_delivery_optimized(selected_orders)

            integration_result, success = await self._process_shipment(grouped_orders, delivery_supplies,
                                                                       order_wild_map, user)
            qr_codes = await self.generate_qr_codes_for_selected_orders(grouped_orders)
            all_files = self._get_images(qr_codes)
            response_data = self._build_response(selected_orders, grouped_orders, target_article,
                                                 supply_data.shipped_count, user, all_files, integration_result,
                                                 success)

            logger.info(f"Отгрузка фактического количества завершена: {len(selected_orders)} заказов")
            return response_data

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отгрузке фактического количества: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")
