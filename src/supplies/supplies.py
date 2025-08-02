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

    async def _prepare_and_execute_fetch(self, request_data, wb_tokens: dict) -> Tuple[List, List]:
        """
        Подготавливает задачи и выполняет параллельные запросы к WB API.
        
        Args:
            request_data: Данные запроса
            wb_tokens: Токены WB для аккаунтов
            
        Returns:
            Tuple[List, List]: (results, task_metadata)
        """
        # Подготовка задач для параллельного выполнения
        tasks = []
        task_metadata = []
        
        for wild_code, wild_item in request_data.orders.items():
            for supply_item in wild_item.supplies:
                account = supply_item.account
                
                if account not in wb_tokens:
                    logger.error(f"Токен для аккаунта {account} не найден")
                    continue
                
                # Создаем задачу для получения заказов
                supplies_api = Supplies(account, wb_tokens[account])
                task = supplies_api.get_supply_orders(supply_item.supply_id)
                tasks.append(task)
                task_metadata.append((wild_code, account, supply_item.supply_id))
        
        # Параллельное выполнение всех запросов
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return results, task_metadata

    def _process_fetch_results(self, results: List, task_metadata: List) -> Dict[Tuple[str, str], List[dict]]:
        """
        Обрабатывает результаты запросов и фильтрует заказы по артикулу.
        
        Args:
            results: Результаты выполнения запросов
            task_metadata: Метаданные задач
            
        Returns:
            Dict[Tuple[str, str], List[dict]]: Заказы по ключу (wild_code, account)
        """
        orders_by_wild_account = {}
        
        for (wild_code, account, supply_id), result in zip(task_metadata, results):
            if isinstance(result, Exception):
                logger.error(f"Ошибка получения заказов для {wild_code}, {account}: {str(result)}")
                continue
                
            try:
                if account in result and supply_id in result[account]:
                    all_orders = result[account][supply_id]['orders']
                    
                    # Фильтруем заказы по артикулу и добавляем метаданные
                    filtered_orders = []
                    for order in all_orders:
                        order_article = process_local_vendor_code(order.get('article', ''))
                        
                        if order_article == wild_code:
                            enriched_order = {
                                **order,
                                'wild_code': wild_code,
                                'account': account,
                                'original_supply_id': supply_id,
                                'timestamp': datetime.fromisoformat(
                                    order['createdAt'].replace('Z', '+00:00')
                                ).timestamp()
                            }
                            filtered_orders.append(enriched_order)
                    
                    key = (wild_code, account)
                    if key not in orders_by_wild_account:
                        orders_by_wild_account[key] = []
                    orders_by_wild_account[key].extend(filtered_orders)
                    
                    logger.info(f"Получено {len(filtered_orders)} заказов с артикулом {wild_code} из кабинета {account}")
                    
            except Exception as e:
                logger.error(f"Ошибка обработки результата для {wild_code}, {account}: {str(e)}")
        
        return orders_by_wild_account

    async def _fetch_orders_from_supplies(self, request_data, wb_tokens: dict) -> Dict[Tuple[str, str], List[dict]]:
        """
        Получает все заказы из исходных поставок параллельно.
        
        Args:
            request_data: Данные запроса
            wb_tokens: Токены WB для аккаунтов
            
        Returns:
            Dict[Tuple[str, str], List[dict]]: Заказы по ключу (wild_code, account)
        """
        results, task_metadata = await self._prepare_and_execute_fetch(request_data, wb_tokens)
        return self._process_fetch_results(results, task_metadata)

    def _select_orders_for_move(self, request_data, orders_by_wild_account: Dict[Tuple[str, str], List[dict]]) -> Tuple[List[dict], Set[Tuple[str, str]]]:
        """
        Отбирает заказы для перемещения по времени создания.
        
        Args:
            request_data: Данные запроса
            orders_by_wild_account: Заказы по ключу (wild_code, account)
            
        Returns:
            Tuple: (selected_orders_for_move, participating_combinations)
        """
        selected_orders_for_move = []
        participating_combinations = set()
        
        for wild_code, wild_item in request_data.orders.items():
            # Собираем все заказы для данного wild_code из всех аккаунтов
            wild_orders = []
            for account in set(supply_item.account for supply_item in wild_item.supplies):
                key = (wild_code, account)
                if key in orders_by_wild_account:
                    wild_orders.extend(orders_by_wild_account[key])
            
            # Сортировка по времени создания (новейшие первые, согласно вашему изменению)
            wild_orders.sort(key=lambda x: -x['timestamp'])
            
            # Выбор первых remove_count заказов
            selected_count = min(wild_item.remove_count, len(wild_orders))
            selected_orders = wild_orders[:selected_count]
            
            # Добавляем в список для перемещения
            selected_orders_for_move.extend(selected_orders)
            
            # Запоминаем какие комбинации (wild_code, account) реально участвуют
            for order in selected_orders:
                participating_combinations.add((order['wild_code'], order['account']))
            
            logger.info(f"Wild {wild_code}: отобрано {len(selected_orders)} из {len(wild_orders)} заказов")
        
        return selected_orders_for_move, participating_combinations

    async def _prepare_and_execute_create_supplies(self, participating_combinations: Set[Tuple[str, str]], wb_tokens: dict) -> Tuple[List, List]:
        """
        Подготавливает задачи и выполняет параллельное создание поставок в WB API.
        
        Args:
            participating_combinations: Комбинации (wild_code, account)
            wb_tokens: Токены WB для аккаунтов
            
        Returns:
            Tuple[List, List]: (results, task_metadata)
        """
        # Подготовка задач для параллельного создания поставок
        tasks = []
        task_metadata = []
        
        for wild_code, account in participating_combinations:
            supply_full_name = f"Висячая_FBS_{wild_code}_{datetime.now().strftime('%d.%m.%Y_%H:%M')}_{account}"
            supplies_api = Supplies(account, wb_tokens[account])
            task = supplies_api.create_supply(supply_full_name)
            tasks.append(task)
            task_metadata.append((wild_code, account))
        
        # Параллельное выполнение всех запросов
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return results, task_metadata

    async def _process_create_supplies_results(self, results: List, task_metadata: List, user: dict) -> Dict[Tuple[str, str], str]:
        """
        Обрабатывает результаты создания поставок и формирует маппинг.
        
        Args:
            results: Результаты выполнения запросов на создание поставок
            task_metadata: Метаданные задач
            user: Данные пользователя для указания оператора
            
        Returns:
            Dict[Tuple[str, str], str]: Новые поставки по ключу (wild_code, account)
        """
        new_supplies = {}
        
        for (wild_code, account), result in zip(task_metadata, results):
            if isinstance(result, Exception):
                logger.error(f"Исключение при создании поставки для {wild_code}, {account}: {str(result)}")
                continue
                
            try:
                if 'id' in result:
                    new_supply_id = result['id']
                    new_supplies[(wild_code, account)] = new_supply_id
                    logger.info(f"Создана поставка {new_supply_id} для {wild_code} в кабинете {account}")
                    
                    # Сохраняем как висячую поставку в БД
                    await self._save_as_hanging_supply(new_supply_id, account, wild_code, user)
                else:
                    logger.error(f"Ошибка создания поставки для {wild_code}, {account}: {result}")
            except Exception as e:
                logger.error(f"Ошибка обработки результата создания поставки для {wild_code}, {account}: {str(e)}")
        
        return new_supplies

    async def _save_as_hanging_supply(self, supply_id: str, account: str, wild_code: str, user: dict):
        """
        Сохраняет созданную поставку как висячую в БД.
        
        Args:
            supply_id: ID созданной поставки
            account: Аккаунт Wildberries
            wild_code: Артикул (wild) для которого создана поставка
            user: Данные пользователя для указания оператора
        """
        try:
            hanging_supplies = HangingSupplies(self.db)
            order_data = {
                "orders": [], 
                "wild_code": wild_code, 
                "created_for_move": True,
                "created_at": datetime.utcnow().isoformat()
            }
            order_data_json = json.dumps(order_data)
            operator = user.get('username', 'move_orders_system')
            
            await hanging_supplies.save_hanging_supply(supply_id, account, order_data_json, operator)
            logger.info(f"Сохранена висячая поставка {supply_id} для {wild_code} в аккаунте {account}, оператор: {operator}")
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении висячей поставки {supply_id} для {wild_code} в аккаунте {account}: {str(e)}")

    async def _create_new_supplies(self, participating_combinations: Set[Tuple[str, str]], wb_tokens: dict, user: dict) -> Dict[Tuple[str, str], str]:
        """
        Создает новые поставки для участвующих комбинаций параллельно.
        
        Args:
            participating_combinations: Комбинации (wild_code, account)
            wb_tokens: Токены WB для аккаунтов
            
        Returns:
            Dict[Tuple[str, str], str]: Новые поставки по ключу (wild_code, account)
        """
        results, task_metadata = await self._prepare_and_execute_create_supplies(participating_combinations, wb_tokens)
        return await self._process_create_supplies_results(results, task_metadata, user)

    async def _move_orders_to_supplies(self, selected_orders_for_move: List[dict], new_supplies: Dict[Tuple[str, str], str], wb_tokens: dict) -> List[int]:
        """
        Перемещает отобранные заказы в новые поставки параллельно.
        
        Args:
            selected_orders_for_move: Отобранные заказы для перемещения
            new_supplies: Новые поставки по ключу (wild_code, account)
            wb_tokens: Токены WB для аккаунтов
            
        Returns:
            List[int]: ID успешно перемещенных заказов
        """
        # Подготовка задач для параллельного перемещения
        tasks = []
        task_metadata = []
        
        for order in selected_orders_for_move:
            wild_code = order['wild_code']
            account = order['account']
            order_id = order['id']
            
            # Находим новую поставку для этой комбинации
            new_supply_id = new_supplies.get((wild_code, account))
            if not new_supply_id:
                logger.warning(f"Не найдена новая поставка для {wild_code}, {account}")
                continue
            
            # Создаем задачу для добавления заказа в поставку
            supplies_api = Supplies(account, wb_tokens[account])
            task = supplies_api.add_order_to_supply(new_supply_id, order_id)
            tasks.append(task)
            task_metadata.append((order_id, order['original_supply_id'], new_supply_id))
        
        # Параллельное выполнение всех запросов
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обработка результатов
        moved_order_ids = []
        
        for (order_id, original_supply_id, new_supply_id), result in zip(task_metadata, results):
            if isinstance(result, Exception):
                logger.error(f"Исключение при перемещении заказа {order_id}: {str(result)}")
                continue
                
            # Добавляем все заказы в список перемещенных
            moved_order_ids.append(order_id)
            logger.info(f"Заказ {order_id} перемещен из {original_supply_id} в {new_supply_id}")
        
        return moved_order_ids

    async def move_orders_between_supplies_implementation(self, request_data, user: dict) -> Dict[str, Any]:
        """
        Перемещение заказов между поставками.
        
        Args:
            request_data: Данные запроса с заказами для перемещения
            user: Данные пользователя
            
        Returns:
            Dict[str, Any]: Результат операции перемещения
        """
        logger.info(f"Начало перемещения заказов от пользователя {user.get('username', 'unknown')}")

        # 1. Получение токенов WB
        wb_tokens = get_wb_tokens()

        # 2. Получаем данные о всех заказах из исходных поставок (параллельно)
        orders_by_wild_account = await self._fetch_orders_from_supplies(request_data, wb_tokens)

        # 3. Отбираем заказы для перемещения по времени создания
        selected_orders_for_move, participating_combinations = self._select_orders_for_move(request_data, orders_by_wild_account)

        # 4. Проверяем что есть заказы для перемещения
        if not selected_orders_for_move:
            return {
                "success": False,
                "message": "Не найдено заказов для перемещения",
                "removed_order_ids": [],
                "processed_supplies": 0,
                "processed_wilds": 0
            }

        logger.info(f"Всего отобрано {len(selected_orders_for_move)} заказов для перемещения")
        logger.info(f"Участвующие комбинации (wild, account): {participating_combinations}")

        # 5. Создаем поставки для участвующих комбинаций (параллельно)
        new_supplies = await self._create_new_supplies(participating_combinations, wb_tokens, user)

        # 6. Если не удалось создать поставки
        if not new_supplies:
            raise HTTPException(status_code=500, detail="Не удалось создать поставки для перемещения")

        logger.info(f"Успешно создано {len(new_supplies)} поставок")

        # 7. Перемещаем отобранные заказы в новые поставки (параллельно)
        moved_order_ids = await self._move_orders_to_supplies(selected_orders_for_move, new_supplies, wb_tokens)

        # 8. Возврат результата
        logger.info(f"Перемещение завершено. Успешно перемещено {len(moved_order_ids)} заказов")

        return {
            "success": True,
            "message": f"Операция перемещения выполнена. Перемещено {len(moved_order_ids)} заказов",
            "removed_order_ids": moved_order_ids,
            "processed_supplies": len(new_supplies),
            "processed_wilds": len({order['wild_code'] for order in selected_orders_for_move}),}

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
                                user: dict,
                                skip_shipment_api: bool = False) -> Tuple[Dict, bool]:
        """
        Обрабатывает интеграцию с 1C и сохранение отгрузок.
        
        Args:
            grouped_orders: Заказы, сгруппированные по поставкам
            delivery_supplies: Данные поставок для доставки
            order_wild_map: Соответствие заказов и артикулов
            user: Данные пользователя
            skip_shipment_api: Если True, пропускает отправку в shipment API (для висячих поставок)
        """
        await self.update_hanging_supplies_shipped_orders_batch(grouped_orders)

        integration = OneCIntegration()
        integration_result = await integration.format_delivery_data(delivery_supplies, order_wild_map)
        integration_success = isinstance(integration_result, dict) and integration_result.get("status_code") == 200

        if not integration_success:
            logger.error(f"Ошибка интеграции с 1C: {integration_result}")

        if not skip_shipment_api:
            shipment_result = await self.save_shipments(delivery_supplies, order_wild_map, user.get('username', 'unknown'))
        else:
            shipment_result = True  # Считаем успешным, так как данные уже отправлены в shipment API
            logger.info("Пропуск отправки в shipment API - данные уже отправлены через _send_enhanced_shipment_data")

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
        Отгрузка фактического количества из висячих поставок с созданием новых поставок.
        Args:
            supply_data: Данные о поставках с количеством для отгрузки
            user: Данные пользователя
        Returns:
            Dict[str, Any]: Результат операции со статистикой
        """
        logger.info(f"Начало обработки отгрузки фактического количества: {supply_data.shipped_count} заказов")

        try:
            target_article, all_orders = await self._validate_and_get_data(supply_data)
            selected_orders, grouped_orders = self._select_and_group_orders(all_orders, supply_data.shipped_count)
            
            # 1. Создаем новые поставки и перемещаем заказы
            new_supplies_map = await self._create_and_transfer_orders(selected_orders, target_article, user)
            
            # 2. Переводим новые поставки в статус доставки
            await self._deliver_new_supplies(new_supplies_map)
            
            # 3. Обновляем данные заказов с новыми supply_id
            updated_selected_orders = self._update_orders_with_new_supplies(selected_orders, new_supplies_map)
            updated_grouped_orders = self.group_selected_orders_by_supply(updated_selected_orders)
            
            # 4. Подготавливаем данные для 1C и shipment_goods
            delivery_supplies, order_wild_map = self.prepare_data_for_delivery_optimized(updated_selected_orders)
            
            # 5. Обновляем висячие поставки и получаем product_reserves_id
            shipped_goods_response = await self._update_hanging_supplies_shipped_quantities(grouped_orders)
            
            # 6. Отправляем данные в shipment API с product_reserves_id
            logger.info(f"Отправка данных в shipment API с product_reserves_id и автором '{user.get('username', 'unknown')}'")
            await self._send_enhanced_shipment_data(updated_selected_orders, shipped_goods_response, user)
            
            # 7. Отправляем в 1C (БЕЗ повторной отправки в shipment API)
            integration_result, success = await self._process_shipment(updated_grouped_orders, delivery_supplies,
                                                                       order_wild_map, user, skip_shipment_api=True)
            
            # 8. Генерируем PDF со стикерами для новых поставок
            pdf_stickers = await self._generate_pdf_stickers_for_new_supplies(new_supplies_map, target_article, updated_selected_orders)
            
            response_data = {
                "success": success,
                "message": "Отгрузка фактического количества выполнена успешно" if success else "Операция выполнена с ошибками",
                "processed_orders": len(updated_selected_orders),
                "processed_supplies": len(updated_grouped_orders),
                "target_article": target_article,
                "shipped_count": supply_data.shipped_count,
                "operator": user.get('username', 'unknown'),
                "qr_codes": pdf_stickers,
                "integration_result": integration_result,
                "shipment_result": success,
                "new_supplies": list(new_supplies_map.values())
            }

            logger.info(f"Отгрузка фактического количества завершена: {len(selected_orders)} заказов, "
                       f"создано {len(new_supplies_map)} новых поставок")
            return response_data

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отгрузке фактического количества: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")

    async def _create_and_transfer_orders(self, selected_orders: List[dict], target_article: str, user: dict) -> Dict[str, str]:
        """
        Создает новые поставки и перемещает в них заказы.
        Возвращает маппинг account -> new_supply_id
        """
        logger.info(f"Создание новых поставок для артикула {target_article}")
        
        # Группируем заказы по аккаунтам
        orders_by_account = defaultdict(list)
        for order in selected_orders:
            account = order["account"]
            orders_by_account[account].append(order)
        
        new_supplies_map = {}
        wb_tokens = get_wb_tokens()
        
        for account, orders in orders_by_account.items():
            # Создаем имя поставки
            timestamp = datetime.now().strftime("%d.%m.%Y_%H:%M")
            supply_name = f"Факт_{target_article}_{timestamp}_{user.get('username', 'auto')}"
            
            logger.info(f"Создание поставки '{supply_name}' для аккаунта {account} с {len(orders)} заказами")
            
            # Создаем поставку
            supplies_api = Supplies(account, wb_tokens[account])
            create_response = await supplies_api.create_supply(supply_name)
            
            if create_response.get("errors"):
                raise HTTPException(status_code=500, detail=f"Ошибка создания поставки для {account}: {create_response['errors']}")
                
            new_supply_id = create_response.get("id")
            if not new_supply_id:
                raise HTTPException(status_code=500, detail=f"Не получен ID новой поставки для аккаунта {account}")
            
            logger.info(f"Создана поставка {new_supply_id} для аккаунта {account}")
            
            # Перемещаем заказы
            for order in orders:
                order_id = order["order_id"]
                transfer_response = await supplies_api.add_order_to_supply(new_supply_id, order_id)

                logger.debug(f"Заказ {order_id} перемещен в поставку {new_supply_id}")
            
            new_supplies_map[account] = new_supply_id
        
        return new_supplies_map

    async def _deliver_new_supplies(self, new_supplies_map: Dict[str, str]):
        """
        Переводит новые поставки в статус доставки.
        """
        logger.info(f"Перевод {len(new_supplies_map)} новых поставок в статус доставки")
        
        wb_tokens = get_wb_tokens()
        
        for account, supply_id in new_supplies_map.items():
            supplies_api = Supplies(account, wb_tokens[account])
            await supplies_api.deliver_supply(supply_id)
            
            logger.info(f"Поставка {supply_id} переведена в статус доставки")

    def _update_orders_with_new_supplies(self, selected_orders: List[dict], new_supplies_map: Dict[str, str]) -> List[dict]:
        """
        Обновляет заказы с новыми supply_id и сохраняет исходную висячую поставку.
        """
        updated_orders = []
        
        for order in selected_orders:
            account = order["account"]
            if account in new_supplies_map:
                updated_order = order.copy()
                # Сохраняем исходную висячую поставку перед заменой
                updated_order["original_hanging_supply_id"] = updated_order.get("supply_id")
                # Обновляем supply_id на новую поставку
                updated_order["supply_id"] = new_supplies_map[account]
                updated_orders.append(updated_order)
            else:
                updated_orders.append(order)  # Fallback
        
        return updated_orders

    async def _update_hanging_supplies_shipped_quantities(self, grouped_orders: Dict[str, List[dict]]) -> List[Dict[str, Any]]:
        """
        Отправляет данные об отгруженных количествах для висячих поставок в API add_shipped_goods.
        
        Args:
            grouped_orders: Заказы, сгруппированные по исходным висячим поставкам
            
        Returns:
            List[Dict[str, Any]]: Ответ от API с product_reserves_id для каждой поставки
        """
        logger.info(f"Отправка данных об отгруженных количествах для {len(grouped_orders)} висячих поставок")
        
        shipped_goods_data = self._prepare_shipped_goods_data(grouped_orders)
        
        if not shipped_goods_data:
            logger.warning("Нет данных для отправки в API add_shipped_goods")
            return []
        
        return await self._send_shipped_goods_to_api(shipped_goods_data)
    
    def _prepare_shipped_goods_data(self, grouped_orders: Dict[str, List[dict]]) -> List[Dict[str, Any]]:
        """
        Подготавливает данные об отгруженных количествах для API.
        
        Args:
            grouped_orders: Заказы, сгруппированные по исходным висячим поставкам
            
        Returns:
            List[Dict[str, Any]]: Подготовленные данные для API
        """
        shipped_goods_data = []
        
        for supply_id, orders in grouped_orders.items():
            if not orders:
                continue
                
            quantity_shipped = len(orders)
            
            shipped_goods_item = {
                "supply_id": supply_id,
                "quantity_shipped": quantity_shipped
            }
            
            shipped_goods_data.append(shipped_goods_item)
            logger.debug(f"Подготовлены данные для поставки {supply_id}: отгружено {quantity_shipped} заказов")
        
        return shipped_goods_data
    
    async def _send_shipped_goods_to_api(self, shipped_goods_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Отправляет данные об отгруженных количествах в API.
        
        Args:
            shipped_goods_data: Подготовленные данные для отправки в API
            
        Returns:
            List[Dict[str, Any]]: Ответ от API с product_reserves_id для каждой поставки
        """
        try:
            api_url = settings.SHIPPED_GOODS_API_URL

            logger.info(f"Отправка запроса на URL: {api_url}")
            logger.debug(f"Данные для отправки: {json.dumps(shipped_goods_data, ensure_ascii=False, indent=2)}")



            response = await self.async_client.post(
                url=api_url,
                json=shipped_goods_data,
                headers={"Content-Type": "application/json"}
            )

            if response:
                logger.info(f"Успешная отправка данных об отгруженных количествах. Ответ: {response}")
                # Ожидаем ответ в формате: [{"supply_id": "string", "product_reserves_id": 0}]
                try:
                    response_data = json.loads(response) if isinstance(response, str) else response
                    if isinstance(response_data, list):
                        return response_data
                    logger.error(f"Неожиданный формат ответа от API add_shipped_goods: {response_data}")
                    return []
                except (json.JSONDecodeError, TypeError) as e:
                    logger.error(f"Ошибка парсинга ответа от API add_shipped_goods: {e}")
                    return []
            else:
                logger.error("Получен пустой ответ от API add_shipped_goods")
                return []

        except Exception as e:
            logger.error(f"Ошибка при отправке данных об отгруженных количествах: {str(e)}")
            # Не пробрасываем исключение, так как это не критично для основного процесса
            return []

    async def _send_enhanced_shipment_data(self, updated_selected_orders: List[dict], 
                                         shipped_goods_response: List[Dict[str, Any]], 
                                         user: dict) -> None:
        """
        Отправляет данные об отгрузке в API с добавлением product_reserves_id из ответа shipped_goods API.
        
        Args:
            updated_selected_orders: Обновленные заказы с новыми supply_id
            shipped_goods_response: Ответ от API add_shipped_goods с product_reserves_id
            user: Данные пользователя для определения автора
        """
        logger.info(f"Отправка расширенных данных об отгрузке для {len(updated_selected_orders)} заказов")
        
        reserves_mapping = self._create_reserves_mapping(shipped_goods_response)
        delivery_supplies, order_wild_map = self._prepare_delivery_data(updated_selected_orders)
        shipment_data = await self._get_base_shipment_data(delivery_supplies, order_wild_map, user)
        enhanced_shipment_data = self._enhance_with_reserves(shipment_data, updated_selected_orders, reserves_mapping)
        await self._filter_and_send_shipment_data(enhanced_shipment_data)
    
    def _create_reserves_mapping(self, shipped_goods_response: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Создает маппинг supply_id -> product_reserves_id из ответа shipped_goods API.
        
        Args:
            shipped_goods_response: Ответ от API add_shipped_goods
            
        Returns:
            Dict[str, int]: Маппинг supply_id -> product_reserves_id
        """
        reserves_mapping = {}
        for item in shipped_goods_response:
            if isinstance(item, dict) and 'supply_id' in item and 'product_reserves_id' in item:
                reserves_mapping[item['supply_id']] = item['product_reserves_id']
        
        logger.debug(f"Маппинг резервов: {reserves_mapping}")
        return reserves_mapping
    
    def _prepare_delivery_data(self, updated_selected_orders: List[dict]) -> Tuple[List, Dict[str, str]]:
        """
        Подготавливает данные для создания DeliverySupplyInfo объектов.
        
        Args:
            updated_selected_orders: Обновленные заказы с новыми supply_id
            
        Returns:
            Tuple[List, Dict[str, str]]: delivery_supplies и order_wild_map
        """
        delivery_supplies = []
        order_wild_map = {}
        
        # Группируем заказы по supply_id для создания DeliverySupplyInfo
        orders_by_supply = defaultdict(list)
        for order in updated_selected_orders:
            supply_id = order.get("supply_id")
            account = order.get("account")
            orders_by_supply[(supply_id, account)].append(order.get("order_id"))
            # Сохраняем маппинг order_id -> wild для order_wild_map
            order_wild_map[str(order.get("order_id"))] = order.get("article")
        
        # Создаем объекты DeliverySupplyInfo
        for (supply_id, account), order_ids in orders_by_supply.items():
            delivery_supply = type('DeliverySupplyInfo', (), {
                'supply_id': supply_id,
                'account': account,
                'order_ids': order_ids
            })()
            delivery_supplies.append(delivery_supply)
        
        return delivery_supplies, order_wild_map
    
    async def _get_base_shipment_data(self, delivery_supplies: List, order_wild_map: Dict[str, str], user: dict) -> List[Dict[str, Any]]:
        """
        Получает базовые данные для отгрузки через существующий метод prepare_shipment_data.
        
        Args:
            delivery_supplies: Список объектов DeliverySupplyInfo
            order_wild_map: Маппинг order_id -> wild
            user: Данные пользователя для определения автора
            
        Returns:
            List[Dict[str, Any]]: Базовые данные для отгрузки
        """
        return await self.prepare_shipment_data(
            delivery_supplies, 
            order_wild_map, 
            user.get('username', 'unknown'),  # Используем реального пользователя вместо 'system_hanging_shipment'
            warehouse_id=1,
            delivery_type="ФБС"
        )
    
    def _enhance_with_reserves(self, shipment_data: List[Dict[str, Any]], 
                              updated_selected_orders: List[dict], 
                              reserves_mapping: Dict[str, int]) -> List[Dict[str, Any]]:
        """
        Добавляет product_reserves_id к данным отгрузки.
        
        Args:
            shipment_data: Базовые данные для отгрузки
            updated_selected_orders: Обновленные заказы с новыми supply_id
            reserves_mapping: Маппинг supply_id -> product_reserves_id
            
        Returns:
            List[Dict[str, Any]]: Обогащенные данные с product_reserves_id
        """
        enhanced_shipment_data = []
        
        for item in shipment_data:
            enhanced_item = item.copy()
            
            # Ищем соответствующий заказ для получения original_hanging_supply_id
            supply_id = item.get("supply_id")
            matching_order = next(
                (order for order in updated_selected_orders if order.get("supply_id") == supply_id), 
                None
            )
            
            if matching_order:
                original_supply_id = matching_order.get("original_hanging_supply_id")
                if original_supply_id and original_supply_id in reserves_mapping:
                    enhanced_item["product_reserves_id"] = reserves_mapping[original_supply_id]
                    logger.debug(f"Добавлен product_reserves_id={reserves_mapping[original_supply_id]} для supply_id {supply_id}")
            
            enhanced_shipment_data.append(enhanced_item)
        
        return enhanced_shipment_data
    
    async def _filter_and_send_shipment_data(self, enhanced_shipment_data: List[Dict[str, Any]]) -> None:
        """
        Фильтрует и отправляет обогащенные данные отгрузки в API.
        
        Args:
            enhanced_shipment_data: Обогащенные данные с product_reserves_id
        """
        if not enhanced_shipment_data:
            logger.warning("Нет данных для отправки в shipment API")
            return

        shipment_repository = ShipmentOfGoods(self.db)
        filter_wild = await shipment_repository.filter_wilds()
        
        filtered_shipment_data = [item for item in enhanced_shipment_data if item['product_id'] in filter_wild]
        logger.info(f"Отфильтровано записей для висячих: {len(enhanced_shipment_data)} -> {len(filtered_shipment_data)}")
        
        if filtered_shipment_data:
            await self._send_shipment_data_to_api(filtered_shipment_data)
        else:
            logger.warning("Нет данных для отправки в shipment API после фильтрации")

    async def _generate_pdf_stickers_for_new_supplies(self, new_supplies_map: Dict[str, str], target_article: str, 
                                                     updated_selected_orders: List[dict]) -> str:
        """
        Генерирует PDF со стикерами для новых поставок, переиспользуя логику из роутера.
        
        Args:
            new_supplies_map: Маппинг account -> new_supply_id
            target_article: Артикул (wild) для всех заказов
            updated_selected_orders: Обновленные заказы с новыми supply_id
            
        Returns:
            str: Base64 строка PDF файла со стикерами
        """
        logger.info(f'Генерация PDF стикеров для новых поставок с артикулом: {target_article}')
        
        # Группируем заказы по новым поставкам
        supplies_data = defaultdict(list)
        for order in updated_selected_orders:
            supply_id = order["supply_id"]
            account = order["account"]
            
            # Проверяем, что это новая поставка
            if account in new_supplies_map and new_supplies_map[account] == supply_id:
                supplies_data[supply_id].append({
                    "account": account,
                    "order_id": order["order_id"]
                })
        
        if not supplies_data:
            logger.warning("Нет данных для генерации PDF стикеров новых поставок")
            return ""
        
        # Подготавливаем данные в формате WildFilterRequest
        from src.supplies.schema import WildFilterRequest, WildSupplyItem, WildOrderItem
        
        wild_supply_items = []
        for supply_id, orders in supplies_data.items():
            if orders:
                account = orders[0]["account"]
                wild_supply_items.append(
                    WildSupplyItem(
                        account=account,
                        supply_id=supply_id,
                        orders=[WildOrderItem(order_id=order["order_id"]) for order in orders]
                    )
                )
        
        wild_filter = WildFilterRequest(
            wild=target_article,
            supplies=wild_supply_items
        )
        
        # Переиспользуем точно ту же логику что и в роутере generate_stickers_by_wild
        logger.info(f"Генерация PDF стикеров для {len(wild_supply_items)} новых поставок")
        result_stickers = await self.filter_and_fetch_stickers_by_wild(wild_filter)
        
        # Импортируем функцию для создания PDF
        from src.service.service_pdf import collect_images_sticker_to_pdf
        pdf_sticker = await collect_images_sticker_to_pdf(result_stickers)
        
        # Конвертируем PDF в base64 для передачи
        import base64
        pdf_base64 = base64.b64encode(pdf_sticker.getvalue()).decode('utf-8')
        
        logger.info(f"PDF стикеры сгенерированы успешно для артикула {target_article}")
        return pdf_base64
