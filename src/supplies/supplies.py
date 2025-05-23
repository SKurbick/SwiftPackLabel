import asyncio
from typing import List, Dict, Any, Set, Optional

from src.settings import settings
from src.logger import app_logger as logger
from src.utils import get_wb_tokens, process_local_vendor_code
from src.wildberries_api.supplies import Supplies
from src.wildberries_api.orders import Orders
from src.db import AsyncGenerator
from src.models.card_data import CardData
from src.models.shipment_of_goods import ShipmentOfGoods
from fastapi import HTTPException

from src.supplies.schema import (
    SupplyIdResponseSchema, SupplyIdBodySchema, OrderSchema, StickerSchema, SupplyId,
    SupplyDeleteBody, SupplyDeleteResponse, SupplyDeleteItem, WildFilterRequest, DeliverySupplyInfo
)


class SuppliesService:

    def __init__(self, db: AsyncGenerator = None):
        self.db = db

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

    async def get_list_supplies(self) -> SupplyIdResponseSchema:
        logger.info("Получение данных о поставках, инициализация")
        supplies_ids: List[Any] = await self.get_information_to_supplies()
        supplies: Dict[str, Dict] = self.group_result(await self.get_information_orders_to_supplies(supplies_ids))
        result: List = []
        supplies_ids: Dict[str, List] = {key: value for d in supplies_ids for key, value in d.items()}
        for account, value in supplies.items():
            for supply_id, orders in value.items():
                supply: Dict[str, Dict[str, Any]] = {data["id"]: {"name": data["name"], "createdAt": data['createdAt']}
                                                     for data in supplies_ids[account] if not data['done']}
                result.append(self.create_supply_result(supply, supply_id, account, orders))
        return SupplyIdResponseSchema(supplies=result)

    async def check_current_orders(self, supply_ids: SupplyIdBodySchema):
        logger.info("Проверка поставок на соответствие наличия заказов (сверка заказов по поставкам)")
        tasks: List = []
        for supply in supply_ids.supplies:
            tasks.append(Supplies(supply.account, get_wb_tokens()[supply.account]).get_supply_orders(supply.supply_id))
        result: Dict[str, Dict] = self.group_result(await asyncio.gather(*tasks))
        for supply in supply_ids.supplies:
            supply_orders: Set[int] = {order.order_id for order in supply.orders}
            check_orders: Set[int] = {order.get("id") for order in
                                      result[supply.account][supply.supply_id].get("orders", [])}
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
        Подготавливает данные для записи в таблицу shipment_of_goods.

        Args:
            supply_ids: Список поставок для перевода в статус доставки
            order_wild_map: Соответствие заказов и артикулов wild
            author: Имя автора отгрузки
            warehouse_id: ID склада (по умолчанию 1)
            delivery_type: Тип доставки (по умолчанию "ФБС")

        Returns:
            List[Dict[str, Any]]: Список данных для записи в таблицу shipment_of_goods
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
                             delivery_type: str = "ФБС") -> Dict[str, Any]:
        """
        Сохраняет данные об отгрузках в таблицу shipment_of_goods.
        """
        logger.info(f"Сохранение данных об отгрузках: {len(supply_ids)} поставок")

        shipment_data = await self.prepare_shipment_data(
            supply_ids, order_wild_map, author, warehouse_id, delivery_type
        )
        shipment_repository = ShipmentOfGoods(self.db)

        success = await shipment_repository.create_all(shipment_data)

        return success