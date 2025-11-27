import asyncio
import json
import base64
import io
import time
from typing import List, Dict, Any, Set, Optional, Tuple
from datetime import datetime
from collections import defaultdict
from PIL import Image

from io import BytesIO

from src.service.service_pdf import collect_images_sticker_to_pdf
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
from src.models.final_supplies import FinalSupplies
from src.models.delivered_supplies import DeliveredSupplies
from src.models.assembly_task_status import AssemblyTaskStatus
from src.models.qr_scan_db import QRScanDB
from src.response import AsyncHttpClient, parse_json
from fastapi import HTTPException

from src.orders.order_status_service import OrderStatusService
from src.wildberries_api.supplies import Supplies

from src.supplies.schema import (
    SupplyIdResponseSchema, SupplyIdBodySchema, OrderSchema, StickerSchema, SupplyId,
    SupplyDeleteBody, SupplyDeleteResponse, SupplyDeleteItem, WildFilterRequest, DeliverySupplyInfo,
    SupplyIdWithShippedBodySchema
)


class SuppliesService:

    def __init__(self, db: AsyncGenerator = None):
        self.db = db
        self.async_client = AsyncHttpClient(timeout=120, retries=3, delay=5)

    async def get_supply_detailed_info(self, supply_id: str, account: str) -> Optional[Dict[str, Any]]:
        """
        Получает детальную информацию о поставке из WB API.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт WB
            
        Returns:
            Dict с информацией о поставке или None при ошибке
            
        Пример возвращаемых данных:
        {
            "id": "WB-GI-1234567",
            "done": false,
            "createdAt": "2022-05-04T07:56:29Z", 
            "closedAt": null,
            "scanDt": null,
            "name": "Тестовая поставка_ФИНАЛ",
            "cargoType": 0,
            "destinationOfficeId": 123
        }
        """
        try:
            # Получаем токены
            wb_tokens = get_wb_tokens()
            if account not in wb_tokens:
                logger.error(f"Токен для аккаунта {account} не найден")
                return None

            supplies_api = Supplies(account, wb_tokens[account])

            supply_info = await supplies_api.get_information_to_supply(supply_id)
        
            logger.info(f"Получена информация о поставке {supply_id} для аккаунта {account}")
            logger.debug(f"Данные поставки: {supply_info}")

            return supply_info or None
        
        except Exception as e:
            logger.error(f"Ошибка получения информации о поставке {supply_id} для аккаунта {account}: {str(e)}")
            return None


    def convert_current_name_to_final(self, current_name: str) -> str:
        """
        Преобразует текущее название поставки в финальное.
        
        Args:
            current_name: Текущее название поставки
            
        Returns:
            str: Финальное название с суффиксом _ФИНАЛ
            
        Примеры:
            "Основная поставка_ТЕХ" -> "Основная поставка_ФИНАЛ"
            "Простая поставка" -> "Простая поставка_ФИНАЛ"
        """
        if not current_name:
            return "Финальная_поставка_ФИНАЛ"

        clean_name = current_name.strip()

        if clean_name.endswith("_ФИНАЛ"):
            return clean_name  # Уже финальная
        elif clean_name.endswith("_ТЕХ") or clean_name.endswith("_TEX"):
            return f"{clean_name[:-4]}_ФИНАЛ"
        else:
            return f"{clean_name}_ФИНАЛ"

    async def get_current_supply_names_for_accounts(
        self, 
        participating_combinations: Set[Tuple[str, str]], 
        request_data: Any
    ) -> Dict[str, str]:
        """
        Получает текущие названия поставок для аккаунтов из WB API.
        
        Args:
            participating_combinations: Комбинации (wild_code, account)
            request_data: Данные запроса с исходными поставками
            
        Returns:
            Dict[str, str]: Словарь {account: supply_name}
        """
        current_supply_names = {}
        
        try:
            for wild_code, account in participating_combinations:
                if account in current_supply_names:
                    continue  # Уже получили название для этого аккаунта
                    
                # Ищем supply_id для этого аккаунта в request_data
                if wild_code in request_data.orders:
                    wild_item = request_data.orders[wild_code]
                    for supply_item in wild_item.supplies:
                        if supply_item.account == account:
                            supply_info = await self.get_supply_detailed_info(
                                supply_item.supply_id, 
                                account
                            )
                            if supply_info:
                                current_supply_names[account] = supply_info.get("name", f"Поставка_{account}")
                                logger.info(f"Получено название текущей поставки для {account}: {current_supply_names[account]}")
                                break
                
                # Если не нашли название, используем стандартное
                if account not in current_supply_names:
                    current_supply_names[account] = f"Финальная_поставка_{account}"
                    logger.warning(f"Не удалось получить название поставки для {account}, используем стандартное")
        
        except Exception as e:
            logger.error(f"Ошибка получения текущих названий поставок: {str(e)}")
        
        return current_supply_names

    async def _create_new_final_supply(self, account: str, current_name: str) -> Optional[str]:
        """
        Создает новую финальную поставку в WB API и сохраняет в БД.
        
        Args:
            account: Аккаунт WB
            current_name: Текущее название для преобразования
            
        Returns:
            str: ID созданной поставки или None при ошибке
        """
        try:
            # Получаем токены
            wb_tokens = get_wb_tokens()
            if account not in wb_tokens:
                logger.error(f"Токен для аккаунта {account} не найден")
                return None
            
            # Преобразуем название в финальное
            final_name = self.convert_current_name_to_final(current_name)
            
            # Создаем поставку в WB API
            supplies_api = Supplies(account, wb_tokens[account])
            result = await supplies_api.create_supply(final_name)
            
            if not result or 'id' not in result:
                logger.error(f"Не удалось создать финальную поставку для {account}")
                return None
                
            new_supply_id = result['id']
            logger.info(f"Создана новая финальная поставка {new_supply_id} ({final_name}) для {account}")
            
            # Сохраняем в БД final_supplies
            if self.db:
                final_supplies_db = FinalSupplies(self.db)
                await final_supplies_db.save_final_supply(new_supply_id, account, final_name)
            
            return new_supply_id
            
        except Exception as e:
            logger.error(f"Ошибка создания новой финальной поставки для {account}: {str(e)}")
            return None

    async def _create_or_use_final_supplies(
        self, 
        participating_combinations: Set[Tuple[str, str]], 
        wb_tokens: dict, 
        request_data: Any, 
        user: dict
    ) -> Dict[Tuple[str, str], str]:
        """
        Создает или использует существующие финальные поставки.
        
        Args:
            participating_combinations: Комбинации (wild_code, account)
            wb_tokens: Токены WB API
            request_data: Данные запроса
            user: Данные пользователя
            
        Returns:
            Dict[Tuple[str, str], str]: Маппинг комбинаций на supply_id
        """
        # 1. Группируем по аккаунтам
        unique_accounts = {account for _, account in participating_combinations}
        logger.info(f"Обработка финальных поставок для аккаунтов: {unique_accounts}")
        
        # 2. Получаем текущие названия поставок
        current_supply_names = await self.get_current_supply_names_for_accounts(
            participating_combinations, 
            request_data
        )
        
        # 3. Обрабатываем каждый аккаунт
        account_final_supplies = {}  # {account: supply_id}
        
        if self.db:
            final_supplies_db = FinalSupplies(self.db)
            
            for account in unique_accounts:
                current_name = current_supply_names.get(account, f"Финальная_поставка_{account}")
                
                # Ищем последнюю активную финальную поставку
                last_final_supply = await final_supplies_db.get_latest_final_supply(account)
                
                if last_final_supply:
                    logger.info(f"Найдена существующая финальная поставка {last_final_supply['supply_id']} для {account}")
                    
                    # Проверяем статус в WB API
                    wb_status = await self.get_supply_detailed_info(
                        last_final_supply["supply_id"], 
                        account
                    )
                    
                    if wb_status and not wb_status.get("done", True):
                        # Поставка активна - используем её
                        account_final_supplies[account] = last_final_supply["supply_id"]
                        logger.info(f"Используем активную финальную поставку {last_final_supply['supply_id']} для {account}")
                    else:
                        # Поставка неактивна - обновляем статус и создаем новую
                        new_supply_id = await self._create_new_final_supply(account, current_name)
                        if new_supply_id:
                            account_final_supplies[account] = new_supply_id
                else:
                    # Нет существующих финальных поставок - создаем новую
                    logger.info(f"Нет финальных поставок для {account}, создаем первую")
                    new_supply_id = await self._create_new_final_supply(account, current_name)
                    if new_supply_id:
                        account_final_supplies[account] = new_supply_id
        
        # 4. Формируем результат для всех комбинаций
        new_supplies = {}
        for wild_code, account in participating_combinations:
            if account in account_final_supplies:
                new_supplies[(wild_code, account)] = account_final_supplies[account]
                logger.debug(f"Маппинг: ({wild_code}, {account}) -> {account_final_supplies[account]}")
        
        logger.info(f"Финальные поставки подготовлены: {len(new_supplies)} комбинаций -> {len(account_final_supplies)} поставок")
        return new_supplies

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
                "photo_link": name_and_photo.get(order.nm_id, {"photo_link": "НЕТ ФОТО"})["photo_link"],
                "createdAt": order.createdAt}

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
        data = {k: sorted(v, key=lambda x: (x.get('createdAt', ''), x.get('id',
                                                                          0)), reverse=True) for k, v in result.items()}
        return dict(sorted(data.items(), key=lambda x: (min(item['subject_name']
                                                            for item in x[1]), min(item.get('id', 0) for item in x[1]),
                                                        x[0])))

    @staticmethod
    async def get_information_to_supplies() -> List[Dict]:
        logger.info("Получение данных по всем кабинетам о поставках")
        tasks: List = []
        for account, token in get_wb_tokens().items():
            tasks.append(Supplies(account, token).get_supplies_filter_done())
        return await asyncio.gather(*tasks)

    async def get_information_to_supply_details(self, basic_supplies_ids: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Обогащает базовые supply_ids полной информацией из WB API.
        Args:
            basic_supplies_ids: Базовые данные о поставках из БД (только supply_id и account)
        Returns:
            List[Dict[str, Any]]: Полные данные о поставках с информацией из WB API
        """
        logger.info(f"Обогащение {len(basic_supplies_ids)} базовых поставок информацией из WB API")

        if not basic_supplies_ids:
            return []

        enriched_supplies = []
        wb_tokens = get_wb_tokens()

        for account_data in basic_supplies_ids:
            for account, supplies_list in account_data.items():
                # Создаем задачи для параллельного получения информации о поставках
                tasks = []
                for supply_info in supplies_list:
                    if supply_id := supply_info.get('id'):
                        supplies_api = Supplies(account, wb_tokens[account])
                        tasks.append(supplies_api.get_information_to_supply(supply_id))

                if not tasks:
                    continue

                # Выполняем все запросы параллельно
                wb_supplies_info = await asyncio.gather(*tasks)

                # Обрабатываем результаты
                account_supplies = []
                for i, wb_supply_info in enumerate(wb_supplies_info):
                    supply_id = supplies_list[i].get('id')

                    if wb_supply_info and not wb_supply_info.get('errors'):
                        enriched_supply = {
                            'id': supply_id,
                            'name': wb_supply_info.get('name', f'Supply_{supply_id}'),
                            'createdAt': wb_supply_info.get('createdAt', ''),
                            'done': wb_supply_info.get('done', False)
                        }
                        account_supplies.append(enriched_supply)

                if account_supplies:
                    enriched_supplies.append({account: account_supplies})

        logger.info(f"Обогащено {len(enriched_supplies)} групп поставок информацией из WB API")
        return enriched_supplies

    def _merge_supplies_data(self, basic_supplies: List[Dict], fictitious_supplies: List[Dict]) -> List[Dict]:
        """
        Объединяет данные поставок из разных источников по аккаунтам.
        
        Args:
            basic_supplies: Поставки из shipment_of_goods
            fictitious_supplies: Поставки из hanging_supplies
            
        Returns:
            List[Dict]: Объединенные данные поставок
        """
        merged_accounts = {}
        for supplies_group in basic_supplies:
            if isinstance(supplies_group, dict):
                for account, supplies_list in supplies_group.items():
                    if account not in merged_accounts:
                        merged_accounts[account] = []
                    merged_accounts[account].extend(supplies_list)

        for supplies_group in fictitious_supplies:
            if isinstance(supplies_group, dict):
                for account, supplies_list in supplies_group.items():
                    if account not in merged_accounts:
                        merged_accounts[account] = []
                    merged_accounts[account].extend(supplies_list)

        return [merged_accounts] if merged_accounts else []

    def _exclude_wb_active_from_db_supplies(self, db_supplies: List[Dict], wb_active_supplies: List[Dict]) -> List[
        Dict]:
        """
        Исключает из БД поставок те, которые есть среди активных WB поставок.
        
        Args:
            db_supplies: Поставки из базы данных (из get_weekly_supply_ids)
            wb_active_supplies: Активные поставки из WB API (из get_information_to_supplies)
        
        Returns:
            List[Dict]: Отфильтрованные поставки из БД (только те, которых нет в активных WB)
        """
        # Создаем множество активных поставок (supply_id, account)
        wb_active_set = set()
        for account_data in wb_active_supplies:
            for account, supplies_list in account_data.items():
                for supply in supplies_list:
                    wb_active_set.add((supply['id'], account))

        logger.info(f"Найдено {len(wb_active_set)} активных поставок в WB API для исключения")

        # Фильтруем БД поставки
        filtered_supplies = []
        excluded_count = 0
        total_count = 0

        for account_data in db_supplies:
            filtered_account_data = {}
            for account, supplies_list in account_data.items():
                filtered_supplies_list = []
                for supply in supplies_list:
                    total_count += 1
                    if (supply['id'], account) not in wb_active_set:
                        filtered_supplies_list.append(supply)
                    else:
                        excluded_count += 1
                        logger.debug(f"Исключена поставка {supply['id']} из аккаунта {account} (найдена в активных WB)")

                if filtered_supplies_list:
                    filtered_account_data[account] = filtered_supplies_list

            if filtered_account_data:
                filtered_supplies.append(filtered_account_data)

        logger.info(f"Обработано {total_count} поставок из БД, исключено {excluded_count}, "
                    f"осталось {total_count - excluded_count} для дальнейшей обработки")

        return filtered_supplies

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
                                local_vendor_code=process_local_vendor_code(data["article"]),
                                createdAt=data["createdAt"])
                    for data in orders["orders"]]}

    async def filter_supplies_by_hanging(self, supplies_data: List, hanging_only: bool = False) -> List:
        """
        Фильтрует список поставок по признаку "висячая".

        НОВОЕ: Добавлена валидация статусов заказов из assembly_task_status:
        - Вычисляет canceled_order_ids (wb_status = canceled/canceled_by_client)
        - Скрывает полностью отгруженные поставки: shipped_count >= (count - canceled_count)

        Args:
            supplies_data: Список поставок для фильтрации
            hanging_only: Если True - оставить только висячие поставки, если False - только обычные (не висячие)
        Returns:
            List: Отфильтрованный список поставок
        """
        hanging_supplies_list = await HangingSupplies(self.db).get_hanging_supplies()
        hanging_supplies_map = {(hs['supply_id'], hs['account']): hs for hs in hanging_supplies_list}

        # ========================================
        # НОВОЕ: Получаем статусы заказов из assembly_task_status
        # ========================================
        if hanging_only:
            # Собираем все order_id для висячих поставок, группируем по аккаунтам
            orders_by_account = defaultdict(set)  # {account: {order_id1, order_id2, ...}}
            supply_orders_map = {}  # {(supply_id, account): [order_ids]}

            for supply in supplies_data:
                key = (supply['supply_id'], supply['account'])
                if key in hanging_supplies_map:
                    order_ids = [
                        order['order_id'] if isinstance(order, dict) else order.order_id
                        for order in supply.get('orders', [])
                    ]
                    supply_orders_map[key] = order_ids
                    orders_by_account[supply['account']].update(order_ids)

            # Получаем статусы батчем для каждого аккаунта
            statuses_cache = {}  # {account: {order_id: {'wb_status': '...', 'supplier_status': '...'}}}
            assembly_task_status_service = AssemblyTaskStatus(self.db)

            for account, order_ids_set in orders_by_account.items():
                if order_ids_set:
                    order_ids_list = list(order_ids_set)
                    statuses = await assembly_task_status_service.get_order_statuses_batch(account, order_ids_list)
                    statuses_cache[account] = statuses
                    logger.info(
                        f"Получено {len(statuses)} статусов для аккаунта {account} "
                        f"из {len(order_ids_list)} запрошенных заказов"
                    )

        target_wilds = {}
        filtered_supplies = []

        for supply in supplies_data:
            is_hanging = (supply['supply_id'], supply['account']) in hanging_supplies_map

            if hanging_only == is_hanging:
                if hanging_only:
                    supply["is_hanging"] = True
                    key = (supply['supply_id'], supply['account'])

                    # Добавляем количество отгруженных товаров
                    hanging_supply_data = hanging_supplies_map[key]
                    fictitious_shipped_order_ids = hanging_supply_data.get('fictitious_shipped_order_ids', [])

                    # Десериализуем fictitious_shipped_order_ids если это строка JSON
                    if isinstance(fictitious_shipped_order_ids, str):
                        try:
                            fictitious_shipped_order_ids = json.loads(fictitious_shipped_order_ids)
                        except json.JSONDecodeError:
                            fictitious_shipped_order_ids = []

                    if fictitious_shipped_order_ids and isinstance(fictitious_shipped_order_ids, list):
                        # Подсчитываем уникальные ID заказов
                        unique_shipped_ids = set(
                            order.get('order_id') for order in fictitious_shipped_order_ids
                            if isinstance(order, dict) and order.get('order_id')
                        )
                        supply["shipped_count"] = len(unique_shipped_ids)
                    else:
                        unique_shipped_ids = set()  # Пустое множество
                        supply["shipped_count"] = 0

                    # Сохраняем для использования в логике проверки доступных заказов
                    supply["_unique_shipped_ids"] = unique_shipped_ids

                    # Добавляем информацию о фиктивной доставке
                    is_fictitious_delivered = hanging_supply_data.get('is_fictitious_delivered', False)
                    supply["is_fictitious_delivered"] = is_fictitious_delivered

                    # ========================================
                    # СТРОГАЯ ВАЛИДАЦИЯ: применяется ТОЛЬКО для поставок в доставке (is_fictitious_delivered=True)
                    # ========================================
                    if is_fictitious_delivered:
                        # Поставка в статусе доставки - применяем строгую валидацию
                        blocked_order_ids = []  # Все невалидные заказы
                        valid_order_ids = []    # Валидные заказы (для подсчета)
                        order_ids = supply_orders_map.get(key, [])
                        account_statuses = statuses_cache.get(supply['account'], {})

                        for order_id in order_ids:
                            status_data = account_statuses.get(order_id, {})
                            supplier_status = status_data.get('supplier_status')
                            wb_status = status_data.get('wb_status')

                            # Разрешаем ТОЛЬКО конкретную комбинацию статусов
                            is_valid_for_delivery = (
                                supplier_status == 'complete' and wb_status == 'waiting'
                            )

                            if not is_valid_for_delivery:
                                blocked_order_ids.append(order_id)
                            else:
                                valid_order_ids.append(order_id)

                        # Используем существующее поле для совместимости с фронтендом
                        supply["canceled_order_ids"] = blocked_order_ids

                        # Проверяем наличие валидных заказов и полноту отгрузки
                        # Используем теорию множеств для точного расчета
                        all_order_ids = set(order_ids)
                        shipped_ids = supply.get("_unique_shipped_ids", set())
                        blocked_ids = set(blocked_order_ids)

                        # Доступные = Все - (Отгруженные ∪ Заблокированные)
                        unavailable_ids = shipped_ids | blocked_ids
                        available_ids = all_order_ids - unavailable_ids
                        available_to_ship = len(available_ids)

                        # Заблокированные не отгруженные (для логирования)
                        not_shipped_blocked_ids = blocked_ids - shipped_ids

                        # Скрываем только если НЕТ доступных заказов
                        is_fully_processed = (available_to_ship == 0)

                        if is_fully_processed:
                            # Определяем причину
                            if len(all_order_ids - shipped_ids) == 0:
                                reason = 'все заказы отгружены'
                            elif len(not_shipped_blocked_ids) > 0:
                                reason = 'все не отгруженные заблокированы'
                            else:
                                reason = 'нет доступных заказов'

                            logger.info(
                                f"Скрываем поставку в доставке {supply['supply_id']} (аккаунт {supply['account']}): "
                                f"всего={len(all_order_ids)}, отгружено={len(shipped_ids)}, "
                                f"заблокировано_не_отгруженных={len(not_shipped_blocked_ids)}, "
                                f"доступно={available_to_ship} "
                                f"(причина: {reason})"
                            )
                            continue  # Не добавляем в результат - скрываем поставку!
                    else:
                        # Активная висячая поставка (НЕ в доставке) - старая логика
                        canceled_order_ids = []
                        order_ids = supply_orders_map.get(key, [])
                        account_statuses = statuses_cache.get(supply['account'], {})

                        for order_id in order_ids:
                            status_data = account_statuses.get(order_id, {})
                            wb_status = status_data.get('wb_status')

                            # Блокируем только canceled и canceled_by_client (старая логика)
                            if wb_status in ['canceled', 'canceled_by_client']:
                                canceled_order_ids.append(order_id)

                        supply["canceled_order_ids"] = canceled_order_ids

                        # Проверяем полноту отгрузки (старая логика)
                        count = len(supply.get('orders', []))
                        canceled_count = len(canceled_order_ids)
                        shipped_count = supply["shipped_count"]
                        available_to_ship = count - canceled_count

                        is_fully_shipped = (shipped_count >= available_to_ship)

                        if is_fully_shipped:
                            logger.info(
                                f"Скрываем полностью отгруженную активную поставку {supply['supply_id']} (аккаунт {supply['account']}): "
                                f"shipped={shipped_count}, available={available_to_ship} "
                                f"(count={count}, canceled={canceled_count})"
                            )
                            continue  # Не добавляем в результат - скрываем поставку!

                    # Проверка на target_wilds (оставляем без изменений)
                    has_target_wild = any(
                        (order.local_vendor_code if hasattr(order, 'local_vendor_code') else order.get('local_vendor_code')) in target_wilds
                        for order in supply.get('orders', [])
                    )
                    if not has_target_wild:
                        filtered_supplies.append(supply)
                else:
                    filtered_supplies.append(supply)

        # Очищаем временные данные перед возвратом
        for supply in filtered_supplies:
            if "_unique_shipped_ids" in supply:
                del supply["_unique_shipped_ids"]

        return filtered_supplies

    async def enrich_orders_with_qr_codes(self, supplies_data: List[Dict]) -> List[Dict]:
        """
        Обогащает заказы в поставках QR-кодами из таблицы qr_scans.

        Оптимизация:
        - Batch-запрос для всех order_ids сразу
        - Минимальное количество обращений к БД

        Args:
            supplies_data: Список поставок с заказами

        Returns:
            Обогащенный список поставок (изменяет in-place и возвращает)
        """
        if not supplies_data:
            return supplies_data

        # ============ Шаг 1: Собираем все order_ids ============
        all_order_ids = []
        order_supply_map = {}  # {order_id: (supply_index, order_index)}

        for supply_idx, supply in enumerate(supplies_data):
            for order_idx, order in enumerate(supply.get('orders', [])):
                # Извлекаем order_id (поддержка dict и Pydantic модели)
                order_id = order['order_id'] if isinstance(order, dict) else order.order_id
                all_order_ids.append(order_id)
                order_supply_map[order_id] = (supply_idx, order_idx)

        if not all_order_ids:
            logger.debug("Нет заказов для обогащения QR-кодами")
            return supplies_data

        # ============ Шаг 2: Получаем QR-коды batch-запросом ============
        qr_scan_db = QRScanDB(self.db)

        logger.debug(f"Получение QR-кодов для {len(all_order_ids)} заказов")
        qr_codes = await qr_scan_db.get_qr_codes_by_order_ids(all_order_ids)

        # ============ Шаг 3: Обогащаем заказы QR-кодами ============
        enriched_count = 0

        for order_id, qr_code in qr_codes.items():
            if order_id not in order_supply_map:
                continue

            supply_idx, order_idx = order_supply_map[order_id]
            order = supplies_data[supply_idx]['orders'][order_idx]

            # Устанавливаем QR-код (поддержка dict и Pydantic модели)
            if isinstance(order, dict):
                order['qr_code'] = qr_code
            else:
                order.qr_code = qr_code

            enriched_count += 1

        logger.info(
            f"Обогащено {enriched_count} заказов QR-кодами из {len(all_order_ids)} общих "
            f"({enriched_count / len(all_order_ids) * 100:.1f}% покрытие)"
        )

        return supplies_data

    @staticmethod
    def _is_supply_empty(hanging_supply: Dict) -> bool:
        """
        Проверяет, является ли поставка пустой (нет заказов).

        Args:
            hanging_supply: Запись висячей поставки из БД

        Returns:
            bool: True если поставка пустая, False если есть заказы
        """
        try:
            order_data = hanging_supply.get('order_data', {})
            if isinstance(order_data, str):
                order_data = json.loads(order_data)

            orders = order_data.get('orders', [])
            return len(orders) == 0

        except Exception as e:
            logger.error(
                f"Ошибка парсинга order_data для поставки "
                f"{hanging_supply.get('supply_id')}: {e}"
            )
            # В случае ошибки парсинга считаем поставку пустой (безопаснее)
            return True

    def _should_mark_supply_as_fictitious(
        self,
        hanging_supply: Dict,
        active_supply_ids: Set[Tuple[str, str]]
    ) -> Tuple[bool, Optional[str]]:
        """
        Проверяет, нужно ли пометить поставку как фиктивную.

        Args:
            hanging_supply: Запись висячей поставки из БД
            active_supply_ids: Множество (supply_id, account) поставок в статусе сборки (done=False)

        Returns:
            Tuple[bool, Optional[str]]: (нужно_пометить, причина_пропуска)
        """
        supply_id = hanging_supply['supply_id']
        account = hanging_supply['account']

        # ПРОВЕРКА 1: Поставка еще в сборке (done=False в WB)
        if (supply_id, account) in active_supply_ids:
            return False, "active_in_wb"

        # ПРОВЕРКА 2: Уже помечена фиктивной
        if hanging_supply.get('is_fictitious_delivered', False):
            return False, "already_marked"

        # ПРОВЕРКА 3: Поставка пустая (нет заказов)
        if self._is_supply_empty(hanging_supply):
            return False, "empty_supply"

        # Все проверки пройдены: поставка перешла в доставку (done=True)
        return True, None

    async def _auto_mark_done_supplies_as_fictitious(
        self,
        active_supplies_result: List[Dict]
    ) -> Tuple[int, int]:
        """
        Автоматически помечает висячие поставки как фиктивные, если они перешли в доставку (done=True).

        Логика:
        1. Получает все висячие поставки из БД
        2. Сравнивает с поставками в сборке из WB API (done=False)
        3. Поставки из БД, которых нет в списке сборки → перешли в доставку → помечает как фиктивные

        Статусы WB:
        - done=False: поставка в статусе "Сборка" (активная)
        - done=True:  поставка в статусе "Доставка" (завершена)

        Защита от ошибок:
        - Пропускает пустые поставки (будут обработаны EmptySupplyCleaner)
        - Пропускает уже помеченные фиктивные
        - Обрабатывает ошибки для каждой поставки независимо

        Args:
            active_supplies_result: Список поставок в статусе сборки (done=False) из WB API

        Returns:
            Tuple[int, int]: (количество_помеченных, количество_пропущенных_пустых)
        """
        hanging_supplies_model = HangingSupplies(self.db)
        all_hanging = await hanging_supplies_model.get_hanging_supplies()

        # Множество поставок в статусе сборки (done=False) из WB API
        active_supply_ids = {
            (supply['supply_id'], supply['account'])
            for supply in active_supplies_result
        }

        marked_count = 0
        skipped_empty = 0

        for hanging in all_hanging:
            supply_id = hanging['supply_id']
            account = hanging['account']

            # Проверяем все условия для пометки
            should_mark, skip_reason = self._should_mark_supply_as_fictitious(
                hanging, active_supply_ids
            )

            if not should_mark:
                if skip_reason == "empty_supply":
                    logger.debug(f"⏭️ Пропускаем пустую поставку {supply_id} ({account})")
                    skipped_empty += 1
                continue

            # ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ: Поставка перешла в доставку, помечаем фиктивной
            try:
                # Получаем количество заказов для логирования
                order_data = hanging.get('order_data', {})
                if isinstance(order_data, str):
                    order_data = json.loads(order_data)
                orders_count = len(order_data.get('orders', []))

                await hanging_supplies_model.mark_as_fictitious_delivered(
                    supply_id, account, operator='auto_system'
                )
                logger.info(
                    f"🔔 Поставка {supply_id} ({account}) помечена как фиктивная "
                    f"(перешла в доставку done=True, {orders_count} заказов)"
                )
                marked_count += 1

            except Exception as e:
                logger.error(f"Ошибка пометки {supply_id}: {e}")

        return marked_count, skipped_empty

    async def get_list_supplies(self, hanging_only: bool = False, is_delivery: bool = False) -> SupplyIdResponseSchema:
        """
        Получить список поставок с фильтрацией по висячим и доставке.

        Логика источников данных:
        - is_delivery=True  → Redis кэш → БД хранилище → WB API
        - is_delivery=False → Redis кэш → WB API (без изменений)

        Args:
            hanging_only: Если True - вернуть только висячие поставки, если False - только обычные (не висячие)
            is_delivery: Если True - получать поставки из отгрузок за неделю, если False - из WB API
        Returns:
            SupplyIdResponseSchema: Список поставок с их деталями
        """
        logger.info(f"Получение данных о поставках, hanging_only={hanging_only}, is_delivery={is_delivery}")

        if is_delivery:
            logger.info("Режим доставленных поставок: Redis → БД хранилище → WB API")

            # 1. Получаем номера доставленных поставок из источников
            wb_active_supplies_ids = await self.get_information_to_supplies()
            basic_supplies_ids = await ShipmentOfGoods(self.db).get_weekly_supply_ids()
            fictitious_supplies_ids = await HangingSupplies(self.db).get_weekly_fictitious_supplies_ids(
                is_fictitious_delivered=True
            )

            all_db_supplies_ids = self._merge_supplies_data(basic_supplies_ids, fictitious_supplies_ids)
            filtered_supplies_ids = self._exclude_wb_active_from_db_supplies(
                all_db_supplies_ids,
                wb_active_supplies_ids
            )

            # 2. Формируем список запрашиваемых поставок
            requested_supplies = []
            for account_data in filtered_supplies_ids:
                for account, supplies_list in account_data.items():
                    for supply in supplies_list:
                        requested_supplies.append((supply['id'], account))

            logger.info(f"Запрошено {len(requested_supplies)} доставленных поставок")

            # 3. Проверяем БД хранилище
            delivered_storage = DeliveredSupplies(self.db)
            stored_supplies = await delivered_storage.get_supplies_from_storage(requested_supplies)

            # 4. Определяем недостающие поставки
            missing_supplies = await delivered_storage.get_missing_supplies(requested_supplies)

            # 5. Получаем недостающие из WB API (существующая логика)
            if missing_supplies:
                logger.info(f"Получение {len(missing_supplies)} недостающих поставок из WB API")

                # Формируем структуру для WB API
                missing_by_account = {}
                for supply_id, account in missing_supplies:
                    if account not in missing_by_account:
                        missing_by_account[account] = []
                    missing_by_account[account].append({'id': supply_id})

                missing_formatted = [{acc: sups} for acc, sups in missing_by_account.items()]

                # Получаем данные из WB API (стандартная логика)
                enriched_supplies = await self.get_information_to_supply_details(missing_formatted)
                missing_orders = await self.get_information_orders_to_supplies(enriched_supplies)

                # Формируем result для недостающих (используем существующую логику)
                missing_result = []
                enriched_dict = {key: val for d in enriched_supplies for key, val in d.items()}

                for order_data in missing_orders:
                    for account, supply_orders in order_data.items():
                        for supply_id, orders in supply_orders.items():
                            supply_meta = {
                                data["id"]: {"name": data["name"], "createdAt": data['createdAt']}
                                for data in enriched_dict.get(account, [])
                            }

                            if supply_id in supply_meta:
                                supply_obj = self.create_supply_result(
                                    supply_meta,
                                    supply_id,
                                    account,
                                    orders
                                )
                                missing_result.append(supply_obj)

                # 6. Сохраняем недостающие в БД хранилище
                if missing_result:
                    saved_count = await delivered_storage.save_supplies_to_storage(missing_result)
                    logger.info(f"Сохранено {saved_count} новых поставок в БД хранилище")

                    # Добавляем к stored_supplies
                    for supply_obj in missing_result:
                        key = (supply_obj['supply_id'], supply_obj['account'])
                        stored_supplies[key] = supply_obj

            # 7. Формируем итоговый result из БД хранилища
            result = list(stored_supplies.values())

            # Метрики
            db_hit_count = len(stored_supplies) - len(missing_supplies)
            db_hit_rate = (db_hit_count / len(requested_supplies) * 100) if requested_supplies else 0

            logger.info(
                f"Доставленные поставки: total={len(result)}, "
                f"from_db={db_hit_count}, "
                f"from_api={len(missing_supplies)}, "
                f"db_hit_rate={db_hit_rate:.1f}%"
            )

        else:
            # ========== АКТИВНЫЕ ПОСТАВКИ (БЕЗ ИЗМЕНЕНИЙ) ==========
            logger.info("Получение поставок из WB API")
            supplies_ids = await self.get_information_to_supplies()
            supplies = self.group_result(await self.get_information_orders_to_supplies(supplies_ids))
            result = []
            supplies_ids_dict = {key: value for d in supplies_ids for key, value in d.items()}

            for account, value in supplies.items():
                for supply_id, orders in value.items():
                    supply = {
                        data["id"]: {"name": data["name"], "createdAt": data['createdAt']}
                        for data in supplies_ids_dict[account] if not data['done']
                    }
                    result.append(self.create_supply_result(supply, supply_id, account, orders))

        # Автоматическая пометка висячих поставок с done=True как фиктивных
        if hanging_only and not is_delivery:
            # Формируем список ТОЛЬКО активных поставок (done=False) для корректной пометки
            active_supplies_only_false = []
            for account, supplies_list in supplies_ids_dict.items():
                for supply_data in supplies_list:
                    if not supply_data['done']:  # Только done=False
                        active_supplies_only_false.append({
                            'supply_id': supply_data['id'],
                            'account': account
                        })

            marked_count, skipped_empty = await self._auto_mark_done_supplies_as_fictitious(active_supplies_only_false)
            if marked_count > 0 or skipped_empty > 0:
                logger.info(
                    f"Автопометка: {marked_count} фиктивных, "
                    f"{skipped_empty} пропущено (пустые)"
                )

        # Финальная фильтрация
        filtered_result = await self.filter_supplies_by_hanging(result, hanging_only)

        # Обогащение QR-кодами
        enriched_result = await self.enrich_orders_with_qr_codes(filtered_result)

        return SupplyIdResponseSchema(supplies=enriched_result)

    async def get_delivery_supplies_ids_only(self, hanging_only: bool = False) -> Set[str]:
        """
        Получает только номера поставок доставки без полных данных заказов.
        
        Оптимизированная версия для сравнения - возвращает только supply_id.
        
        Args:
            hanging_only: Фильтр по висячим поставкам
            
        Returns:
            Set[str]: Множество supply_id для delivery поставок
        """
        try:
            logger.info(f"Получение только supply_id для delivery поставок, hanging_only={hanging_only}")

            # Получаем базовые данные как в get_list_supplies для is_delivery=True
            wb_active_supplies_ids = await self.get_information_to_supplies()
            basic_supplies_ids = await ShipmentOfGoods(self.db).get_weekly_supply_ids()
            fictitious_supplies_ids = await HangingSupplies(self.db).get_weekly_fictitious_supplies_ids(
                is_fictitious_delivered=True)

            # Объединяем и фильтруем как в оригинальном методе
            all_db_supplies_ids = self._merge_supplies_data(basic_supplies_ids, fictitious_supplies_ids)
            filtered_supplies_ids = self._exclude_wb_active_from_db_supplies(
                all_db_supplies_ids, wb_active_supplies_ids)

            # Извлекаем только supply_id (без запроса полных данных заказов)
            supply_ids_set = set()
            for account_data in filtered_supplies_ids:
                for account, supplies_list in account_data.items():
                    for supply_data in supplies_list:
                        supply_ids_set.add(supply_data['id'])

            # Получаем висячие supply_id из БД
            hanging_supplies_model = HangingSupplies(self.db)
            hanging_supply_ids_data = await hanging_supplies_model.get_hanging_supplies()
            hanging_supply_ids = {item['supply_id'] for item in hanging_supply_ids_data}
            # Применяем фильтр hanging_only если нужно
            if hanging_only:
                supply_ids_set = supply_ids_set.intersection(hanging_supply_ids)
            else:
                supply_ids_set = supply_ids_set - hanging_supply_ids

            logger.info(f"Получено {len(supply_ids_set)} delivery supply_id, hanging_only={hanging_only}")
            return supply_ids_set

        except Exception as e:
            logger.error(f"Ошибка получения delivery supply_id, hanging_only={hanging_only}: {str(e)}")
            return set()

    async def check_current_orders(self, supply_ids: SupplyIdBodySchema, allow_partial: bool = False):
        logger.info("Проверка поставок на соответствие наличия заказов (сверка заказов по поставкам)")
        tasks: List = [
            Supplies(
                supply.account, get_wb_tokens()[supply.account]
            ).get_supply_orders(supply.supply_id)
            for supply in supply_ids.supplies
        ]
        result: Dict[str, Dict] = self.group_result(await asyncio.gather(*tasks))
        self._enrich_orders_with_created_at(supply_ids, result)

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

    @staticmethod
    def _enrich_orders_with_created_at(supply_ids: SupplyIdBodySchema, wb_result: Dict[str, Dict]) -> None:
        """
        Обогащает заказы значениями createdAt из данных WB API
        
        Args:
            supply_ids: Схема с поставками и заказами для обогащения
            wb_result: Результат от WB API с полными данными заказов
        """
        order_dates = {
            order['id']: order['createdAt']
            for account_data in wb_result.values()
            for supply_data in account_data.values()
            for order in supply_data.get('orders', [])
            if order.get('id') and order.get('createdAt')
        }

        enriched_count = 0
        for supply in supply_ids.supplies:
            for order in supply.orders:
                if not order.createdAt and order.order_id in order_dates:
                    order.createdAt = order_dates[order.order_id]
                    enriched_count += 1

        if enriched_count > 0:
            logger.info(f"Обогащено {enriched_count} заказов данными createdAt")

    async def filter_and_fetch_stickers(self, supply_ids: SupplyIdBodySchema, allow_partial: bool = False) -> Dict[str, List[Dict[str, Any]]]:
        logger.info('Инициализация получение документов (Стикеры и Лист подбора)')
        await self.check_current_orders(supply_ids, allow_partial)
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
                            local_vendor_code=wild_filter.wild, createdAt=order_detail.get('createdAt'))
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

    def _create_fictitious_delivery_response(self, success: bool, message: str, supply_id: str, account: str,
                                             delivery_response=None, marked_as_fictitious: bool = False,
                                             operator: str = 'unknown') -> Dict[str, Any]:
        """
        Создает стандартный ответ для операций с фиктивными поставками.
        
        Args:
            success: Успешность операции
            message: Сообщение о результате
            supply_id: ID поставки
            account: Аккаунт Wildberries
            delivery_response: Ответ от WB API
            marked_as_fictitious: Была ли поставка помечена как фиктивная
            operator: Оператор
            
        Returns:
            Dict[str, Any]: Стандартизированный ответ
        """
        return {
            "success": success,
            "message": message,
            "supply_id": supply_id,
            "account": account,
            "delivery_response": delivery_response,
            "marked_as_fictitious": marked_as_fictitious,
            "operator": operator
        }

    async def _validate_fictitious_delivery_preconditions(self, supply_id: str, account: str, operator: str) -> Dict[
        str, Any]:
        """
        Проверяет предварительные условия для фиктивной доставки.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            operator: Оператор
            
        Returns:
            Dict[str, Any]: Результат валидации или None если все проверки пройдены
        """

        if not self.db:
            raise ValueError("Отсутствует подключение к базе данных")

        hanging_supplies = HangingSupplies(self.db)

        # Проверяем, существует ли поставка
        hanging_supply = await hanging_supplies.get_hanging_supply_by_id(supply_id, account)
        if not hanging_supply:
            return self._create_fictitious_delivery_response(
                success=False,
                message=f"Висячая поставка {supply_id} для аккаунта {account} не найдена",
                supply_id=supply_id,
                account=account,
                operator=operator
            )

        # Проверяем, не была ли уже помечена как фиктивно доставленная
        is_already_delivered = await hanging_supplies.is_fictitious_delivered(supply_id, account)
        if is_already_delivered:
            return self._create_fictitious_delivery_response(
                success=False,
                message=f"Поставка {supply_id} ({account}) уже помечена как фиктивно доставленная",
                supply_id=supply_id,
                account=account,
                operator=operator
            )

    async def _execute_delivery_to_wb(self, supply_id: str, account: str) -> Any:
        """
        Выполняет перевод поставки в доставку через WB API.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            
        Returns:
            Any: Ответ от WB API
            
        Raises:
            ValueError: Если токен для аккаунта не найден
        """
        wb_tokens = get_wb_tokens()
        if account not in wb_tokens:
            raise ValueError(f"Токен для аккаунта {account} не найден")

        supplies_api = Supplies(account, wb_tokens[account])
        return await supplies_api.deliver_supply(supply_id)

    def _is_delivery_successful(self, delivery_response: Any) -> bool:
        """
        Проверяет успешность ответа от WB API.
        
        Args:
            delivery_response: Ответ от WB API
            
        Returns:
            bool: True если доставка успешна
        """
        if hasattr(delivery_response, 'status_code') and delivery_response.status_code >= 400:
            return False
        return True

    async def _mark_supply_as_fictitious_delivered(self, supply_id: str, account: str, operator: str) -> bool:
        """
        Помечает поставку как фиктивно доставленную в БД.
        
        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            operator: Оператор
            
        Returns:
            bool: True если операция успешна
        """

        hanging_supplies = HangingSupplies(self.db)
        return await hanging_supplies.mark_as_fictitious_delivered(supply_id, account, operator)

    async def _process_successful_delivery(self, supply_id: str, account: str, operator: str,
                                           delivery_response: Any) -> Dict[str, Any]:
        """
        Обрабатывает успешную доставку поставки.

        Args:
            supply_id: ID поставки
            account: Аккаунт Wildberries
            operator: Оператор
            delivery_response: Ответ от WB API

        Returns:
            Dict[str, Any]: Результат обработки
        """
        marked_success = await self._mark_supply_as_fictitious_delivered(supply_id, account, operator)

        if marked_success:
            logger.info(f"Фиктивная поставка {supply_id} ({account}) успешно переведена в доставку и помечена")

            # Логируем статус FICTITIOUS_DELIVERED для всех заказов поставки
            if self.db:
                try:
                    # Получаем все заказы поставки из WB API
                    wb_tokens = get_wb_tokens()
                    supplies_api = Supplies(account, wb_tokens[account])
                    supply_orders_response = await supplies_api.get_supply_orders(supply_id)

                    # Извлекаем список заказов из вложенной структуры
                    # Структура: {account: {supply_id: {"orders": [...]}}}
                    orders_list = supply_orders_response.get(account, {}).get(supply_id, {}).get('orders', [])

                    # ВАЖНО: НЕ снимаем резерв при переводе в фиктивную доставку!
                    # Резерв будет снят только при фактической отгрузке товара (process_fictitious_shipment)
                    # Причина: товар физически не покинул склад, только изменился виртуальный статус в WB

                    # Подготавливаем данные для логирования
                    if orders_list:
                        fictitious_delivered_data = [{'order_id': order['id'],'supply_id': supply_id,'account': account}
                                                     for order in orders_list]

                        status_service = OrderStatusService(self.db)
                        logged_count = await status_service.process_and_log_fictitious_delivered(
                            fictitious_delivered_data
                        )
                        logger.info(f"Залогировано {logged_count} заказов со статусом FICTITIOUS_DELIVERED")
                except Exception as e:
                    logger.error(f"Ошибка при логировании статуса FICTITIOUS_DELIVERED: {str(e)}")
                    # Не пробрасываем ошибку, чтобы не сломать основной flow

            return self._create_fictitious_delivery_response(
                success=True,
                message=f"Фиктивная поставка {supply_id} успешно переведена в доставку",
                supply_id=supply_id,
                account=account,
                delivery_response=delivery_response,
                marked_as_fictitious=True,
                operator=operator
            )
        else:
            logger.error(f"Поставка {supply_id} переведена в доставку, но не удалось пометить как фиктивную")
            return self._create_fictitious_delivery_response(
                success=False,
                message="Поставка переведена в доставку, но не удалось пометить как фиктивную",
                supply_id=supply_id,
                account=account,
                delivery_response=delivery_response,
                marked_as_fictitious=False,
                operator=operator
            )

    async def deliver_fictitious_supply(self, supply_id: str, account: str, operator: str = 'unknown') -> Dict[
        str, Any]:
        """
        Переводит фиктивную висячую поставку в статус доставки.
        
        Args:
            supply_id: ID поставки Wildberries
            account: Аккаунт Wildberries
            operator: Оператор, выполняющий операцию
            
        Returns:
            Dict[str, Any]: Результат операции
        """

        logger.info(f"Начало перевода фиктивной поставки {supply_id} ({account}) в доставку оператором {operator}")

        validation_result = await self._validate_fictitious_delivery_preconditions(supply_id, account, operator)
        if validation_result:
            return validation_result

        delivery_response = await self._execute_delivery_to_wb(supply_id, account)

        return await self._process_successful_delivery(supply_id, account, operator, delivery_response)

    async def deliver_fictitious_supplies_batch(self, supplies: Dict[str, str], operator: str = 'unknown') -> Dict[
        str, Any]:
        """
        Переводит объект фиктивных висячих поставок в статус доставки.
        
        Args:
            supplies: Объект поставок {supply_id: account}
            operator: Оператор, выполняющий операцию
            
        Returns:
            Dict[str, Any]: Результат пакетной операции
        """

        start_time = time.time()
        logger.info(f"Начало пакетной обработки {len(supplies)} фиктивных поставок оператором {operator}")

        results = []
        successful_count = 0
        failed_count = 0

        for supply_id, account in supplies.items():

            try:
                result = await self.deliver_fictitious_supply(supply_id, account, operator)
                if result['success']:
                    successful_count += 1
                else:
                    failed_count += 1
                results.append(result)
            except Exception as e:
                failed_count += 1
                error_result = self._create_fictitious_delivery_response(
                    success=False,
                    message=f"Ошибка обработки: {str(e)}",
                    supply_id=supply_id,
                    account=account,
                    operator=operator
                )
                results.append(error_result)
                logger.error(f"Ошибка при обработке поставки {supply_id} ({account}): {str(e)}")

        end_time = time.time()
        processing_time = end_time - start_time

        logger.info(
            f"Пакетная обработка завершена: {successful_count} успешных, {failed_count} неудачных, время: {processing_time:.2f}с")

        return {
            "success": failed_count == 0,  # Успех только если все поставки обработаны успешно
            "message": f"Обработано {len(supplies)} поставок: {successful_count} успешных, {failed_count} неудачных",
            "total_processed": len(supplies),
            "successful_count": successful_count,
            "failed_count": failed_count,
            "results": results,
            "processing_time_seconds": round(processing_time, 2),
            "operator": operator
        }

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

                    logger.info(
                        f"Получено {len(filtered_orders)} заказов с артикулом {wild_code} из кабинета {account}")

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

    def _select_orders_for_move(self, request_data, orders_by_wild_account: Dict[Tuple[str, str], List[dict]]) -> Tuple[
        List[dict], Set[Tuple[str, str]]]:
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
            for account in {supply_item.account for supply_item in wild_item.supplies}:
                key = (wild_code, account)
                if key in orders_by_wild_account:
                    wild_orders.extend(orders_by_wild_account[key])

            # Сортировка по времени создания:
            # - Для финальных поставок: старые первые (FIFO)
            # - Для висячих поставок: новые первые
            if getattr(request_data, 'move_to_final', False):
                wild_orders.sort(key=lambda x: (x['timestamp'], x.get('id', 0)))  # старые первые
            else:
                wild_orders.sort(key=lambda x: (-x['timestamp'], x.get('id', 0)))  # новые первые

            # Определяем количество заказов для выбора
            selected_count = min(wild_item.remove_count, len(wild_orders))
            supply_type = 'финальные' if getattr(request_data, 'move_to_final', False) else 'висячие'
            logger.info(
                f"Перемещение в {supply_type} поставки: "
                f"выбираем {selected_count} из {len(wild_orders)} заказов для wild {wild_code}"
            )

            selected_orders = wild_orders[:selected_count]

            # Добавляем в список для перемещения
            selected_orders_for_move.extend(selected_orders)

            # Запоминаем какие комбинации (wild_code, account) реально участвуют
            for order in selected_orders:
                participating_combinations.add((order['wild_code'], order['account']))

            logger.info(f"Wild {wild_code}: отобрано {len(selected_orders)} из {len(wild_orders)} заказов")

        return selected_orders_for_move, participating_combinations

    async def _prepare_and_execute_create_supplies(self, participating_combinations: Set[Tuple[str, str]],
                                                   wb_tokens: dict) -> Tuple[List, List]:
        """
        Подготавливает задачи и выполняет параллельное создание поставок в WB API.
        
        Args:
            participating_combinations: Комбинации (wild_code, account)
            wb_tokens: Токены WB для аккаунтов
            
        Returns:
            Tuple[List, List]: (results, task_metadata)
        """
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

    async def _process_create_supplies_results(self, results: List, task_metadata: List, user: dict) -> Dict[
        Tuple[str, str], str]:
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
            logger.info(
                f"Сохранена висячая поставка {supply_id} для {wild_code} в аккаунте {account}, оператор: {operator}")

        except Exception as e:
            logger.error(
                f"Ошибка при сохранении висячей поставки {supply_id} для {wild_code} в аккаунте {account}: {str(e)}")

    async def _create_new_supplies(self, participating_combinations: Set[Tuple[str, str]], wb_tokens: dict,
                                   user: dict) -> Dict[Tuple[str, str], str]:
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

    async def _move_orders_to_supplies(self, selected_orders_for_move: List[dict],
                                       new_supplies: Dict[Tuple[str, str], str], wb_tokens: dict,
                                       check_status: bool = False) -> Tuple[List[int], List[dict]]:
        """
        Перемещает отобранные заказы в новые поставки параллельно.

        Args:
            selected_orders_for_move: Отобранные заказы для перемещения
            new_supplies: Новые поставки по ключу (wild_code, account)
            wb_tokens: Токены WB для аккаунтов
            check_status: Проверять ли статус заказов перед добавлением (default False, т.к. делаем пре-валидацию)

        Returns:
            Tuple[List[int], List[dict]]: (ID успешно перемещенных заказов, список неудачных попыток с деталями)
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
            task = supplies_api.add_order_to_supply(new_supply_id, order_id, check_status=check_status)
            tasks.append(task)
            task_metadata.append({
                'order_id': order_id,
                'account': account,
                'wild_code': wild_code,
                'original_supply_id': order['original_supply_id'],
                'new_supply_id': new_supply_id
            })

        # Параллельное выполнение всех запросов
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Обработка результатов
        moved_order_ids = []
        failed_orders = []

        for metadata, result in zip(task_metadata, results):
            order_id = metadata['order_id']
            original_supply_id = metadata['original_supply_id']
            new_supply_id = metadata['new_supply_id']
            account = metadata['account']
            wild_code = metadata['wild_code']

            # Проверка на исключение
            if isinstance(result, Exception):
                error_msg = f"Исключение при перемещении: {str(result)}"
                logger.error(f"Заказ {order_id} ({account}): {error_msg}")
                failed_orders.append({
                    'order_id': order_id,
                    'account': account,
                    'wild_code': wild_code,
                    'original_supply_id': original_supply_id,
                    'new_supply_id': new_supply_id,
                    'error': error_msg,
                    'reason': 'exception'
                })
                continue

            # Проверка на ошибку в ответе WB API
            if isinstance(result, dict) and result.get('error'):
                error_msg = result.get('error', 'Неизвестная ошибка')
                logger.error(f"Ошибка WB API при перемещении заказа {order_id} ({account}): {error_msg}")
                failed_orders.append({
                    'order_id': order_id,
                    'account': account,
                    'wild_code': wild_code,
                    'original_supply_id': original_supply_id,
                    'new_supply_id': new_supply_id,
                    'error': error_msg,
                    'reason': 'wb_api_error'
                })
                continue

            # Проверка на неуспешный ответ
            if isinstance(result, dict) and result.get('success') == False:
                error_msg = result.get('errorText', 'Операция не выполнена')
                logger.error(f"Неудачное перемещение заказа {order_id} ({account}): {error_msg}")
                failed_orders.append({
                    'order_id': order_id,
                    'account': account,
                    'wild_code': wild_code,
                    'original_supply_id': original_supply_id,
                    'new_supply_id': new_supply_id,
                    'error': error_msg,
                    'reason': 'unsuccessful_response'
                })
                continue

            # Проверка на успешный ответ: пустая строка (код 204) означает успех
            if isinstance(result, str) and result == "":
                # Успешное перемещение (WB API вернул 204 с пустым телом)
                moved_order_ids.append(order_id)
                logger.info(f"Заказ {order_id} ({account}, {wild_code}) успешно перемещен из {original_supply_id} в {new_supply_id}")
                continue

            # Если result - это dict с успешным статусом, тоже считаем успехом
            if isinstance(result, dict) and not result.get('error') and result.get('success') != False:
                moved_order_ids.append(order_id)
                logger.info(f"Заказ {order_id} ({account}, {wild_code}) перемещен из {original_supply_id} в {new_supply_id}")
                continue

            # Любой другой случай - ошибка
            error_msg = f"Неожиданный ответ API: {type(result).__name__} = {result}"
            logger.error(f"Некорректный ответ для заказа {order_id} ({account}): {error_msg}")
            failed_orders.append({
                'order_id': order_id,
                'account': account,
                'wild_code': wild_code,
                'original_supply_id': original_supply_id,
                'new_supply_id': new_supply_id,
                'error': error_msg,
                'reason': 'invalid_response_type'
            })

        logger.info(f"Результат перемещения: успешно {len(moved_order_ids)}, неудачно {len(failed_orders)}")
        return moved_order_ids, failed_orders

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

        # 1. Подготовка и получение данных заказов
        selected_orders_for_move, participating_combinations = await self._prepare_orders_for_move(request_data)

        # 2. Проверка наличия заказов для перемещения
        if not selected_orders_for_move:
            return self._create_empty_result("Не найдено заказов для перемещения")

        logger.info(f"Всего отобрано {len(selected_orders_for_move)} заказов для перемещения")
        logger.info(f"Участвующие комбинации (wild, account): {participating_combinations}")

        # 3. Создание целевых поставок
        new_supplies = await self._create_target_supplies(participating_combinations, request_data, user)

        # 4. Выполнение перемещения заказов с валидацией
        moved_order_ids, invalid_status_orders, failed_movement_orders = await self._execute_orders_move(
            selected_orders_for_move, new_supplies
        )

        # 5. Отправка данных во внешние системы (успешно перемещенные + заблокированные)
        shipment_success, blocked_prepared_count = await self._process_external_systems_integration(
            request_data, selected_orders_for_move, moved_order_ids, new_supplies, user,
            invalid_status_orders, failed_movement_orders
        )

        # 6. Возврат результата со статистикой
        return self._create_success_result(
            moved_order_ids, new_supplies, selected_orders_for_move,
            invalid_status_orders, failed_movement_orders,
            request_data.move_to_final, shipment_success, blocked_prepared_count
        )

    async def _prepare_orders_for_move(self, request_data) -> Tuple[List[dict], Set[Tuple[str, str]]]:
        """
        Подготавливает данные заказов для перемещения.
        
        Returns:
            Tuple: (selected_orders_for_move, participating_combinations)
        """
        logger.info("Подготовка данных заказов для перемещения")
        
        # Получение токенов WB
        wb_tokens = get_wb_tokens()

        # Получаем данные о всех заказах из исходных поставок
        orders_by_wild_account = await self._fetch_orders_from_supplies(request_data, wb_tokens)

        # Отбираем заказы для перемещения по времени создания
        selected_orders_for_move, participating_combinations = self._select_orders_for_move(
            request_data, orders_by_wild_account
        )

        return selected_orders_for_move, participating_combinations

    async def _create_target_supplies(self, participating_combinations: Set[Tuple[str, str]], 
                                    request_data, user: dict) -> Dict[Tuple[str, str], str]:
        """
        Создает целевые поставки для перемещения заказов.
        
        Returns:
            Dict: Словарь новых поставок {(wild_code, account): supply_id}
        """
        wb_tokens = get_wb_tokens()
        
        if getattr(request_data, 'move_to_final', False):
            logger.info("Создание финальных поставок")
            new_supplies = await self._create_or_use_final_supplies(
                participating_combinations, wb_tokens, request_data, user
            )
        else:
            logger.info("Создание висячих поставок")
            new_supplies = await self._create_new_supplies(
                participating_combinations, wb_tokens, user
            )

        if not new_supplies:
            raise HTTPException(status_code=500, detail="Не удалось создать поставки для перемещения")

        logger.info(f"Успешно создано {len(new_supplies)} поставок")
        return new_supplies

    def _determine_blocked_status(self, supplier_status: str) -> str:
        """
        Определяет конкретный статус блокировки на основе supplierStatus.

        Returns:
            OrderStatus enum значение
        """
        from src.models.order_status_log import OrderStatus

        if supplier_status == "complete":
            return OrderStatus.BLOCKED_ALREADY_DELIVERED
        elif supplier_status == "cancel":
            return OrderStatus.BLOCKED_CANCELED
        else:
            return OrderStatus.BLOCKED_INVALID_STATUS

    def _log_invalid_orders_by_status(self, invalid_orders: List[dict]) -> None:
        """Логирует невалидные заказы с группировкой по статусам."""
        logger.warning(f"\n{'='*70}")
        logger.warning(f"⚠️  ЗАКАЗЫ С НЕКОРРЕКТНЫМ СТАТУСОМ WB")
        logger.warning(f"{'='*70}")

        # Группируем по supplierStatus
        by_status = defaultdict(list)
        for inv in invalid_orders:
            # Используем правильное имя поля из структуры invalid_status_orders
            status = inv.get('blocked_supplier_status', inv.get('supplier_status', 'unknown'))
            by_status[status].append(inv)

        for status, orders in by_status.items():
            logger.warning(f"\nsupplierStatus = '{status}': {len(orders)} заказов")

            # Группируем по аккаунтам
            by_account = defaultdict(list)
            for order in orders:
                # Поддерживаем оба варианта: 'id' (invalid_status_orders) и 'order_id' (failed_movement_orders)
                order_id = order.get('id') if 'id' in order else order.get('order_id')
                by_account[order['account']].append(order_id)

            for account, order_ids in by_account.items():
                logger.warning(f"  {account}: {order_ids[:10]}")
                if len(order_ids) > 10:
                    logger.warning(f"    ... и еще {len(order_ids) - 10}")

        logger.warning(f"{'='*70}\n")

    def _log_all_failures(
        self,
        failed_orders: List[dict],
        invalid_status_orders: List[dict]
    ) -> None:
        """Логирует все неудачи с группировкой по причинам."""

        total_failures = len(failed_orders) + len(invalid_status_orders)
        if total_failures == 0:
            return

        logger.warning(f"\n{'='*70}")
        logger.warning(f"⚠️  ДЕТАЛЬНАЯ СВОДКА ПО ИСКЛЮЧЕННЫМ ЗАКАЗАМ")
        logger.warning(f"{'='*70}")
        logger.warning(f"Всего исключено из отправки в 1C: {total_failures} заказов\n")

        # 1. Невалидные статусы
        if invalid_status_orders:
            logger.warning(f"📋 Невалидный статус ({len(invalid_status_orders)} заказов):")
            logger.warning(f"   Причина: Заказы нельзя переместить из-за статуса WB")

            by_account = defaultdict(list)
            for inv in invalid_status_orders:
                by_account[inv['account']].append(inv['order_id'])

            for account, order_ids in by_account.items():
                logger.warning(f"   {account}: {len(order_ids)} заказов - {order_ids[:5]}")

        # 2. Ошибки перемещения
        if failed_orders:
            logger.warning(f"\n📋 Ошибки при перемещении ({len(failed_orders)} заказов):")

            by_reason = defaultdict(list)
            for fail in failed_orders:
                reason = fail.get('reason', 'Unknown')
                by_reason[reason].append(fail['order_id'])

            for reason, order_ids in by_reason.items():
                logger.warning(f"   {reason}: {len(order_ids)} заказов - {order_ids[:5]}")

        logger.warning(f"{'='*70}\n")

    async def _validate_orders_status_before_move(
        self,
        selected_orders: List[dict]
    ) -> Tuple[List[dict], List[dict]]:
        """
        Проверяет статусы заказов ПЕРЕД перемещением.

        Args:
            selected_orders: Все отобранные заказы для перемещения

        Returns:
            Tuple[List[dict], List[dict]]: (valid_orders, invalid_orders)
        """
        logger.info(f"Валидация статусов {len(selected_orders)} заказов перед перемещением")

        # Группируем заказы по аккаунтам
        order_ids_by_account = defaultdict(list)
        order_by_id = {}  # Для быстрого поиска

        for order in selected_orders:
            account = order['account']
            order_id = order['id']
            order_ids_by_account[account].append(order_id)
            order_by_id[order_id] = order

        # Массовая проверка статусов по всем аккаунтам
        wb_tokens = get_wb_tokens()
        validation_results = {}

        for account, order_ids in order_ids_by_account.items():
            try:
                orders_api = Orders(account, wb_tokens[account])

                # Разбиваем на батчи по 1000 заказов (лимит WB API)
                batch_size = 1000
                for i in range(0, len(order_ids), batch_size):
                    batch = order_ids[i:i + batch_size]
                    logger.debug(
                        f"Проверка статусов батча {i//batch_size + 1} "
                        f"({len(batch)} заказов) для {account}"
                    )
                    result = await orders_api.can_add_to_supply_batch(batch)
                    validation_results.update(result)

                logger.info(
                    f"Проверено {len(order_ids)} заказов для {account} "
                    f"в {(len(order_ids) - 1) // batch_size + 1} батчах"
                )
            except Exception as e:
                logger.error(f"Ошибка валидации для {account}: {e}")
                # Помечаем все заказы аккаунта как невалидные
                for order_id in order_ids:
                    validation_results[order_id] = {
                        "can_add": False,
                        "supplier_status": "error",
                        "wb_status": "error"
                    }

        # Разделяем на валидные и невалидные
        valid_orders = []
        invalid_orders = []

        for order_id, status_info in validation_results.items():
            order = order_by_id.get(order_id)
            if not order:
                continue

            can_add = status_info.get("can_add", False)
            supplier_status = status_info.get("supplier_status", "unknown")
            wb_status = status_info.get("wb_status", "unknown")

            if can_add:
                valid_orders.append(order)
            else:
                # Определяем конкретный статус блокировки
                blocked_status = self._determine_blocked_status(supplier_status)

                # Сохраняем ПОЛНЫЙ объект заказа + информацию о блокировке
                # Это нужно для отправки в 1C/Shipment с оригинальным supply_id
                invalid_orders.append({
                    **order,  # Все поля оригинального заказа
                    'blocked_status': blocked_status,  # Для логирования
                    'blocked_supplier_status': supplier_status,
                    'blocked_wb_status': wb_status,
                    'blocked_reason': f"supplierStatus={supplier_status}, wbStatus={wb_status}"
                })

        logger.info(
            f"Валидация: {len(valid_orders)} валидных, "
            f"{len(invalid_orders)} невалидных"
        )

        # Детальное логирование
        if invalid_orders:
            self._log_invalid_orders_by_status(invalid_orders)

        # НОРМАЛИЗАЦИЯ: Добавляем 'order_id' для совместимости с методами отправки в 1C/Shipment
        # Заблокированные заказы имеют ключ 'id', но многие методы ожидают 'order_id'
        # Делаем это ОДИН РАЗ здесь, чтобы все последующие методы работали единообразно
        for invalid_order in invalid_orders:
            if 'order_id' not in invalid_order:
                invalid_order['order_id'] = invalid_order['id']

        return valid_orders, invalid_orders

    async def _execute_orders_move(self, selected_orders_for_move: List[dict],
                                 new_supplies: Dict[Tuple[str, str], str]) -> Tuple[List[int], List[dict], List[dict]]:
        """
        Выполняет перемещение заказов в новые поставки с предварительной валидацией статусов.

        Returns:
            Tuple[List[int], List[dict], List[dict]]: (ID успешно перемещенных заказов,
                                                        заказы с невалидным статусом,
                                                        заказы с ошибками при перемещении)
        """
        logger.info(f"Начало перемещения {len(selected_orders_for_move)} заказов в новые поставки")

        # ШАГ 1: Предварительная валидация статусов всех заказов
        logger.info("=== ШАГ 1: Проверка статусов заказов перед перемещением ===")
        valid_orders, invalid_status_orders = await self._validate_orders_status_before_move(
            selected_orders_for_move
        )

        logger.info(
            f"Результат валидации: валидных={len(valid_orders)}, "
            f"с невалидным статусом={len(invalid_status_orders)}"
        )

        # Логируем заказы с невалидными статусами
        if invalid_status_orders:
            self._log_invalid_orders_by_status(invalid_status_orders)

        # ШАГ 2: Перемещаем только валидные заказы
        logger.info("=== ШАГ 2: Перемещение валидных заказов ===")
        wb_tokens = get_wb_tokens()

        if valid_orders:
            # check_status=False, т.к. мы уже сделали пре-валидацию
            moved_order_ids, failed_movement_orders = await self._move_orders_to_supplies(
                valid_orders, new_supplies, wb_tokens, check_status=False
            )
        else:
            logger.warning("Нет валидных заказов для перемещения после проверки статусов")
            moved_order_ids = []
            failed_movement_orders = []

        # ШАГ 3: Логируем итоговую статистику
        logger.info(
            f"=== ИТОГО ПЕРЕМЕЩЕНИЕ ===\n"
            f"  Всего заказов: {len(selected_orders_for_move)}\n"
            f"  Успешно перемещено: {len(moved_order_ids)}\n"
            f"  Невалидный статус WB: {len(invalid_status_orders)}\n"
            f"  Ошибки при перемещении: {len(failed_movement_orders)}\n"
            f"  Всего неудач: {len(invalid_status_orders) + len(failed_movement_orders)}"
        )

        # Подробный лог всех ошибок
        if invalid_status_orders or failed_movement_orders:
            self._log_all_failures(failed_movement_orders, invalid_status_orders)

        return moved_order_ids, invalid_status_orders, failed_movement_orders

    async def _process_external_systems_integration(
        self,
        request_data,
        selected_orders_for_move: List[dict],
        moved_order_ids: List[int],
        new_supplies: Dict[Tuple[str, str], str],
        user: dict,
        invalid_status_orders: List[dict] = None,
        failed_movement_orders: List[dict] = None
    ) -> Tuple[Optional[bool], int]:
        """
        Обрабатывает интеграцию с внешними системами.
        - Для финальных: снятие резерва + отправка в 1C (успешно перемещённые + заблокированные)
        - Для висячих: создание резерва с перемещением (только успешно перемещённые)

        Args:
            invalid_status_orders: Заказы с невалидным статусом (для финального режима)
            failed_movement_orders: Заказы с ошибкой перемещения (НЕ отправляются)

        Returns:
            Tuple[Optional[bool], int]: (shipment_success для финального режима или None, количество подготовленных заблокированных заказов)
        """
        if invalid_status_orders is None:
            invalid_status_orders = []
        if failed_movement_orders is None:
            failed_movement_orders = []
        # Фильтруем только успешно перемещенные заказы
        successfully_moved_orders = [
            order for order in selected_orders_for_move
            if order['id'] in moved_order_ids
        ]

        logger.info(
            f"Интеграция с внешними системами: "
            f"всего отобрано {len(selected_orders_for_move)}, "
            f"успешно перемещено {len(successfully_moved_orders)}, "
            f"заблокировано {len(invalid_status_orders)}"
        )

        if not successfully_moved_orders and not invalid_status_orders:
            logger.warning("⚠️ Нет заказов для интеграции с внешними системами")
            return None, 0

        if getattr(request_data, 'move_to_final', False):
            logger.info("=== РЕЖИМ: ПЕРЕВОД В ФИНАЛЬНЫЙ КРУГ ===")

            # 1. НОВОЕ: Снимаем резерв с исходных поставок (только для успешно перемещенных)
            if successfully_moved_orders:
                shipped_goods_response = await self._release_reserve_for_final_move(
                    successfully_moved_orders
                )
                logger.info(f"Снято резервов: {len(shipped_goods_response)}")

            # 2. НОВОЕ: Подготавливаем заблокированные заказы для отгрузки (с оригинальным supply_id)
            # Важно: failed_movement_orders НЕ включаем, т.к. неясно их состояние
            blocked_orders_for_shipment = self._prepare_blocked_orders_for_shipment(
                invalid_status_orders,
                []  # failed_movement_orders не отгружаем
            )

            logger.info(
                f"Подготовлено для отгрузки: "
                f"{len(successfully_moved_orders)} успешно перемещённых + "
                f"{len(blocked_orders_for_shipment)} заблокированных = "
                f"{len(successfully_moved_orders) + len(blocked_orders_for_shipment)} всего"
            )

            # 3. Обновляем supply_id для успешно перемещённых (на новые поставки)
            updated_moved_orders = self._update_orders_with_new_supply_ids(
                successfully_moved_orders, new_supplies
            )

            # 4. НОВОЕ: Объединяем обе группы для отправки в 1C/Shipment
            all_orders_for_shipment = updated_moved_orders + blocked_orders_for_shipment

            # 5. НОВОЕ: Создаём supplies_dict с ОБОИМИ типами поставок (новые + старые)
            supplies_dict = {
                supply_id: account
                for (wild_code, account), supply_id in new_supplies.items()
            }

            # Добавляем старые supply_id из заблокированных заказов
            for order in blocked_orders_for_shipment:
                old_supply_id = order.get('supply_id')
                account = order.get('account')
                if old_supply_id and account and old_supply_id not in supplies_dict:
                    supplies_dict[old_supply_id] = account
                    logger.debug(f"Добавлен старый supply_id в словарь: {old_supply_id} ({account})")

            logger.info(
                f"Отправка в 1C/Shipment: "
                f"{len(all_orders_for_shipment)} заказов, "
                f"{len(supplies_dict)} уникальных поставок"
            )

            # 6. Отправляем данные в 1C + shipment API (обе группы)
            shipment_success = await self._send_shipment_data_to_external_systems(
                all_orders_for_shipment,
                supplies_dict,
                user.get('username', 'unknown')
            )

            if shipment_success:
                logger.info("✅ Данные об отгрузке успешно отправлены в внешние системы")
            else:
                logger.warning("⚠️ Не удалось отправить данные об отгрузке в внешние системы")

            # Возвращаем результат отгрузки и количество подготовленных заблокированных заказов
            return shipment_success, len(blocked_orders_for_shipment)
        else:
            logger.info("=== РЕЖИМ: ПЕРЕВОД В ВИСЯЧИЙ ===")

            # НОВОЕ: Создаем резерв с перемещением для висячих поставок (только для успешно перемещенных)
            reserve_success = await self._create_reserve_with_movement_for_wilds(
                successfully_moved_orders,
                new_supplies,
                user
            )

            if reserve_success:
                logger.info("✅ Резерв с перемещением успешно создан для висячих поставок")
            else:
                logger.warning("⚠️ Не удалось создать резерв с перемещением")

            # В висячем режиме заблокированные заказы не отгружаются
            return None, 0

    async def _create_reserve_with_movement_for_wilds(
        self,
        selected_orders: List[dict],
        new_supplies: Dict[Tuple[str, str], str],
        user: dict
    ) -> bool:
        """
        Создает резерв с перемещением для висячих поставок через API.
        Отправляет последовательно для каждой комбинации (wild, account, original_supply).

        Args:
            selected_orders: Отобранные заказы для перемещения
            new_supplies: Новые поставки {(wild_code, account): supply_id}
            user: Данные пользователя

        Returns:
            bool: True если все запросы успешны
        """
        logger.info("=== СОЗДАНИЕ РЕЗЕРВА С ПЕРЕМЕЩЕНИЕМ ДЛЯ ВИСЯЧИХ ПОСТАВОК ===")

        # Группируем заказы по (wild, account, original_supply_id)
        grouped_data = defaultdict(lambda: {
            "orders": [],
            "new_supply_id": None,
            "account": None
        })

        for order in selected_orders:
            wild_code = order['wild_code']
            account = order['account']
            original_supply_id = order.get('original_supply_id')

            key = (wild_code, account, original_supply_id)
            grouped_data[key]["orders"].append(order)
            grouped_data[key]["new_supply_id"] = new_supplies.get((wild_code, account))
            grouped_data[key]["account"] = account

        # Формируем данные для каждой группы
        reservation_data_list = []

        for (wild_code, account, original_supply_id), group_info in grouped_data.items():
            quantity_to_move = len(group_info["orders"])
            new_supply_id = group_info["new_supply_id"]

            if not new_supply_id:
                logger.warning(f"Пропуск: нет новой поставки для {wild_code}, {account}")
                continue

            if not original_supply_id:
                logger.warning(f"Пропуск: нет исходной поставки для {wild_code}, {account}")
                continue

            # Генерируем даты резерва
            from src.orders.orders import OrdersService
            reserve_date, expires_at = OrdersService._generate_reservation_dates()

            reservation_item = {
                "product_id": wild_code,
                "warehouse_id": settings.PRODUCT_RESERVATION_WAREHOUSE_ID,
                "ordered": quantity_to_move,
                "account": account,
                "delivery_type": settings.PRODUCT_RESERVATION_DELIVERY_TYPE,
                "wb_warehouse": None,
                "reserve_date": reserve_date,
                "supply_id": new_supply_id,  # НОВАЯ висячая поставка
                "expires_at": expires_at,
                "is_hanging": True,  # Это висячая поставка
                "move_from_supply": original_supply_id,  # Откуда перемещаем
                "quantity_to_move": quantity_to_move  # Сколько перемещаем
            }

            reservation_data_list.append(reservation_item)

            logger.info(
                f"📦 Резерв с перемещением: {wild_code} | "
                f"из {original_supply_id} → {new_supply_id} | "
                f"количество: {quantity_to_move}"
            )

        if not reservation_data_list:
            logger.warning("Нет данных для создания резерва с перемещением")
            return False

        # Отправляем в API
        return await self._send_creation_reserve_with_movement(reservation_data_list)

    async def _send_creation_reserve_with_movement(
        self,
        reservation_data: List[Dict[str, Any]]
    ) -> bool:
        """
        Отправляет запрос на создание резерва с перемещением.

        Args:
            reservation_data: Список данных для резервирования

        Returns:
            bool: True если успешно
        """
        try:
            # Формируем URL (заменяем /create_reserve на /creation_reserve_with_movement)
            base_url = settings.PRODUCT_RESERVATION_API_URL.replace('/create_reserve', '')
            api_url = f"{base_url}/creation_reserve_with_movement"

            # Добавляем delivery_type как query parameter (требование API)
            url_with_params = f"{api_url}?delivery_type={settings.PRODUCT_RESERVATION_DELIVERY_TYPE}"

            logger.info(f"📡 Отправка запроса: {url_with_params}")
            logger.debug(f"📄 Данные: {json.dumps(reservation_data, ensure_ascii=False, indent=2)}")

            response = None
            #     await self.async_client.post(
            #     url=url_with_params,
            #     json=reservation_data,
            #     headers={"Content-Type": "application/json"}
            # )

            if response:
                logger.info(f"✅ Резерв с перемещением создан. Ответ: {response}")
                return True
            else:
                logger.error("❌ Получен пустой ответ от API creation_reserve_with_movement")
                return False

        except Exception as e:
            logger.error(f"❌ Ошибка создания резерва с перемещением: {str(e)}")
            return False

    async def _release_reserve_for_final_move(
        self,
        selected_orders: List[dict]
    ) -> List[Dict[str, Any]]:
        """
        Снимает резерв при переводе заказов в финальный круг через API add_shipped_goods.
        Группирует по (original_supply_id, wild) и отправляет последовательно.

        Args:
            selected_orders: Отобранные заказы для перевода в финальный

        Returns:
            List[Dict[str, Any]]: Ответ от API с product_reserves_id
        """
        logger.info("=== СНЯТИЕ РЕЗЕРВА ПРИ ПЕРЕВОДЕ В ФИНАЛЬНЫЙ КРУГ ===")

        # Группируем по (original_supply_id, wild_code)
        grouped_data = defaultdict(lambda: {
            "wild_code": None,
            "orders": []
        })

        for order in selected_orders:
            original_supply_id = order.get('original_supply_id')
            wild_code = order['wild_code']

            if not original_supply_id:
                logger.warning(f"Пропуск: нет original_supply_id для заказа {order.get('id')}")
                continue

            key = (original_supply_id, wild_code)
            grouped_data[key]["wild_code"] = wild_code
            grouped_data[key]["orders"].append(order)

        # Формируем данные для add_shipped_goods
        shipped_goods_data = []

        for (original_supply_id, wild_code), group_info in grouped_data.items():
            quantity_shipped = len(group_info["orders"])

            shipped_goods_item = {
                "supply_id": original_supply_id,  # ИСХОДНАЯ поставка
                "quantity_shipped": quantity_shipped,
                "product_id": wild_code  # product_id обязателен
            }

            shipped_goods_data.append(shipped_goods_item)
            logger.info(
                f"🔓 Снятие резерва: {original_supply_id} | "
                f"wild: {wild_code} | количество: {quantity_shipped}"
            )

        if not shipped_goods_data:
            logger.warning("Нет данных для снятия резерва")
            return []

        # Отправляем в API (используем существующий метод)
        return await self._send_shipped_goods_to_api(shipped_goods_data)

    def _update_orders_with_new_supply_ids(self, selected_orders: List[dict], 
                                         new_supplies: Dict[Tuple[str, str], str]) -> List[dict]:
        """
        Обновляет supply_id в заказах на новые целевые поставки.
        
        Args:
            selected_orders: Исходные заказы со старыми supply_id
            new_supplies: Маппинг {(wild_code, account): new_supply_id}
            
        Returns:
            List[dict]: Заказы с обновленными supply_id
        """
        updated_orders = []
        
        for order in selected_orders:
            updated_order = order.copy()
            
            # Добавляем supply_id из original_supply_id если нет
            if 'supply_id' not in updated_order:
                updated_order['supply_id'] = updated_order.get('original_supply_id', '')
            
            # Обновляем на новый supply_id
            key = (order['wild_code'], order['account'])
            if key in new_supplies:
                updated_order['supply_id'] = new_supplies[key]
                logger.debug(f"Обновлен supply_id для заказа {order['id']}: {order.get('original_supply_id', 'N/A')} -> {new_supplies[key]}")
            else:
                logger.warning(f"Не найдено новое supply_id для заказа {order['id']} ({key})")
                
            updated_orders.append(updated_order)
        
        return updated_orders

    def _prepare_blocked_orders_for_shipment(
        self,
        invalid_status_orders: List[dict],
        failed_movement_orders: List[dict]
    ) -> List[dict]:
        """
        Подготавливает заблокированные заказы для отгрузки с их ОРИГИНАЛЬНЫМ supply_id.

        Эти заказы не смогли переместиться в новую поставку, но их всё равно нужно
        отгрузить в 1C/Shipment с номером той поставки, где они изначально находились.

        Args:
            invalid_status_orders: Заказы с невалидным статусом (complete/cancel и т.д.)
            failed_movement_orders: Заказы, которые упали при попытке перемещения

        Returns:
            List[dict]: Заказы с оригинальным supply_id, готовые для отгрузки
        """
        blocked_orders = []

        # Объединяем обе группы заблокированных заказов
        all_blocked = invalid_status_orders + failed_movement_orders

        for order in all_blocked:
            prepared_order = order.copy()

            # Убеждаемся что supply_id есть (используем original_supply_id)
            if 'supply_id' not in prepared_order:
                prepared_order['supply_id'] = prepared_order.get('original_supply_id', '')

            # Если supply_id пустой, используем original_supply_id
            if not prepared_order.get('supply_id'):
                prepared_order['supply_id'] = prepared_order.get('original_supply_id', '')

            # Поддерживаем оба варианта ключа для логирования
            order_id = order.get('id') if 'id' in order else order.get('order_id')

            # Проверяем критичную ситуацию: отсутствие supply_id
            if not prepared_order.get('supply_id'):
                logger.error(
                    f"❌ КРИТИЧНО: Заказ {order_id} не может быть отгружен - "
                    f"отсутствует supply_id и original_supply_id! "
                    f"Это приведёт к некорректному учёту остатков!"
                )
                continue  # Пропускаем такой заказ

            blocked_orders.append(prepared_order)

            logger.debug(
                f"Заказ {order_id} подготовлен для отгрузки "
                f"с оригинальным supply_id={prepared_order.get('supply_id')}"
            )

        logger.info(
            f"Подготовлено {len(blocked_orders)} заблокированных заказов "
            f"для отгрузки с оригинальными supply_id"
        )

        return blocked_orders

    def _create_empty_result(self, message: str) -> Dict[str, Any]:
        """Создает результат для случая отсутствия заказов."""
        return {
            "success": False,
            "message": message,
            "removed_order_ids": [],
            "processed_supplies": 0,
            "processed_wilds": 0,
            # Статистика (все нули для пустого результата)
            "total_orders": 0,
            "successful_count": 0,
            "invalid_status_count": 0,
            "blocked_but_shipped_count": 0,
            "failed_movement_count": 0,
            "total_failed_count": 0
        }

    def _create_success_result(self, moved_order_ids: List[int],
                             new_supplies: Dict[Tuple[str, str], str],
                             selected_orders_for_move: List[dict],
                             invalid_status_orders: List[dict],
                             failed_movement_orders: List[dict],
                             move_to_final: bool,
                             shipment_success: Optional[bool],
                             blocked_prepared_count: int) -> Dict[str, Any]:
        """
        Создает успешный результат операции с полной статистикой.

        Args:
            moved_order_ids: ID успешно перемещенных заказов
            new_supplies: Созданные целевые поставки
            selected_orders_for_move: Все отобранные для перемещения заказы
            invalid_status_orders: Заказы с невалидным статусом WB
            failed_movement_orders: Заказы с ошибками при перемещении
            move_to_final: Режим финальной поставки
            shipment_success: Успешность отгрузки в 1C/Shipment (только для финального режима)
            blocked_prepared_count: Количество реально подготовленных заблокированных заказов

        Returns:
            Dict с результатами операции и статистикой
        """
        total_orders = len(selected_orders_for_move)
        successful_count = len(moved_order_ids)
        invalid_status_count = len(invalid_status_orders)
        failed_movement_count = len(failed_movement_orders)
        total_failed = invalid_status_count + failed_movement_count

        # ИСПРАВЛЕНО: Заблокированные заказы отгружаются ТОЛЬКО в финальном режиме
        # И только те, которые реально были подготовлены (с валидным supply_id)
        if move_to_final:
            blocked_but_shipped_count = blocked_prepared_count  # Реальное количество подготовленных
        else:
            blocked_but_shipped_count = 0  # В висячем режиме не отгружаем

        logger.info(
            f"=== ИТОГОВАЯ СТАТИСТИКА ПЕРЕМЕЩЕНИЯ ===\n"
            f"  Всего заказов: {total_orders}\n"
            f"  Успешно перемещено: {successful_count}\n"
            f"  Невалидный статус WB: {invalid_status_count}\n"
            f"  Заблокировано но отгружено: {blocked_but_shipped_count}\n"
            f"  Ошибки при перемещении: {failed_movement_count}\n"
            f"  Всего неудач: {total_failed}"
        )

        # Формируем детали перемещенных заказов для внутреннего использования (логирование статусов)
        moved_orders_details = []
        for order in selected_orders_for_move:
            if order['id'] in moved_order_ids:  # Только успешно перемещенные
                key = (order['wild_code'], order['account'])
                moved_orders_details.append({
                    'order_id': order['id'],
                    'supply_id': new_supplies.get(key),
                    'account': order['account'],
                    'wild': order['wild_code']
                })

        # Определяем сообщение с учетом ошибок
        if total_failed == 0:
            message = f"✅ Все заказы ({successful_count}) успешно перемещены"
        else:
            message = (
                f"⚠️ Перемещено {successful_count} из {total_orders} заказов. "
                f"Не перемещено: {total_failed} (невалидный статус: {invalid_status_count}, "
                f"ошибки перемещения: {failed_movement_count})"
            )

        # Формируем список removed_order_ids с учётом заблокированных заказов в финальном режиме
        final_removed_order_ids = moved_order_ids.copy()

        # В финальном режиме, если отгрузка успешна, добавляем заблокированные заказы
        if move_to_final and shipment_success:
            blocked_order_ids = []
            # Добавляем ID из invalid_status_orders
            for order in invalid_status_orders:
                order_id = order.get('order_id', order.get('id'))
                if order_id:
                    blocked_order_ids.append(order_id)
            # Добавляем ID из failed_movement_orders
            for order in failed_movement_orders:
                order_id = order.get('order_id', order.get('id'))
                if order_id:
                    blocked_order_ids.append(order_id)

            final_removed_order_ids.extend(blocked_order_ids)
            logger.info(f"Добавлено {len(blocked_order_ids)} заблокированных заказов в removed_order_ids (финальный режим, успешная отгрузка)")

        return {
            "success": True,
            "message": message,
            "removed_order_ids": final_removed_order_ids,
            "processed_supplies": len(new_supplies),
            "processed_wilds": len({order['wild_code'] for order in selected_orders_for_move}),
            # Статистика (вместо подробных списков заказов)
            "total_orders": total_orders,
            "successful_count": successful_count,
            "invalid_status_count": invalid_status_count,
            "blocked_but_shipped_count": blocked_but_shipped_count,
            "failed_movement_count": failed_movement_count,
            "total_failed_count": total_failed,
            # Внутренние поля для логирования (не включаются в API response)
            "_moved_orders_details": moved_orders_details,
            "_invalid_status_orders": invalid_status_orders,
            "_failed_movement_orders": failed_movement_orders,
            "_shipment_success": shipment_success  # Успешность отгрузки в 1C/Shipment (только для финального режима)
        }

    def _group_orders_by_supply(self, selected_orders: List[dict]) -> Tuple[Dict[str, dict], Dict[str, str]]:
        """Группирует заказы по поставкам и создает маппинг заказов."""
        supply_orders = defaultdict(lambda: {"order_ids": [], "account": None})
        order_wild_map = {}

        for order in selected_orders:
            supply_id = order["supply_id"]
            # Поддерживаем оба варианта ключа (для заблокированных заказов - 'id', для обычных - 'order_id')
            order_id = order.get('id') if 'id' in order else order.get('order_id')
            supply_orders[supply_id]["order_ids"].append(order_id)
            supply_orders[supply_id]["account"] = order["account"]
            order_wild_map[str(order_id)] = process_local_vendor_code(order["article"])

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
                    nm_id=order["nm_id"],
                    createdAt=order["createdAt"]
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

        integration = OneCIntegration(self.db)
        integration_result = await integration.format_delivery_data(delivery_supplies, order_wild_map)
        integration_success = isinstance(integration_result, dict) and integration_result.get("code") == 200

        if not integration_success:
            logger.error(f"Ошибка интеграции с 1C: {integration_result}")

        if not skip_shipment_api:
            shipment_result = await self.save_shipments(delivery_supplies, order_wild_map,
                                                        user.get('username', 'unknown'))
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

            # 1. Создаем ЧЕРНОВЫЕ поставки (БЕЗ перемещения заказов)
            logger.info(f"=== СОЗДАНИЕ ЧЕРНОВЫХ ПОСТАВОК ДЛЯ ПОЛУЧЕНИЯ СТИКЕРОВ ===")

            orders_by_account = defaultdict(list)
            for order in selected_orders:
                account = order["account"]
                orders_by_account[account].append(order)

            new_supplies_map = {}
            wb_tokens = get_wb_tokens()

            for account, orders in orders_by_account.items():
                timestamp = datetime.now().strftime("%d.%m.%Y_%H:%M")
                supply_name = f"Факт_{target_article}_{timestamp}_{user.get('username', 'auto')}"

                logger.info(f"Создание черновой поставки '{supply_name}' для {account}")

                supplies_api = Supplies(account, wb_tokens[account])
                create_response = await supplies_api.create_supply(supply_name)

                if create_response.get("errors"):
                    raise HTTPException(
                        status_code=500,
                        detail=f"Ошибка создания поставки для {account}: {create_response['errors']}"
                    )

                new_supply_id = create_response.get("id")
                if not new_supply_id:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Не получен ID новой поставки для {account}"
                    )

                logger.info(f"Создана черновая поставка {new_supply_id} для {account}")
                new_supplies_map[account] = new_supply_id

            # 2. Получаем стикеры ДО перемещения заказов
            logger.info(f"=== ПОЛУЧЕНИЕ СТИКЕРОВ ДЛЯ {len(selected_orders)} ЗАКАЗОВ ===")

            orders_by_supply = defaultdict(list)
            for order in selected_orders:
                account = order["account"]
                if account in new_supplies_map:
                    new_supply_id = new_supplies_map[account]
                    orders_by_supply[(account, new_supply_id)].append(order)

            supplies_list = []
            for (account, supply_id), orders in orders_by_supply.items():
                order_schemas = [
                    OrderSchema(
                        order_id=order["order_id"],
                        nm_id=order.get("nm_id", 0),
                        local_vendor_code=target_article,
                        createdAt=order.get("createdAt", "")
                    )
                    for order in orders
                ]

                supplies_list.append(
                    SupplyId(
                        name="",
                        createdAt="",
                        supply_id=supply_id,
                        account=account,
                        count=len(order_schemas),
                        orders=order_schemas
                    )
                )

            if not supplies_list:
                raise HTTPException(
                    status_code=400,
                    detail="Не удалось подготовить данные для получения стикеров"
                )

            supply_ids_schema = SupplyIdBodySchema(supplies=supplies_list)
            stickers_raw = await self.get_stickers(supply_ids_schema)
            stickers_grouped = self.group_result(stickers_raw)

            # 3. Извлекаем _received_order_ids
            received_order_ids = set()
            for account_data in stickers_grouped.values():
                for sticker_data in account_data.values():
                    received_ids = sticker_data.get('_received_order_ids', [])
                    received_order_ids.update(received_ids)

            logger.info(f"Стикеры получены: {len(received_order_ids)} из {len(selected_orders)} заказов")

            # 4. Фильтруем заказы - отгружаем ТОЛЬКО те что получили стикеры
            orders_with_stickers = [
                order for order in selected_orders
                if order['order_id'] in received_order_ids
            ]

            orders_without_stickers = [
                order for order in selected_orders
                if order['order_id'] not in received_order_ids
            ]

            # 5. Обработка случаев без стикеров
            if not orders_with_stickers:
                logger.error(
                    f"❌ КРИТИЧЕСКАЯ ОШИБКА: Ни один из {len(selected_orders)} заказов "
                    f"не получил стикеры от WB API"
                )
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Невозможно выполнить фактическую отгрузку: "
                        f"ни один из {len(selected_orders)} заказов не получил стикеры от WB API. "
                        f"Возможные причины: заказы больше не в статусе 'confirm', проблемы с WB API. "
                        f"Черновые поставки {list(new_supplies_map.values())} будут автоматически удалены."
                    )
                )

            if orders_without_stickers:
                logger.warning(
                    f"⚠️ ЧАСТИЧНАЯ ОТГРУЗКА: {len(orders_without_stickers)} заказов не получили стикеры. "
                    f"Будет отгружено только {len(orders_with_stickers)} заказов. "
                    f"Заказы без стикеров: {[o['order_id'] for o in orders_without_stickers]}"
                )

            logger.info(f"Продолжаем с {len(orders_with_stickers)} заказами которые получили стикеры")

            # 6. Перемещаем ТОЛЬКО заказы со стикерами
            logger.info(f"=== ПЕРЕМЕЩЕНИЕ ЗАКАЗОВ СО СТИКЕРАМИ В ПОСТАВКИ ===")
            for account, orders in orders_by_account.items():
                if account not in new_supplies_map:
                    continue

                supply_id = new_supplies_map[account]
                supplies_api = Supplies(account, wb_tokens[account])

                # Фильтруем только заказы со стикерами для этого аккаунта
                orders_to_move = [o for o in orders if o['order_id'] in received_order_ids]

                logger.info(f"Перемещение {len(orders_to_move)} заказов в поставку {supply_id} ({account})")

                for order in orders_to_move:
                    order_id = order["order_id"]
                    await supplies_api.add_order_to_supply(supply_id, order_id)
                    logger.debug(f"Заказ {order_id} перемещен в поставку {supply_id}")

            # 7. Переводим новые поставки в статус доставки
            await self._deliver_new_supplies(new_supplies_map)

            # 8. Обновляем данные заказов с новыми supply_id (ТОЛЬКО orders_with_stickers!)
            updated_selected_orders = self._update_orders_with_new_supplies(orders_with_stickers, new_supplies_map)
            updated_grouped_orders = self.group_selected_orders_by_supply(updated_selected_orders)

            # 9. Подготавливаем данные для 1C и shipment_goods
            delivery_supplies, order_wild_map = self.prepare_data_for_delivery_optimized(updated_selected_orders)

            # 10. Обновляем висячие поставки (используем orders_with_stickers для правильного подсчета)
            grouped_orders_with_stickers = self.group_selected_orders_by_supply(orders_with_stickers)
            shipped_goods_response = await self._update_hanging_supplies_shipped_quantities(grouped_orders_with_stickers)

            # 11. Отправляем данные в shipment API с product_reserves_id (ТОЛЬКО orders_with_stickers!)
            logger.info(
                f"Отправка данных в shipment API с product_reserves_id и автором '{user.get('username', 'unknown')}'")
            await self._send_enhanced_shipment_data(updated_selected_orders, shipped_goods_response, user)

            # 6.1. Логируем статус PARTIALLY_SHIPPED для частично отгруженных заказов
            if self.db:
                from src.orders.order_status_service import OrderStatusService

                # Подготавливаем данные для логирования
                partially_shipped_data = []
                for order in updated_selected_orders:
                    partially_shipped_data.append({
                        'order_id': order['order_id'],
                        'supply_id': order.get('supply_id'),  # Новый supply_id
                        'account': order['account']
                    })

                status_service = OrderStatusService(self.db)
                logged_count = await status_service.process_and_log_partially_shipped(partially_shipped_data)
                logger.info(f"Залогировано {logged_count} заказов со статусом PARTIALLY_SHIPPED")

            # 6.2. ВАЖНО: НЕ сохраняем фактические поставки как висячие
            # Причина: Реально отгруженные поставки не являются висячими по определению.
            # После перевода в доставку они:
            #   - Находятся в пути к WB (статус "В доставке" в WB API)
            #   - Резерв уже списан (через add_shipped_goods API)
            #   - Данные зафиксированы в shipment_of_goods и 1C
            # Сохранение их в hanging_supplies приводит к:
            #   - Риску повторной фиктивной доставки/отгрузки
            #   - Двойному списанию резерва
            #   - Путанице для операторов (реальные поставки в списке висячих)
            logger.info(f"Фактические поставки {list(new_supplies_map.values())} НЕ сохраняются как висячие (уже реально отгружены)")

            # 12. Отправляем в 1C (БЕЗ повторной отправки в shipment API)
            integration_result, success = await self._process_shipment(updated_grouped_orders, delivery_supplies,
                                                                       order_wild_map, user, skip_shipment_api=True)

            # 13. Генерируем PDF переиспользуя УЖЕ полученные стикеры
            logger.info(f"=== ГЕНЕРАЦИЯ PDF ИЗ УЖЕ ПОЛУЧЕННЫХ СТИКЕРОВ ===")
            try:
                self.union_results_stickers(supply_ids_schema, stickers_grouped)
                grouped_stickers = await self.group_orders_to_wild(supply_ids_schema)
                stickers_pdf = await collect_images_sticker_to_pdf(grouped_stickers)
                pdf_stickers = base64.b64encode(stickers_pdf.getvalue()).decode('utf-8')
                logger.info(f"PDF стикеры сгенерированы для {len(grouped_stickers)} wild-кодов")
            except Exception as e:
                logger.error(f"Ошибка генерации PDF стикеров: {str(e)}")
                pdf_stickers = ""

            response_data = {
                "success": success,
                "message": (
                    f"Отгрузка выполнена: {len(orders_with_stickers)} заказов отгружено"
                    + (f", {len(orders_without_stickers)} заказов без стикеров пропущено" if orders_without_stickers else "")
                ),
                "processed_orders": len(orders_with_stickers),
                "processed_supplies": len(updated_grouped_orders),
                "target_article": target_article,
                "shipped_count": supply_data.shipped_count,
                "orders_without_stickers_count": len(orders_without_stickers),
                "operator": user.get('username', 'unknown'),
                "qr_codes": pdf_stickers,
                "integration_result": integration_result,
                "shipment_result": success,
                "new_supplies": list(new_supplies_map.values())
            }

            logger.info(
                f"Отгрузка фактического количества завершена: {len(orders_with_stickers)} заказов отгружено, "
                f"{len(orders_without_stickers)} без стикеров пропущено, "
                f"создано {len(new_supplies_map)} новых поставок"
            )
            return response_data

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отгрузке фактического количества: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")

    async def _create_and_transfer_orders(self, selected_orders: List[dict], target_article: str, user: dict) -> Dict[
        str, str]:
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
                raise HTTPException(status_code=500,
                                    detail=f"Ошибка создания поставки для {account}: {create_response['errors']}")

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

    def _update_orders_with_new_supplies(self, selected_orders: List[dict], new_supplies_map: Dict[str, str]) -> List[
        dict]:
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

    async def _update_hanging_supplies_shipped_quantities(self, grouped_orders: Dict[str, List[dict]]) -> List[
        Dict[str, Any]]:
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
        Подготавливает данные об отгруженных количествах для API add_shipped_goods.
        Висячая поставка = один wild, берем product_id из первого заказа.

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

            # Висячая поставка = один wild, получаем product_id из первого заказа
            product_id = process_local_vendor_code(orders[0].get("article", ""))

            shipped_goods_item = {
                "supply_id": supply_id,
                "quantity_shipped": quantity_shipped,
                "product_id": product_id  # Добавлено для корректного снятия резерва
            }

            shipped_goods_data.append(shipped_goods_item)
            logger.debug(
                f"Подготовлены данные для поставки {supply_id}, "
                f"product_id {product_id}: отгружено {quantity_shipped} заказов"
            )

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

            response = None
            #     await self.async_client.post(
            #     url=api_url,
            #     json=shipped_goods_data,
            #     headers={"Content-Type": "application/json"}
            # )

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
            # Сохраняем маппинг order_id -> wild для order_wild_map с унификацией через process_local_vendor_code
            order_wild_map[str(order.get("order_id"))] = process_local_vendor_code(order.get("article"))

        # Создаем объекты DeliverySupplyInfo
        for (supply_id, account), order_ids in orders_by_supply.items():
            delivery_supply = type('DeliverySupplyInfo', (), {
                'supply_id': supply_id,
                'account': account,
                'order_ids': order_ids
            })()
            delivery_supplies.append(delivery_supply)

        return delivery_supplies, order_wild_map

    async def _get_base_shipment_data(self, delivery_supplies: List, order_wild_map: Dict[str, str], user: dict) -> \
            List[Dict[str, Any]]:
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
                    logger.debug(
                        f"Добавлен product_reserves_id={reserves_mapping[original_supply_id]} для supply_id {supply_id}")

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
        logger.info(
            f"Отфильтровано записей для висячих: {len(enhanced_shipment_data)} -> {len(filtered_shipment_data)}")

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

    async def get_single_supply_sticker(self, supply_id: str, account: str) -> BytesIO:
        """
                Get PNG sticker for supply.

                Args:
                    supply_id: Order ID
                    account: Account name

                Returns:
                    BytesIO: PNG sticker data
                """
        try:
            # Get tokens
            tokens = get_wb_tokens()
            if account not in tokens:
                raise ValueError(f"Account not found: {account}")

            # Create WB orders client
            wb_orders = Supplies(account, tokens[account])

            # Get sticker data
            sticker_data = await wb_orders.get_sticker_by_supply_ids(supply_id)

            # Validate response
            if not sticker_data:
                raise ValueError(f"No sticker data for order {supply_id}")

            # Get base64 data
            sticker_base64 = sticker_data.get("file")
            if not sticker_base64:
                raise ValueError(f"Sticker data corrupted for order {sticker_data}")

            # Decode base64 to PNG
            png_data = base64.b64decode(sticker_base64)
            png_buffer = BytesIO(png_data)
            png_buffer.seek(0)

            return png_buffer

        except ValueError:
            raise
        except Exception as e:
            raise Exception(f"Sticker error: {str(e)}")

    async def get_multiple_supply_stickers(self, supplies_map: Dict[str, str]) -> BytesIO:
        """
        Get PNG stickers for multiple supplies from different accounts and combine them into a single PNG file.

        Args:
            supplies_map: Dictionary mapping supply_id to account_name

        Returns:
            BytesIO: Combined PNG file with all stickers arranged vertically
        """
        try:
            # Get tokens
            tokens = get_wb_tokens()

            if missing_accounts := [
                account
                for account in supplies_map.values()
                if account not in tokens
            ]:
                raise ValueError(f"Accounts not found: {missing_accounts}")

            # Group supplies by account for optimization
            account_supplies = {}
            for supply_id, account in supplies_map.items():
                if account not in account_supplies:
                    account_supplies[account] = []
                account_supplies[account].append(supply_id)

            # Create tasks for each supply with its specific account
            sticker_tasks = []
            supply_account_pairs = []

            logger.info(
                f"Fetching stickers for {len(supplies_map)} supplies from {len(account_supplies)} accounts in parallel")

            for supply_id, account in supplies_map.items():
                wb_supplies = Supplies(account, tokens[account])
                task = wb_supplies.get_sticker_by_supply_ids(supply_id)
                sticker_tasks.append(task)
                supply_account_pairs.append((supply_id, account))

            # Fetch all stickers in parallel using asyncio.gather
            sticker_responses = await asyncio.gather(*sticker_tasks, return_exceptions=True)

            # Process responses and collect valid PNG data
            png_images = []
            successful_supplies = []

            for (supply_id, account), response in zip(supply_account_pairs, sticker_responses):
                if isinstance(response, Exception):
                    logger.error(f"Error fetching sticker for supply {supply_id} (account: {account}): {response}")
                    continue

                if not response or not response.get("file"):
                    logger.warning(f"No sticker data for supply {supply_id} (account: {account})")
                    continue

                try:
                    # Decode base64 to PNG data
                    png_data = base64.b64decode(response["file"])
                    png_images.append(png_data)
                    successful_supplies.append(f"{supply_id} ({account})")
                    logger.debug(f"Successfully processed sticker for supply {supply_id} (account: {account})")
                except Exception as e:
                    logger.error(f"Error decoding sticker for supply {supply_id} (account: {account}): {e}")
                    continue

            if not png_images:
                raise ValueError("No valid stickers found for any of the provided supplies")

            # Combine PNG images vertically
            combined_image = self._combine_png_images_vertically(png_images)

            # Convert combined image back to BytesIO
            output_buffer = BytesIO()
            combined_image.save(output_buffer, format='PNG')
            output_buffer.seek(0)

            logger.info(f"Successfully combined {len(png_images)} stickers for supplies: {successful_supplies}")
            return output_buffer

        except ValueError:
            raise
        except Exception as e:
            raise Exception(f"Multiple stickers error: {str(e)}")

    def _combine_png_images_vertically(self, png_data_list: List[bytes]) -> Image.Image:
        """
        Combine multiple PNG images vertically into a single image.

        Args:
            png_data_list: List of PNG image data as bytes

        Returns:
            PIL.Image: Combined image
        """
        try:
            # Open all images
            images = [Image.open(BytesIO(png_data)) for png_data in png_data_list]

            # Calculate total height and max width
            total_height = sum(img.height for img in images)
            max_width = max(img.width for img in images)

            # Create new image with combined dimensions
            combined_image = Image.new('RGB', (max_width, total_height), 'white')

            # Paste images one by one
            y_offset = 0
            for img in images:
                # Center the image horizontally if it's narrower than max_width
                x_offset = (max_width - img.width) // 2
                combined_image.paste(img, (x_offset, y_offset))
                y_offset += img.height

            return combined_image

        except Exception as e:
            logger.error(f"Error combining PNG images: {e}")
            raise Exception(f"Image combination error: {str(e)}")

    async def shipment_fictitious_supplies_with_quantity(self, supplies: Dict[str, str],
                                                         shipped_quantity: int, operator: str) -> Dict[str, Any]:
        """
        Фиктивная отгрузка поставок с указанным количеством.
        
        Args:
            supplies: Объект поставок {supply_id: account}
            shipped_quantity: Количество для отгрузки
            operator: Оператор
            
        Returns:
            Dict[str, Any]: Результат операции
        """
        start_time = time.time()
        logger.info(f"Начало фиктивной отгрузки {len(supplies)} поставок с количеством {shipped_quantity}")

        # 1. Получаем заказы напрямую из WB API (без метаданных поставок)
        all_orders = await self._get_all_orders_from_supplies(supplies)

        # 2. Получаем уже фиктивно отгруженные order_id из БД
        hanging_supplies = HangingSupplies(self.db)
        fictitious_shipped_ids = await hanging_supplies.get_fictitious_shipped_order_ids_batch(supplies)

        # 3. Фильтруем доступные заказы (упрощенная логика)
        available_orders = await self._filter_and_sort_orders(all_orders, fictitious_shipped_ids)

        # 4. Валидация и автокоррекция количества
        if len(available_orders) == 0:
            # Критическая ошибка - нет доступных заказов для отгрузки
            canceled_count = len(all_orders) - len(available_orders)
            raise HTTPException(
                status_code=400,
                detail=f"Нет доступных заказов для отгрузки. "
                       f"Всего заказов: {len(all_orders)}, "
                       f"отменено/уже отгружено: {canceled_count}"
            )

        if len(available_orders) < shipped_quantity:
            # Автокоррекция: отгружаем максимум доступных заказов
            shipped_quantity = len(available_orders)
            canceled_count = len(all_orders) - len(available_orders)

            original_quantity = shipped_quantity
            logger.warning(
                f"⚠️ АВТОКОРРЕКЦИЯ КОЛИЧЕСТВА: запрошено отгрузить {original_quantity} заказов, "
                f"но доступно только {shipped_quantity}. "
                f"Причина: {canceled_count} заказов отменены или уже отгружены. "
                f"Будет отгружено {shipped_quantity} заказов."
            )

        # 5. Выбираем заказы по количеству (старые сначала)
        selected_orders = await self._select_orders_by_quantity(available_orders, shipped_quantity)

        # 5.5. Генерируем стикеры для выбранных заказов
        logger.info(f"Запрос стикеров для {len(selected_orders)} выбранных заказов")
        supply_ids_schema = self._convert_selected_orders_to_supply_schema(selected_orders, supplies)
        stickers_raw = await self.get_stickers(supply_ids_schema)
        stickers_grouped = self.group_result(stickers_raw)

        # 5.6. Извлекаем список order_ids которые РЕАЛЬНО получили стикеры от WB API
        received_order_ids = set()
        for account_data in stickers_grouped.values():
            for supply_data in account_data.values():
                received_ids = supply_data.get('_received_order_ids', [])
                received_order_ids.update(received_ids)

        logger.info(
            f"Стикеры получены: {len(received_order_ids)} из {len(selected_orders)} заказов"
        )

        # 5.7. Фильтруем заказы - отгружаем ТОЛЬКО те что получили стикеры
        orders_with_stickers = [
            order for order in selected_orders
            if order['id'] in received_order_ids
        ]

        # Вычисляем заказы БЕЗ стикеров
        orders_without_stickers = [
            order for order in selected_orders
            if order['id'] not in received_order_ids
        ]

        # 5.8. Обработка случаев когда стикеры не получены
        if not orders_with_stickers:
            # КРИТИЧЕСКАЯ ОШИБКА: Ни один заказ не получил стикеры
            logger.error(
                f"❌ КРИТИЧЕСКАЯ ОШИБКА: Ни один из {len(selected_orders)} выбранных заказов "
                f"не получил стикеры от WB API. Отгрузка невозможна."
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Невозможно выполнить фиктивную отгрузку: "
                    f"ни один из {len(selected_orders)} выбранных заказов не получил стикеры от WB API. "
                    f"Возможные причины: заказы больше не в доступном статусе , проблемы с WB API."
                )
            )

        if orders_without_stickers:
            # ЧАСТИЧНАЯ ОТГРУЗКА: Часть заказов не получила стикеры
            logger.warning(
                f"⚠️ ЧАСТИЧНАЯ ОТГРУЗКА: {len(orders_without_stickers)} заказов не получили стикеры. "
                f"Будет отгружено только {len(orders_with_stickers)} заказов. "
                f"Заказы без стикеров: {[o['id'] for o in orders_without_stickers]}"
            )

        # 5.9. Генерируем PDF только для заказов со стикерами
        try:
            self.union_results_stickers(supply_ids_schema, stickers_grouped)
            grouped_stickers = await self.group_orders_to_wild(supply_ids_schema)
            stickers_pdf = await collect_images_sticker_to_pdf(grouped_stickers)
            logger.info(f"PDF стикеры сгенерированы для {len(grouped_stickers)} wild-кодов")
        except Exception as e:
            logger.error(f"Ошибка генерации PDF стикеров: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Невозможно выполнить фиктивную отгрузку: не сгенерирован pdf файл"
                )
            )

        # 6. Отправка данных в shipment_of_goods и 1C - ТОЛЬКО заказы со стикерами
        await self._send_shipment_data_to_external_systems(orders_with_stickers, supplies, operator)

        # 7. Сохраняем фиктивно отгруженные order_id в БД - ТОЛЬКО заказы со стикерами
        await self._save_fictitious_shipped_orders_batch(orders_with_stickers, supplies, operator)

        # Возвращаем только PDF стикеры
        return {"stickers_pdf": stickers_pdf}

    async def _get_all_orders_from_supplies(self, supplies: Dict[str, str]) -> List[Dict]:
        """
        Получает все заказы из поставок из WB API.

        ВАЖНО: Проверяет статус фиктивной доставки и блокирует операцию,
        если WB API не вернул заказы для фиктивно доставленной поставки.

        Args:
            supplies: Словарь {supply_id: account}

        Returns:
            List[Dict]: Список всех заказов с добавленными supply_id и account

        Raises:
            HTTPException: Если фиктивно доставленная поставка не вернула заказы из WB API
        """
        all_orders = []
        hanging_supplies_model = HangingSupplies(self.db)

        for supply_id, account in supplies.items():
            # 1. Проверяем статус фиктивной доставки
            hanging_supply = await hanging_supplies_model.get_hanging_supply_by_id(supply_id, account)
            is_fictitious_delivered = hanging_supply.get('is_fictitious_delivered', False) if hanging_supply else False

            # 2. Получаем заказы из WB API
            orders_data = await Supplies(account, get_wb_tokens()[account]).get_supply_orders(supply_id)
            orders = orders_data.get(account, {supply_id: {'orders': []}}).get(supply_id).get('orders', [])

            # 3. ВАЛИДАЦИЯ: Блокируем операцию если поставка фиктивно доставлена, но WB API не вернул заказы
            if is_fictitious_delivered and not orders:
                logger.error(
                    f"БЛОКИРОВКА ОПЕРАЦИИ: Поставка {supply_id} ({account}) в статусе фиктивной доставки, "
                    f"но WB API не вернул заказы. Возможно поставка удалена из WB."
                )
                raise HTTPException(
                    status_code=400,
                    detail=f"Поставка {supply_id} ({account}) в статусе фиктивной доставки, "
                           f"но WB API не вернул заказы. Операция заблокирована для безопасности."
                )

            # 4. Добавляем заказы в общий список
            for order in orders:
                order['supply_id'] = supply_id
                order['account'] = account
                all_orders.append(order)

        return all_orders

    async def _filter_and_sort_orders(self, all_orders: List[Dict],
                                      fictitious_shipped_ids: Dict[Tuple[str, str], List[int]]) -> List[Dict]:
        """
        Фильтрует и сортирует заказы.

        НОВОЕ: Добавлена проверка статусов из assembly_task_status:
        - Исключает заказы с wb_status = 'canceled' или 'canceled_by_client'
        - Разрешает заказы без записи в assembly_task_status
        - Логирует отмененные заказы

        Args:
            all_orders: Все заказы из поставок
            fictitious_shipped_ids: Словарь уже отгруженных order_id по (supply_id, account)

        Returns:
            List[Dict]: Отсортированный список доступных заказов
        """
        # ========================================
        # НОВОЕ: Получаем статусы из assembly_task_status
        # ========================================
        # Собираем order_id по аккаунтам для батч-запроса
        orders_by_account = defaultdict(set)  # {account: {order_id1, order_id2, ...}}
        for order in all_orders:
            orders_by_account[order['account']].add(order['id'])

        # Получаем статусы батчем для каждого аккаунта
        statuses_cache = {}  # {account: {order_id: {'wb_status': '...', 'supplier_status': '...'}}}
        assembly_task_status_service = AssemblyTaskStatus(self.db)

        for account, order_ids_set in orders_by_account.items():
            if order_ids_set:
                order_ids_list = list(order_ids_set)
                statuses = await assembly_task_status_service.get_order_statuses_batch(account, order_ids_list)
                statuses_cache[account] = statuses

        # ========================================
        # Фильтруем заказы
        # ========================================
        available_orders = []
        blocked_orders = []  # Для логирования заблокированных заказов

        for order in all_orders:
            supply_id = order['supply_id']
            account = order['account']
            order_id = order['id']
            shipped_key = (supply_id, account)
            shipped_ids = set(fictitious_shipped_ids.get(shipped_key, []))

            # Проверка 1: Заказ уже был фиктивно отгружен?
            if order_id in shipped_ids:
                continue  # Пропускаем

            # Проверка 2: СТРОГАЯ ВАЛИДАЦИЯ - разрешаем только supplier_status='complete' AND wb_status='waiting'
            account_statuses = statuses_cache.get(account, {})
            status_data = account_statuses.get(order_id, {})
            supplier_status = status_data.get('supplier_status')
            wb_status = status_data.get('wb_status')

            # Разрешаем ТОЛЬКО конкретную комбинацию статусов
            is_valid_for_fictitious_shipment = (
                supplier_status == 'complete' and wb_status == 'waiting'
            )

            if not is_valid_for_fictitious_shipment:
                blocked_orders.append({
                    'order_id': order_id,
                    'supply_id': supply_id,
                    'account': account,
                    'wb_status': wb_status,
                    'supplier_status': supplier_status,
                    'block_reason': f"Required: supplier_status='complete' AND wb_status='waiting', Got: supplier_status='{supplier_status}', wb_status='{wb_status}'"
                })
                continue  # Блокируем все кроме разрешенной комбинации

            # Заказ валидный - добавляем в доступные
            available_orders.append(order)

        # ========================================
        # Логирование результатов фильтрации
        # ========================================
        total_orders = len(all_orders)
        already_shipped = total_orders - len(available_orders) - len(blocked_orders)

        logger.info(
            f"Фильтрация заказов для фиктивной отгрузки (СТРОГАЯ ВАЛИДАЦИЯ): "
            f"всего={total_orders}, "
            f"доступно={len(available_orders)}, "
            f"уже отгружено={already_shipped}, "
            f"заблокировано (невалидный статус)={len(blocked_orders)}"
        )

        if blocked_orders:
            blocked_ids = [o['order_id'] for o in blocked_orders]
            logger.warning(
                f"Исключено {len(blocked_orders)} заказов с невалидными статусами: {blocked_ids[:10]}"
                f"{'...' if len(blocked_ids) > 10 else ''}"
            )
            logger.info(
                f"Разрешены только заказы с supplier_status='complete' AND wb_status='waiting'"
            )

            # Детальное логирование первых 5 заблокированных заказов
            for blocked in blocked_orders[:5]:
                logger.debug(
                    f"Заблокированный заказ {blocked['order_id']}: "
                    f"supply_id={blocked['supply_id']}, "
                    f"wb_status={blocked['wb_status']}, "
                    f"supplier_status={blocked['supplier_status']}, "
                    f"reason: {blocked['block_reason']}"
                )

        # Сортируем по времени создания (старые сначала - FIFO)
        available_orders.sort(key=lambda x: x.get('createdAt', ''))
        return available_orders

    async def _select_orders_by_quantity(self, available_orders: List[Dict], shipped_quantity: int) -> List[Dict]:
        """Выбирает заказы по количеству (старые сначала)."""
        selected_orders = available_orders[:shipped_quantity]
        logger.info(f"Выбрано {len(selected_orders)} заказов для фиктивной отгрузки")
        return selected_orders

    async def _send_shipment_data_to_external_systems(self, selected_orders: List[Dict],
                                           supplies: Dict[str, str],
                                           operator: str) -> bool:
        """
        Отправляет данные об отгрузке в shipment_of_goods и 1C.
        НОВОЕ: Также снимает резерв через add_shipped_goods API.

        Args:
            selected_orders: Выбранные заказы для отгрузки
            supplies: Словарь {supply_id: account}
            operator: Оператор, выполняющий операцию

        Returns:
            bool: True если отправка успешна
        """
        try:
            logger.info(f"Отправка данных фиктивной отгрузки {len(selected_orders)} заказов")

            # 1. НОВОЕ: Снимаем резерв через add_shipped_goods API
            grouped_orders = self.group_selected_orders_by_supply(selected_orders)
            shipped_goods_data = self._prepare_shipped_goods_data(grouped_orders)

            if shipped_goods_data:
                shipped_goods_response = await self._send_shipped_goods_to_api(shipped_goods_data)
                logger.info(f"Снято резервов для фиктивной отгрузки: {len(shipped_goods_response)}")
            else:
                logger.warning("Нет данных для снятия резерва при фиктивной отгрузке")

            # 2. Преобразуем selected_orders в формат DeliverySupplyInfo
            delivery_supplies = self._convert_to_delivery_supplies(selected_orders, supplies)

            # 3. Создаем order_wild_map используя process_local_vendor_code
            order_wild_map = self._extract_order_wild_map(selected_orders)

            # 4. Отправляем в shipment_of_goods API
            shipment_success = await self.save_shipments(
                supply_ids=delivery_supplies,
                order_wild_map=order_wild_map,
                author=operator
            )

            # 5. Отправляем в 1C
            integration = OneCIntegration(self.db)
            integration_result = await integration.format_delivery_data(delivery_supplies, order_wild_map)
            integration_success = isinstance(integration_result, dict) and integration_result.get("code") == 200

            logger.info(f"Фиктивная отгрузка: shipment_api={shipment_success}, 1c_integration={integration_success}")
            return shipment_success and integration_success

        except Exception as e:
            logger.error(f"Ошибка отправки данных фиктивной отгрузки: {str(e)}")
            return False

    def _convert_to_delivery_supplies(self, selected_orders: List[Dict], 
                                    supplies: Dict[str, str]) -> List[DeliverySupplyInfo]:
        """
        Преобразует selected_orders в формат DeliverySupplyInfo.
        """
        # Группируем заказы по supply_id
        supply_orders = {}
        for order in selected_orders:
            supply_id = order['supply_id']
            if supply_id not in supply_orders:
                supply_orders[supply_id] = []
            supply_orders[supply_id].append(order['id'])  # order_id
        
        # Создаем DeliverySupplyInfo объекты
        delivery_supplies = []
        for supply_id, order_ids in supply_orders.items():
            account = supplies.get(supply_id, '')
            delivery_supplies.append(DeliverySupplyInfo(
                supply_id=supply_id,
                account=account,
                order_ids=order_ids
            ))
        
        return delivery_supplies

    def _extract_order_wild_map(self, selected_orders: List[Dict]) -> Dict[str, str]:
        """
        Извлекает маппинг order_id -> wild_code из selected_orders.
        Использует process_local_vendor_code для преобразования article.
        """
        order_wild_map = {}
        for order in selected_orders:
            order_id = str(order['id'])
            article = order.get('article', '')
            wild_code = process_local_vendor_code(article)
            order_wild_map[order_id] = wild_code
        
        return order_wild_map

    async def _save_fictitious_shipped_orders_and_build_results(self, selected_orders: List[Dict],
                                                                supplies: Dict[str, str],
                                                                operator: str) -> List[Dict[str, Any]]:
        """
        Сохраняет фиктивно отгруженные order_id в БД и формирует результаты.
        
        Args:
            selected_orders: Выбранные для отгрузки заказы
            supplies: Объект поставок {supply_id: account}
            operator: Оператор
            
        Returns:
            List[Dict[str, Any]]: Список результатов по каждой поставке
        """
        results = []
        hanging_supplies = HangingSupplies(self.db)

        for supply_id, account in supplies.items():
            supply_orders = [order for order in selected_orders if order['supply_id'] == supply_id]

            if supply_orders:
                order_ids = [order['id'] for order in supply_orders]
                success = await hanging_supplies.add_fictitious_shipped_order_ids(
                    supply_id, account, order_ids, operator
                )
                results.append({
                    "supply_id": supply_id,
                    "account": account,
                    "shipped_count": len(order_ids),
                    "success": success,
                    "order_ids": order_ids
                })
            else:
                # Поставка без заказов для отгрузки
                results.append({
                    "supply_id": supply_id,
                    "account": account,
                    "shipped_count": 0,
                    "success": True,
                    "order_ids": []
                })

        return results

    async def _save_fictitious_shipped_orders_batch(self, selected_orders: List[Dict],
                                                   supplies: Dict[str, str],
                                                   operator: str) -> None:
        """
        Сохраняет фиктивно отгруженные order_id в БД (упрощенная версия).
        
        Args:
            selected_orders: Выбранные для отгрузки заказы
            supplies: Объект поставок {supply_id: account}
            operator: Оператор
        """
        hanging_supplies = HangingSupplies(self.db)

        for supply_id, account in supplies.items():
            supply_orders = [order for order in selected_orders if order['supply_id'] == supply_id]
            
            if supply_orders:
                order_ids = [order['id'] for order in supply_orders]
                await hanging_supplies.add_fictitious_shipped_order_ids(
                    supply_id, account, order_ids, operator
                )
                logger.info(f"Сохранено {len(order_ids)} фиктивно отгруженных заказов для поставки {supply_id} ({account})")

    async def generate_stickers_for_selected_orders(self, selected_orders: List[Dict], 
                                                   supplies: Dict[str, str]) -> BytesIO:
        """
        Генерирует PDF стикеры для выбранных заказов фиктивной отгрузки.
        
        Args:
            selected_orders: Заказы из shipment_fictitious_supplies_with_quantity
            supplies: Словарь {supply_id: account}
            
        Returns:
            BytesIO: PDF файл со стикерами
        """
        logger.info(f"Генерация стикеров для {len(selected_orders)} фиктивно отгружаемых заказов")
        
        # 1. Преобразуем selected_orders в формат SupplyIdBodySchema
        supply_ids = self._convert_selected_orders_to_supply_schema(selected_orders, supplies)
        
        # 2. Используем СУЩЕСТВУЮЩУЮ цепочку методов
        stickers: Dict[str, Dict] = self.group_result(await self.get_stickers(supply_ids))
        self.union_results_stickers(supply_ids, stickers) 
        grouped_stickers = await self.group_orders_to_wild(supply_ids)
        
        # 3. Генерируем PDF через существующий метод
        from src.service.service_pdf import collect_images_sticker_to_pdf
        pdf_buffer = await collect_images_sticker_to_pdf(grouped_stickers)
        
        logger.info(f"PDF стикеры сгенерированы для {len(grouped_stickers)} wild-кодов")
        return pdf_buffer

    def _convert_selected_orders_to_supply_schema(self, selected_orders: List[Dict], 
                                                 supplies: Dict[str, str]) -> SupplyIdBodySchema:
        """
        Преобразует selected_orders в SupplyIdBodySchema.
        """
        from collections import defaultdict
        from datetime import datetime
        from src.supplies.schema import SupplyIdBodySchema, SupplyId, OrderSchema
        
        # Группируем заказы по supply_id
        supply_orders_map = defaultdict(list)
        for order in selected_orders:
            supply_orders_map[order['supply_id']].append(order)
        
        supplies_list = []
        for supply_id, orders in supply_orders_map.items():
            account = supplies.get(supply_id, '')
            
            # Создаем OrderSchema объекты
            order_schemas = [
                OrderSchema(
                    order_id=order['id'],
                    nm_id=order['nmId'],
                    local_vendor_code=process_local_vendor_code(order.get('article', '')),
                    createdAt=order.get('createdAt', '')
                ) for order in orders
            ]
            
            supplies_list.append(SupplyId(
                name=f"Fictitious_{supply_id}",
                createdAt=datetime.utcnow().isoformat(),
                supply_id=supply_id,
                account=account,
                count=len(order_schemas),
                orders=order_schemas
            ))
        
        return SupplyIdBodySchema(supplies=supplies_list)
