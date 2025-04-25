import asyncio
import datetime
from typing import List, Dict, Any, Set
from src.logger import app_logger as logger
from src.utils import get_wb_tokens, process_local_vendor_code, get_information_to_data
from src.wildberries_api.orders import Orders
from src.models.article import ArticleDB
from src.models.stock import StockDB
from datetime import timedelta
from src.orders.schema import GroupedOrderInfo, OrdersWithSupplyNameIn, SupplyAccountWildOut, GroupedOrderInfoWithFact, \
    OrderDetail
from src.wildberries_api.supplies import Supplies
from collections import defaultdict


class OrdersService:
    """Сервис для работы с заказами Wildberries."""

    def __init__(self, db=None):
        """
        Инициализирует сервис для работы с заказами.
        Args:
            db: Соединение с базой данных (опционально)
        """
        self.db = db
        self.article_db = ArticleDB(db) if db else None

    async def get_all_new_orders(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Получает все заказы из всех кабинетов с информацией о товарах.
        Делает один запрос в базу данных для всех nm_id.
        Returns:
            Dict[str, List[Dict[str, Any]]]: Словарь с заказами, сгруппированными по кабинетам
        """
        logger.info("Получение заказов по всем кабинетам")
        raw_orders_by_account = await self._get_raw_orders_from_all_accounts()
        all_nm_ids = self._collect_nm_ids(raw_orders_by_account)
        photos_info = await self._get_photos_info(list(all_nm_ids))
        wild_data = get_information_to_data()
        return self._format_all_orders(raw_orders_by_account, photos_info, wild_data)

    @staticmethod
    def _collect_nm_ids(orders_by_account: Dict[str, List[Dict[str, Any]]]) -> Set[int]:
        """
        Собирает все уникальные nm_id из заказов.
        Args:
            orders_by_account: Словарь с заказами по кабинетам
        Returns:
            Set[int]: Множество уникальных nm_id
        """
        all_nm_ids = set()
        for orders in orders_by_account.values():
            for order in orders:
                if nm_id := order.get("nmId"):
                    all_nm_ids.add(nm_id)

        return all_nm_ids

    def _format_all_orders(
            self,
            raw_orders_by_account: Dict[str, List[Dict[str, Any]]],
            photos_info: Dict[int, str],
            wild_data: Dict[str, str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Форматирует все заказы для всех кабинетов.
        Args:
            raw_orders_by_account: Словарь с сырыми данными о заказах
            photos_info: Информация о фото товаров
            wild_data: Данные о соответствии wild-наименование
        Returns:
            Dict[str, List[Dict[str, Any]]]: Отформатированные заказы
        """
        result = {}
        total_orders = 0

        for account, orders_data in raw_orders_by_account.items():
            formatted_orders = self._format_orders(orders_data, photos_info, wild_data, account)
            formatted_orders.sort(key=lambda x: x["created_at"], reverse=True)

            result[account] = formatted_orders
            total_orders += len(formatted_orders)
            logger.info(f"Кабинет {account}: {len(formatted_orders)} заказов")

        logger.info(f"Всего получено заказов: {total_orders}")
        return result

    async def _get_raw_orders_from_all_accounts(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Получает сырые данные о заказах со всех кабинетов.
        Returns:
            Dict[str, List[Dict[str, Any]]]: Словарь с сырыми данными о заказах
        """
        tokens = get_wb_tokens()
        logger.info(f"Найдено {len(tokens)} кабинетов WB")

        tasks = [
            self._get_raw_orders_for_account(Orders(account, token), account)
            for account, token in tokens.items()
        ]

        raw_orders_results = await asyncio.gather(*tasks)

        # Объединяем результаты в один словарь
        raw_orders_by_account = {}
        for result in raw_orders_results:
            raw_orders_by_account.update(result)

        return raw_orders_by_account

    async def _get_raw_orders_for_account(self, orders: Orders, account: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Получает сырые данные о заказах для конкретного кабинета.
        Args:
            orders: Экземпляр класса Orders
            account: Название кабинета
        Returns:
            Dict[str, List[Dict[str, Any]]]: Словарь с сырыми данными о заказах
        """
        try:
            supplies_data = await orders.get_orders()
            logger.info(f"Кабинет {account}: получено {len(supplies_data)} заказов")
            return {account: supplies_data or []}
        except Exception as e:
            logger.error(f"Ошибка при получении заказов для кабинета {account}: {str(e)}")
            return {account: []}

    async def _get_photos_info(self, nm_ids: List[int]) -> Dict[int, str]:
        """
        Получает информацию о фото товаров из базы данных.
        Args:
            nm_ids: Список nm_id
        Returns:
            Dict[int, str]: Словарь с ссылками на фото по nm_id
        """
        if not nm_ids or not self.db:
            return {}

        try:
            logger.info(f"Получение информации о {len(nm_ids)} товарах из БД")
            db_articles = await self.article_db.get_articles_info_by_nm_ids(nm_ids)

            result = {
                article["nm_id"]: article["photo_link"]
                for article in db_articles
                if article and "nm_id" in article and "photo_link" in article
            }

            logger.info(f"Получено {len(result)} фото из БД")
            return result
        except Exception as e:
            logger.error(f"Ошибка при получении фото из БД: {str(e)}")
            return {}

    @staticmethod
    def _calculate_elapsed_time(created_at: str) -> str:
        """
        Рассчитывает, сколько времени прошло с момента создания заказа.
        Args:
            created_at: Дата создания заказа в формате строки
        Returns:
            str: Строка с информацией о прошедшем времени в формате "X ч Y мин"
        """
        try:
            created_datetime = datetime.datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            now = datetime.datetime.now(datetime.timezone.utc)
            time_diff = now - created_datetime
            total_seconds = time_diff.total_seconds()
            hours = int(total_seconds // 3600)
            minutes = int((total_seconds % 3600) // 60)
            return f"{hours}ч {minutes}мин"
        except (ValueError, TypeError):
            return "Н/Д"

    def format_order(self, supply: dict, photos_info: dict, wild_data: dict, account: str) -> dict:
        """
        Форматирует один заказ, добавляя информацию о фото, наименовании и прошедшем времени.
        Args:
            supply: Данные о заказе из WB API
            photos_info: Словарь с информацией о фото товаров {nm_id: photo_link}
            wild_data: Словарь с соответствием артикулов и наименований {артикул: наименование}
            account: Название кабинета WB
        Returns:
            dict: Отформатированный заказ со всеми необходимыми полями
        """
        nm_id = supply.get("nmId")
        raw_article = supply.get("article", "")
        created_at = supply.get("createdAt", "")
        local_vendor_code = process_local_vendor_code(raw_article)
        photo = photos_info.get(nm_id, "Нет фото")
        subject_name = wild_data.get(local_vendor_code, "Нет наименования")

        elapsed_time = self._calculate_elapsed_time(created_at)
        return {
            "id": supply.get("id"),
            "article": local_vendor_code,
            "photo": photo,
            "subject_name": subject_name,
            "price": int(supply.get("price", 0) / 100),
            "account": account,
            "created_at": created_at,
            "elapsed_time": elapsed_time
        }

    def _format_orders(
            self,
            supplies_data: List[Dict[str, Any]],
            photos_info: Dict[int, str],
            wild_data: Dict[str, str],
            account: str
    ) -> List[Dict[str, Any]]:
        """
        Форматирует заказы, добавляя информацию о фото и наименовании.
        Args:
            supplies_data: Данные о заказах
            photos_info: Информация о фото товаров
            wild_data: Данные о соответствии wild-наименование
            account: Название кабинета
        Returns:
            List[Dict[str, Any]]: Список отформатированных заказов
        """
        formatted_orders = []
        for supply in supplies_data:
            try:
                formatted_orders.append(self.format_order(supply, photos_info, wild_data, account))
            except Exception as e:
                logger.error(f"Ошибка при форматировании заказа {supply.get('id', 'Нет ID')}: {str(e)}")
        return formatted_orders

    def filter_orders_by_time(self, orders: list, time_delta: float) -> list:
        """
        Фильтрует заказы по времени создания (оставляет только те, что созданы позже now - time_delta часов)
        """
        if time_delta is None:
            return orders
        now = datetime.datetime.now(datetime.timezone.utc)
        min_created_at = now - timedelta(hours=time_delta)

        def parse_created_at(order):
            try:
                dt = datetime.datetime.fromisoformat(order["created_at"].replace('Z', '+00:00'))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                return dt
            except Exception:
                return None

        return [order for order in orders if (created := parse_created_at(order)) and created < min_created_at]

    def filter_orders_by_article(self, orders: list, article: str) -> list:
        """
        Фильтрует заказы по артикулу (через process_local_vendor_code)
        """
        if not article:
            return orders
        article_processed = process_local_vendor_code(article)
        return [order for order in orders if process_local_vendor_code(order["article"]) == article_processed]

    def sort_orders(self, orders: list) -> list:
        """
        Сортирует заказы по времени создания (по убыванию)
        """
        return sorted(orders, key=lambda x: x["created_at"], reverse=True)

    async def get_filtered_orders(self, time_delta: float = None, article: str = None) -> list:
        """
        Получает, фильтрует и сортирует заказы по заданным параметрам
        """
        all_orders = await self.get_all_new_orders()
        formatted_orders = []
        for orders_list in all_orders.values():
            formatted_orders.extend(orders_list)
        filtered = self.filter_orders_by_time(formatted_orders, time_delta)
        filtered = self.filter_orders_by_article(filtered, article)
        return self.sort_orders(filtered)

    async def group_orders_by_wild(self, order_list):
        """
        Группирует заказы по артикулу wild с добавлением информации из get_information_to_data
        
        Args:
            order_list: Список заказов
            
        Returns:
            Dict[str, GroupedOrderInfo]: Словарь с данными о заказах по артикулам
        """
        logger.info("Группировка заказов по артикулу wild")

        wild_data = get_information_to_data()
        stock_db = StockDB(self.db)
        result = {}

        temp_grouped_orders = defaultdict(list)
        for order in order_list:
            order_dict = order.model_dump()
            order_dict["wild_name"] = wild_data.get(order.article, "")
            temp_grouped_orders[order.article].append(order_dict)

        for wild, orders in temp_grouped_orders.items():
            stock_quantity = await stock_db.get_stock_by_wild(wild)
            api_name = next((item.get('subject_name', 'Нет наименования из API')
                             for item in orders if item.get('subject_name')),
                            'Нет наименования из API')
            doc_name = wild_data.get(wild, "Нет наименования в документе")

            result[wild] = GroupedOrderInfo(
                wild=wild,
                stock_quantity=stock_quantity,
                doc_name=doc_name,
                api_name=api_name,
                orders=orders,
                order_count=len(orders)
            )

        return dict(sorted(
            result.items(),
            key=lambda x: x[0]
        ))

    def _filter_orders_by_fact_count(self, orders_data: Dict[str, GroupedOrderInfoWithFact]) -> Dict[
        str, List[OrderDetail]]:
        """
        Фильтрует заказы по каждому SKU согласно установленному количеству fact_orders.
        Args:
            orders_data: Словарь с данными о заказах по SKU
        Returns:
            Dict[str, List[OrderDetail]]: Отфильтрованные заказы по SKU
        """
        filtered_orders_by_sku = {}
        for wild_key, info in orders_data.items():
            if info.fact_orders == 0:
                continue

            sorted_orders = sorted(info.orders, key=lambda x: x.created_at)
            if selected_orders := sorted_orders[:info.fact_orders]:
                filtered_orders_by_sku[wild_key] = selected_orders

        return filtered_orders_by_sku

    @staticmethod
    def _collect_unique_accounts(filtered_orders: Dict[str, List[OrderDetail]]) -> Set[str]:
        """
        Собирает все уникальные аккаунты из отфильтрованных заказов.
        Args:
            filtered_orders: Отфильтрованные заказы по SKU
        Returns:
            Set[str]: Множество уникальных аккаунтов
        """
        unique_accounts = set()
        for orders in filtered_orders.values():
            for order in orders:
                unique_accounts.add(order.account)
        return unique_accounts

    @staticmethod
    def _process_supply_creation_results(accounts: List[str], results: List[Any]) -> Dict[str, str]:
        """
        Обрабатывает результаты создания поставок.
        Args:
            accounts: Список аккаунтов, для которых создавались поставки
            results: Список результатов выполнения задач
        Returns:
            Dict[str, str]: Словарь с маппингом аккаунта на ID поставки
        """
        supply_by_account = {}

        for account, result in zip(accounts, results):
            if isinstance(result, Exception):
                logger.error(f"Исключение при создании поставки для аккаунта {account}: {str(result)}")
            elif 'id' in result:
                supply_by_account[account] = result['id']
                logger.info(f"Создана поставка {result['id']} для аккаунта {account}")
            else:
                logger.error(f"Ошибка создания поставки для аккаунта {account}: {result}")

        return supply_by_account

    async def _create_supplies_for_accounts(self, accounts: Set[str], supply_name: str) -> Dict[str, str]:
        """
        Создает поставки для каждого уникального аккаунта параллельно.
        Args:
            accounts: Множество уникальных аккаунтов
            supply_name: Название поставки
        Returns:
            Dict[str, str]: Словарь с маппингом аккаунта на ID поставки
        """
        tokens = get_wb_tokens()
        account_tasks = []
        valid_accounts = []

        for account in accounts:
            if token := tokens.get(account):
                valid_accounts.append(account)
                account_tasks.append(Supplies(account, token).create_supply(supply_name))
            else:
                logger.error(f"Не найден токен для аккаунта {account}")

        if not account_tasks:
            return {}

        results = await asyncio.gather(*account_tasks, return_exceptions=True)
        return self._process_supply_creation_results(valid_accounts, results)

    @staticmethod
    async def _add_orders_to_supplies(filtered_orders: Dict[str, List[OrderDetail]],
                                      supply_by_account: Dict[str, str]) -> None:
        """
        Добавляет отфильтрованные заказы в созданные поставки.
        Args:
            filtered_orders: Отфильтрованные заказы по SKU
            supply_by_account: Словарь с маппингом аккаунта на ID поставки
        """
        # Группируем заказы по аккаунту и поставке
        orders_by_supply = defaultdict(list)

        for orders in filtered_orders.values():
            for order in orders:
                account = order.account
                if account not in supply_by_account:
                    logger.warning(f"Пропуск заказа {order.id}: не создана поставка для аккаунта {account}")
                    continue

                supply_id = supply_by_account[account]
                orders_by_supply[(account, supply_id)].append(order.id)

        # Создаем список задач для параллельного выполнения
        add_order_tasks = []
        task_info = []  # Информация о задаче (account, supply_id, order_id)
        tokens = get_wb_tokens()

        for (account, supply_id), order_ids in orders_by_supply.items():
            token = tokens.get(account)
            if not token:
                logger.error(f"Не найден токен для аккаунта {account}")
                continue

            supplies_service = Supplies(account, token)

            for order_id in order_ids:
                task = supplies_service.add_order_to_supply(supply_id, order_id)
                add_order_tasks.append(task)
                task_info.append((account, supply_id, order_id))

        # Если нет задач, завершаем работу
        if not add_order_tasks:
            return

        # Параллельно выполняем все задачи
        results = await asyncio.gather(*add_order_tasks, return_exceptions=True)

        # Обрабатываем результаты
        for (account, supply_id, order_id), result in zip(task_info, results):
            if isinstance(result, Exception):
                logger.error(f"Ошибка при добавлении заказа {order_id} в поставку {supply_id} "
                             f"для аккаунта {account}: {result}")
            elif result and 'error' in result:
                logger.error(f"Ошибка при добавлении заказа {order_id} в поставку {supply_id} "
                             f"для аккаунта {account}: {result['error']}")

    @staticmethod
    def _prepare_result(input_data: OrdersWithSupplyNameIn,
                        supply_by_account: Dict[str, str]) -> SupplyAccountWildOut:
        """
        Формирует результат обработки заказов.
        Args:
            input_data: Исходные данные для обработки
            supply_by_account: Словарь с маппингом аккаунта на ID поставки
        Returns:
            SupplyAccountWildOut: Результат обработки заказов
        """
        return SupplyAccountWildOut(
            wild=[wild_key for wild_key, info in input_data.orders.items() if info.fact_orders > 0],
            supply_account=supply_by_account
        )

    async def process_orders_with_fact_count(self, input_data: OrdersWithSupplyNameIn) -> SupplyAccountWildOut:
        """
        Обрабатывает заказы с учетом установленного количества fact_orders.
        
        Args:
            input_data: Объект с данными о заказах и названием поставки
            
        Returns:
            SupplyAccountWildOut: Объект с результатами обработки
        """
        filtered_orders_by_sku = self._filter_orders_by_fact_count(input_data.orders)
        unique_accounts = self._collect_unique_accounts(filtered_orders_by_sku)
        supply_by_account = await self._create_supplies_for_accounts(unique_accounts, input_data.name_supply)
        await self._add_orders_to_supplies(filtered_orders_by_sku, supply_by_account)
        return self._prepare_result(input_data, supply_by_account)
