import asyncio
import datetime
from typing import List, Dict, Any, Set
from src.logger import app_logger as logger
from src.utils import get_wb_tokens, process_local_vendor_code, get_information_to_data
from src.wildberries_api.orders import Orders
from src.models.article import ArticleDB
from datetime import timedelta
from collections import defaultdict
from src.orders.schema import GroupedOrderInfo


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
            Dict[str, List[Dict]]: Словарь, где ключ - wild, а значение - список заказов,
                                  отсортированный по наименованию предмета и артикулу
        """
        from collections import defaultdict
        
        logger.info("Группировка заказов по артикулу wild")
        
        wild_data = get_information_to_data()
        grouped_orders = defaultdict(list)

        for order in order_list:
            order_dict = order.model_dump()
            order_dict["wild_name"] = wild_data.get(order.article, "")
            grouped_orders[order.article].append(order_dict)

        return dict(sorted(
            grouped_orders.items(),
            key=lambda x: x[0]
        ))
