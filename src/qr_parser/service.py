from typing import List, Dict, Optional, Any
import asyncpg
from src.logger import app_logger as logger
from src.models.card_data import CardData
from src.qr_parser.schema import WildParserResponse, QRLookupResponse
from src.utils import get_information_to_data


class WildParserService:
    """Сервис для парсинга строк формата 'wild123/23'."""

    def __init__(self, db=None):
        """
        Инициализирует сервис для парсинга wild-строк.
        Args:
            db: Соединение с базой данных (опционально)
        """
        self.db = db
        self.card_data = CardData(db) if db else None

    async def get_all_article_data(self, wild_code: str) -> List[Dict[str, Any]]:
        """
        Получает все данные о товаре из базы данных.
        Args:
            wild_code: Уникальный номер wild
        Returns:
            List[Dict[str, Any]]: Все данные о товаре из базы данных
        """
        logger.info(f"Получение данных для wild-кода: {wild_code}")

        data = await self.card_data.get_information_to_local_vendor_code(wild_code)

        if not data:
            logger.warning(f"Не найдены данные для wild-кода {wild_code}")
            return []

        return [dict(d) for d in data]

    async def get_photo_data(self, wild_code: str) -> Dict[str, Any]:
        """
        Получает данные о фотографиях товара только из кабинета Тоноян.
        Args:
            wild_code: Уникальный номер wild
        Returns:
            Dict[str, Any]: Данные о фотографиях товара из кабинета Тоноян
        """
        all_data = await self.get_all_article_data(wild_code)
        filtered_data = [d for d in all_data if d.get('account').upper() == 'ТОНОЯН']
        if not filtered_data:
            logger.warning(f"Не найдены данные для wild-кода {wild_code} в кабинете ТОНОЯН")
            return {}

        return filtered_data[0]

    async def get_most_relevant_value(self, wild_code: str, field_name: str, default_value=None) -> Any:
        """
        Получает наиболее актуальное значение для указанного поля.
        Просматривает весь массив данных и выбирает первое ненулевое значение.
        Args:
            wild_code: Уникальный номер wild
            field_name: Имя поля, значение которого нужно получить
            default_value: Значение по умолчанию, если не найдено ни одного подходящего значения
        Returns:
            Any: Наиболее актуальное значение поля или default_value
        """
        all_data = await self.get_all_article_data(wild_code)

        for item in all_data:
            value = item.get(field_name)
            if value is not None and value != 0 and value != "" and value != []:
                return value

        logger.info(f"Не найдено актуальное значение для поля {field_name} у wild-кода {wild_code}")
        return default_value

    @staticmethod
    async def parse_wild_string_parts(wild_string: str) -> tuple[str, int]:
        """
        Разбирает строку формата 'wild123/23' на wild-код и количество.
        Args:
            wild_string: Строка в формате 'wild123/23'
        Returns:
            tuple[str, int]: Кортеж с wild-кодом и количеством
        """
        try:
            parts = wild_string.split('/')
            return parts[0], int(parts[1])
        except Exception as e:
            logger.error(f"Ошибка парсинга '{wild_string}': {e}")
            raise ValueError(f"Ошибка парсинга '{wild_string}': {e}") from e

    @staticmethod
    async def get_name(wild_code: str) -> str:
        """
        Получает наименование товара по wild-коду.
        Args:
            wild_code: Уникальный номер wild
        Returns:
            str: Наименование товара
        """
        names = get_information_to_data()
        return names.get(wild_code, "Нет Наименования")

    async def get_name_db(self, wild_code: str) -> Optional[int]:
        """
        Получает ширину товара в мм, выбирая наиболее актуальное значение.
        Args:
            wild_code: Уникальный номер wild
        Returns:
            Optional[int]: Ширина товара в мм
        """
        return await self.get_most_relevant_value(wild_code, 'subject_name', None)

    async def get_photos(self, wild_code: str) -> str:
        """
        Получает ссылки на фотографии товара только из кабинета Тоноян.
        Args:
            wild_code: Уникальный номер wild
        Returns:
            str: Ссылки на фотографии товара
        """
        photo_data = await self.get_photo_data(wild_code)
        return photo_data.get('photo_link', "")

    async def get_length(self, wild_code: str) -> Optional[int]:
        """
        Получает длину товара в мм, выбирая наиболее актуальное значение.
        Args:
            wild_code: Уникальный номер wild
        Returns:
            Optional[int]: Длина товара в мм
        """
        return await self.get_most_relevant_value(wild_code, 'length', None)

    async def get_width(self, wild_code: str) -> Optional[int]:
        """
        Получает ширину товара в мм, выбирая наиболее актуальное значение.
        Args:
            wild_code: Уникальный номер wild   
        Returns:
            Optional[int]: Ширина товара в мм
        """
        return await self.get_most_relevant_value(wild_code, 'width', None)

    async def get_height(self, wild_code: str) -> Optional[int]:
        """
        Получает высоту товара в мм, выбирая наиболее актуальное значение.
        Args:
            wild_code: Уникальный номер wild
        Returns:
            Optional[int]: Высота товара в мм
        """
        return await self.get_most_relevant_value(wild_code, 'height', None)

    @staticmethod
    async def get_volume(length: Optional[int], width: Optional[int], height: Optional[int]) -> Optional[float]:
        """
        Рассчитывает объем товара в м³ на основе длины, ширины и высоты в сантиметрах.
        Args:
            height: Высота в сантиметрах
            width: Ширина в сантиметрах
            length: Длина в сантиметрах
        Returns:
            Optional[float]: Объем товара в м³
        """

        if length is None or width is None or height is None:
            return 0

        return (length * width * height) / 1_000_000

    async def get_rating(self, wild_code: str) -> Optional[float]:
        """
        Получает рейтинг товара, выбирая наиболее актуальное значение.
        Args:
            wild_code: Уникальный номер wild
        Returns:
            Optional[float]: Рейтинг товара
        """
        return await self.get_most_relevant_value(wild_code, 'rating', None)

    async def get_colors(self, wild_code: str) -> Optional[List[str]]:
        """
        Получает список цветов товара, выбирая наиболее актуальное значение.
        Args:
            wild_code: Уникальный номер wild
        Returns:
            Optional[List[str]]: Список цветов товара
        """
        return await self.get_most_relevant_value(wild_code, 'colors', None)
    
    async def get_weight_brutto(self, wild_code: str) -> Optional[float]:
        """
        Получает вес товара в кг, выбирая наиболее актуальное значение.
        Args:
            wild_code: Уникальный номер wild
        Returns:
            Optional[float]: Вес товара
        """
        return await self.get_most_relevant_value(wild_code, 'weight_brutto', None)

    async def parse_wild_string(self, wild_string: str) -> WildParserResponse:
        """
        Парсит строку формата 'wild123/23', извлекая wild и количество.
        Args:
            wild_string: Строка в формате 'wild123/23'
        Returns:
            WildParserResponse: Информация о товаре, извлеченная из строки
        """

        wild_code, quantity = await self.parse_wild_string_parts(wild_string)

        name_file = await self.get_name(wild_code)
        name_db = await self.get_name_db(wild_code)
        photos = await self.get_photos(wild_code)
        length = await self.get_length(wild_code)
        width = await self.get_width(wild_code)
        height = await self.get_height(wild_code)
        volume = await self.get_volume(length, width, height)
        rating = await self.get_rating(wild_code)
        colors = await self.get_colors(wild_code)
        weight_brutto = await self.get_weight_brutto(wild_code)

        return WildParserResponse(
            wild=wild_code,
            quantity=quantity,
            name_file=name_file,
            name_db=name_db,
            photos=photos,
            length=length,
            width=width,
            height=height,
            volume=volume,
            rating=rating,
            colors=colors,
            weight_brutto=weight_brutto
        )


class QRLookupService:
    """Сервис для поиска данных по QR-коду."""

    def __init__(self, db=None):
        """
        Инициализирует сервис для поиска по QR-коду.
        Args:
            db: Соединение с базой данных
        """
        self.db = db

    async def find_by_qr_data(self, qr_data: str) -> QRLookupResponse:
        """
        Ищет данные по QR-коду в таблице qr_scans и соответствующий заказ в orders_wb одним запросом.
        
        Args:
            qr_data: QR код стикера, например '*CN+tGIpw'
            
        Returns:
            QRLookupResponse: Найденные данные или пустой ответ
        """
        logger.info(f"Поиск данных по QR-коду: {qr_data}")
        
        try:
            # Объединенный запрос с LEFT JOIN
            data = await self._find_qr_and_order_data(qr_data)
            
            if not data:
                logger.warning(f"QR-код не найден: {qr_data}")
                return QRLookupResponse(
                    found=False,
                    data=None
                )
            
            if data['order_id'] is not None:
                logger.info(f"Найден заказ по order_id {data['qr_order_id']}: {data['order_uid']}")
            else:
                logger.warning(f"Заказ с order_id {data['qr_order_id']} не найден")
            
            return QRLookupResponse(
                found=True,
                data=data
            )
                
        except Exception as e:
            logger.error(f"Ошибка при поиске по QR-коду {qr_data}: {str(e)}")
            raise
    
    async def _find_qr_and_order_data(self, qr_data: str) -> Optional[Dict[str, Any]]:
        """
        Ищет данные QR-скана и связанного заказа одним запросом через LEFT JOIN.
        
        Args:
            qr_data: QR код для поиска
            
        Returns:
            Optional[Dict[str, Any]]: Объединенные данные или None
        """
        query = """
            SELECT 
                -- QR scan data with prefixes
                qr.id as qr_id,
                qr.order_id as qr_order_id,
                qr.qr_data,
                qr.account as qr_account,
                qr.part_a as qr_part_a,
                qr.part_b as qr_part_b,
                qr.created_at as qr_created_at,
                
                -- Order data with prefixes (may be NULL if no matching order)
                o.id as order_id,
                o.order_uid,
                o.rid as order_rid,
                o.article as order_article,
                o.nm_id as order_nm_id,
                o.chrt_id as order_chrt_id,
                o.color_code as order_color_code,
                o.price as order_price,
                o.sale_price as order_sale_price,
                o.converted_price as order_converted_price,
                o.delivery_type as order_delivery_type,
                o.supply_id as order_supply_id,
                o.address as order_address,
                o.comment as order_comment,
                o.created_at as order_created_at
                
            FROM qr_scans qr
            LEFT JOIN orders_wb o ON qr.order_id = o.id
            WHERE qr.qr_data = $1
            LIMIT 1
        """

        try:
            row = await self.db.fetchrow(query, qr_data)
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка при объединенном поиске QR-скана и заказа: {str(e)}")
            raise
