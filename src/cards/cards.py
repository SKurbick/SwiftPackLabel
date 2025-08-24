"""
Сервис для работы с карточками товаров Wildberries.
"""
import asyncio
from typing import List, Dict, Any, Optional

from src.logger import app_logger as logger
from src.wildberries_api.cards import Cards
from src.models.article import ArticleDB
from src.utils import get_wb_tokens

from src.response import AsyncHttpClient, parse_json


class CardsService:
    """Сервис для работы с карточками товаров Wildberries."""

    def __init__(self, db=None):
        """
        Инициализирует сервис для работы с карточками.
        Args:
            db: Соединение с базой данных (опционально)
        """
        self.db = db
        self.async_client = AsyncHttpClient(timeout=30, retries=2, delay=1)

    async def get_vendor_codes_by_wild(self, wild: str) -> List[str]:
        """
        Получает список vendor_code из базы данных по указанному артикулу продавца (wild).
        """
        if not self.db:
            return []

        try:
            article_db = ArticleDB(self.db)
            vendor_codes = await article_db.get_vendor_codes_by_local_vendor_code(wild)
            logger.info(f"Найдено {len(vendor_codes)} vendor_code в базе данных для wild {wild}")
            return vendor_codes
        except Exception as e:
            logger.error(f"Ошибка при получении vendor_code из базы данных: {str(e)}")
            return []

    async def _find_cards_in_account(self, account: str, token: str, wild: str, vendor_codes: List[str]) -> List[
        Dict[str, Any]]:
        """
        Находит карточки в одном аккаунте.
        """
        logger.info(f"Поиск карточек с артикулом {wild} в аккаунте {account}")

        try:
            cards_api = Cards(account, token)
            cards_data = await cards_api.get_cards_list(vendor_codes)

            if cards_data:
                logger.info(f"Найдено {len(cards_data)} карточек с артикулом {wild} в аккаунте {account}")
                for card in cards_data:
                    card["account"] = account
                    card["token"] = token
                return cards_data
            else:
                logger.info(f"Карточки с артикулом {wild} не найдены в аккаунте {account}")
                return []

        except Exception as e:
            logger.error(f"Ошибка при получении карточек из аккаунта {account}: {str(e)}")
            return []

    async def find_cards_by_wild(self, wild: str, vendor_codes: List[str]) -> List[Dict[str, Any]]:
        """
        Находит карточки товаров по артикулу продавца (wild) асинхронно во всех аккаунтах.
        """
        tokens = get_wb_tokens()
        if not tokens:
            logger.error("Не найдены токены для аккаунтов Wildberries")
            return []

        tasks = [
            self._find_cards_in_account(account, token, wild, vendor_codes)
            for account, token in tokens.items()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        found_cards = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Ошибка при поиске карточек: {result}")
                continue
            if isinstance(result, list):
                found_cards.extend(result)

        logger.info(f"Всего найдено {len(found_cards)} карточек с артикулом {wild}")
        return found_cards

    async def _update_cards_in_account(self, account: str, token: str, cards: List[Dict[str, Any]],
                                       width: Optional[int], length: Optional[int],
                                       height: Optional[int], weight: Optional[float]) -> Dict[str, Any]:
        """
        Обновляет карточки в одном аккаунте.
        Все карточки обновляются одним запросом.
        """
        updated_count = 0
        errors = []

        try:
            cards_api = Cards(account, token)
            cards_to_update = []

            for card in cards:
                nm_id = card.get("nmID")
                vendor_code = card.get("vendorCode")
                brand = card.get("brand")
                title = card.get("title")
                description = card.get("description")
                characteristics = card.get("characteristics")
                sizes = card.get("sizes")

                if not nm_id or not vendor_code:
                    logger.warning(f"Пропущена карточка без nmID или vendorCode: {card}")
                    continue

                update_data = {
                    "nmID": nm_id,
                    "vendorCode": vendor_code,
                    "brand": brand,
                    "title": title,
                    "description": description,
                    "characteristics": characteristics,
                    "sizes": sizes
                }

                dimensions = {}

                current_dimensions = card.get("dimensions", {})

                if width is not None and width > 0:
                    dimensions['width'] = width
                if height is not None and height > 0:
                    dimensions['height'] = height
                if length is not None and length > 0:
                    dimensions['length'] = length
                if weight is not None and weight > 0:
                    dimensions['weightBrutto'] = round(float(weight), 3)

                if dimensions:
                    if 'width' not in dimensions and 'width' in current_dimensions:
                        dimensions['width'] = current_dimensions['width']
                    if 'height' not in dimensions and 'height' in current_dimensions:
                        dimensions['height'] = current_dimensions['height']
                    if 'length' not in dimensions and 'length' in current_dimensions:
                        dimensions['length'] = current_dimensions['length']
                    if 'weightBrutto' not in dimensions and 'weightBrutto' in current_dimensions:
                        dimensions['weightBrutto'] = current_dimensions['weightBrutto']

                    update_data["dimensions"] = dimensions
                    logger.debug(f"Подготовлены данные для обновления карточки {nm_id}: {update_data}")
                    cards_to_update.append(update_data)
                else:
                    logger.warning(f"Нет данных для обновления карточки {nm_id}")

            if cards_to_update:
                try:
                    logger.info(f"Отправка {len(cards_to_update)} карточек на обновление в аккаунте {account}")
                    logger.debug(
                        f"Пример структуры карточки: {cards_to_update[0] if cards_to_update else 'Нет карточек'}")

                    result = await cards_api.update_cards(cards_to_update)
                    updated_count = len(cards_to_update)
                    logger.info(f"Обновлены размеры для {updated_count} карточек в аккаунте {account}")
                    logger.debug(f"Результат API: {result}")
                except Exception as e:
                    error_msg = f"Ошибка при массовом обновлении {len(cards_to_update)} карточек в аккаунте {account}: {str(e)}"
                    logger.error(error_msg)
                    logger.error(f"Детали ошибки: {type(e).__name__}: {e}")
                    if hasattr(e, 'status') and hasattr(e, 'message'):
                        logger.error(f"HTTP статус: {e.status}, Сообщение: {e.message}")
                    errors.append(error_msg)

        except Exception as e:
            error_msg = f"Ошибка при работе с аккаунтом {account}: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)

        return {
            "account": account,
            "updated_count": updated_count,
            "errors": errors
        }

    async def update_card_dimensions(self, cards: List[Dict[str, Any]], width: Optional[int],
                                     length: Optional[int], height: Optional[int],
                                     weight: Optional[float]) -> Dict[str, Any]:
        """
        Обновляет размеры и вес для списка карточек товаров асинхронно по всем аккаунтам.
        """
        cards_by_account = {}
        for card in cards:
            account = card["account"]
            if account not in cards_by_account:
                cards_by_account[account] = []
            cards_by_account[account].append(card)

        tasks = [
            self._update_cards_in_account(account, cards_list[0]["token"], cards_list, width, length, height, weight)
            for account, cards_list in cards_by_account.items()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        total_updated = 0
        all_errors = []

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Ошибка при обновлении карточек: {result}")
                all_errors.append(str(result))
                continue
            if isinstance(result, dict):
                total_updated += result.get("updated_count", 0)
                if result.get("errors"):
                    all_errors.extend(result["errors"])
                logger.info(f"Аккаунт {result['account']}: обновлено {result['updated_count']} карточек")

        return {"success": total_updated > 0, "updated_count": total_updated, "errors": all_errors or None}

    async def check_cards(self, cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Проверяет доступность карточек через публичное API Wildberries.
        Возвращает только карточки, которые доступны для обновления.
        """

        async def check_single_card(card: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            nm_id = card.get("nmID")
            if not nm_id:
                return None

            url = f"https://card.wb.ru/cards/v4/list?appType=1&curr=rub&dest=-1257786&spp=30&ab_testing=false&lang=ru&nm={nm_id}&ignore_stocks=true"

            try:
                response_text = await self.async_client.get(url)

                if response_text:
                    data = parse_json(response_text)

                    # Проверяем есть ли данные о карточке
                    if data and data.get("products"):
                        logger.debug(f"Карточка {nm_id} доступна")
                        return card
                    else:
                        logger.warning(f"Карточка {nm_id} не найдена в публичном API")
                        return None
                else:
                    logger.warning(f"Пустой ответ для карточки {nm_id}")
                    return None

            except Exception as e:
                logger.error(f"Ошибка при проверке карточки {nm_id}: {str(e)}")
                return None

        # Проверяем все карточки параллельно
        tasks = [check_single_card(card) for card in cards]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Фильтруем результаты
        filtered_cards = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Ошибка при проверке карточки: {result}")
                continue
            if result is not None:
                filtered_cards.append(result)

        logger.info(f"Из {len(cards)} карточек прошли проверку {len(filtered_cards)}")
        return filtered_cards

    async def update_dimensions(self, wild: str, width: Optional[int] = None,
                                length: Optional[int] = None, height: Optional[int] = None,
                                weight: Optional[float] = None) -> Dict[str, Any]:
        """
        Обновляет размеры и вес товара по артикулу продавца (wild).
        """
        if not any([width, length, height, weight]):
            return {"success": False, "error": "Не указаны параметры для обновления"}

        vendor_codes = await self.get_vendor_codes_by_wild(wild)

        found_cards = await self.find_cards_by_wild(wild, vendor_codes)
        if not found_cards:
            return {"success": False, "error": f"Не найдено карточек с артикулом {wild}"}

        # Проверяем доступность карточек перед обновлением
        validated_cards = await self.check_cards(found_cards)
        if not validated_cards:
            return {"success": False, "error": f"Все найденные карточки с артикулом {wild} недоступны для обновления"}

        result = await self.update_card_dimensions(validated_cards, width, length, height, weight)

        result["found_cards_count"] = len(found_cards)
        result["vendor_codes"] = vendor_codes

        return result
