"""
Сервис для работы с карточками товаров Wildberries.
"""
from typing import List, Dict, Any, Optional, Tuple

from src.logger import app_logger as logger
from src.wildberries_api.cards import Cards
from src.utils import get_wb_tokens


class CardsService:
    """
    Сервис для работы с карточками товаров Wildberries.
    Содержит методы для поиска и обновления данных карточек.
    """

    @staticmethod
    async def find_cards_by_wild(wild: str, vendor_codes: list) -> Tuple[List[Dict[str, Any]], str]:
        """
        Находит карточки товаров по артикулу продавца (wild).

        Args:
            wild: Артикул продавца
            vendor_codes: Список vendor кода
        Returns:
            Tuple[List[Dict[str, Any]], str]: Кортеж из списка найденных карточек и сообщения об ошибке (если есть)
        """
        # Получаем токены для всех аккаунтов
        tokens = get_wb_tokens()
        if not tokens:
            return [], "Не найдены токены для аккаунтов Wildberries"

        found_cards = []

        # Для каждого аккаунта получаем карточки и ищем нужную по wild
        for account, token in tokens.items():
            logger.info(f"Поиск карточек с артикулом {wild} в аккаунте {account}")
            cards_api = Cards(account, token)

            # Получаем все карточки
            try:
                cards_data = await cards_api.get_cards_list(vendor_codes)
            except Exception as e:
                logger.error(f"Ошибка при получении карточек из аккаунта {account}: {str(e)}")
                continue



            if cards_data:
                logger.info(f"Найдено {len(cards_data)} карточек с артикулом {wild} в аккаунте {account}")
                # Добавляем информацию об аккаунте для каждой карточки
                for card in cards_data:
                    card["account"] = account
                    card["token"] = token
                    found_cards.append(card)
            else:
                logger.info(f"Карточки с артикулом {wild} не найдены в аккаунте {account}")

        if not found_cards:
            return [], f"Не найдено карточек с артикулом {wild} во всех аккаунтах"

        return found_cards, ""

    @staticmethod
    async def update_card_dimensions(cards: List[Dict[str, Any]], width: float, length: float, height: float,
                                     weight: float) -> Dict[str, Any]:
        """
        Обновляет размеры и вес для списка карточек товаров.
        
        Args:
            cards: Список карточек для обновления
            width: Ширина товара (см)
            length: Длина товара (см)
            height: Высота товара (см)
            weight: Вес товара (г)
            
        Returns:
            Dict[str, Any]: Результат операции с ключами:
                - success (bool): Успешность операции
                - error (str, optional): Сообщение об ошибке
                - updated_cards_count (int, optional): Количество обновленных карточек
        """
        updated_cards = 0
        errors = []

        for card in cards:
            account = card["account"]
            token = card["token"]
            nm_id = card.get("nmID")

            # Создаем клиент API для работы с карточками
            cards_api = Cards(account, token)

            # Получаем текущие характеристики
            characteristics = card.get("characteristics", [])

            # Находим и обновляем характеристики размеров и веса
            for char in characteristics:
                name = char.get("name", "").lower()

                if "ширина" in name:
                    char["value"] = str(width)
                elif "длина" in name:
                    char["value"] = str(length)
                elif "высота" in name:
                    char["value"] = str(height)
                elif "вес" in name or "масса" in name:
                    char["value"] = str(weight)

            # Формируем данные для обновления
            update_data = {
                "nmID": nm_id,
                "characteristics": characteristics
            }

            try:
                # Отправляем запрос на обновление
                await cards_api.update_cards([update_data])
                logger.info(f"Обновлены размеры и вес для карточки {nm_id} в аккаунте {account}")
                updated_cards += 1
            except Exception as e:
                error_msg = f"Ошибка при обновлении карточки {nm_id} в аккаунте {account}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)

        if updated_cards == 0 and errors:
            return {
                "success": False,
                "error": "; ".join(errors),
                "updated_cards_count": 0
            }

        return {
            "success": True,
            "updated_cards_count": updated_cards,
            "errors": errors if errors else None
        }

    async def get_vendor_codes_by_wild(wild: str, db) -> List[str]:
        """
        Получает список vendor_code из базы данных по указанному артикулу продавца (wild).
        
        Args:
            wild: Артикул продавца (local_vendor_code)
            db: Соединение с базой данных
            
        Returns:
            List[str]: Список найденных vendor_code
        """
        from src.models.article import ArticleDB

        try:
            article_db = ArticleDB(db)
            vendor_codes = await article_db.get_vendor_codes_by_local_vendor_code(wild)
            logger.info(f"Найдено {len(vendor_codes)} vendor_code в базе данных для wild {wild}")
            return vendor_codes
        except Exception as e:
            logger.error(f"Ошибка при получении vendor_code из базы данных: {str(e)}")
            return []

    @staticmethod
    async def update_dimensions(wild: str, width: float, length: float, height: float, weight: float, db=None) -> Dict[
        str, Any]:
        """
        Обновляет размеры и вес товара по артикулу продавца (wild).
        
        Args:
            wild: Артикул продавца
            width: Ширина товара (см)
            length: Длина товара (см)
            height: Высота товара (см)
            weight: Вес товара (г)
            db: Соединение с базой данных (опционально)
            
        Returns:
            Dict[str, Any]: Результат операции с ключами:
                - success (bool): Успешность операции
                - error (str, optional): Сообщение об ошибке
                - found_cards_count (int, optional): Количество найденных карточек
                - updated_cards_count (int, optional): Количество обновленных карточек
                - vendor_codes (List[str], optional): Список vendor_code из базы данных
        """
        vendor_codes = []
        if db:
            vendor_codes = await CardsService.get_vendor_codes_by_wild(wild, db)

        # Шаг 2: Находим карточки по артикулу wild
        found_cards, error = await CardsService.find_cards_by_wild(wild, vendor_codes)

        if error and not vendor_codes:
            return {
                "success": False,
                "error": error,
                "vendor_codes": vendor_codes
            }

        # Если карточки не найдены, но есть информация из БД
        if not found_cards and vendor_codes:
            return {
                "success": False,
                "error": f"Не найдено карточек с артикулом {wild} во всех аккаунтах, "
                         f"но в базе данных есть информация о {len(vendor_codes)} vendor_code",
                "vendor_codes": vendor_codes
            }

        # Шаг 3: Обновляем размеры и вес для найденных карточек
        update_result = await CardsService.update_card_dimensions(
            cards=found_cards,
            width=width,
            length=length,
            height=height,
            weight=weight
        )

        # Добавляем информацию о количестве найденных карточек и vendor_code из БД
        update_result["found_cards_count"] = len(found_cards)
        update_result["vendor_codes"] = vendor_codes

        return update_result
