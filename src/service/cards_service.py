from typing import List, Dict, Any, Tuple, Optional
from src.logger import app_logger as logger
from src.utils import get_wb_tokens
from src.wildberries_api.cards import Cards
from src.models.card_data import CardData


class CardsService:
    """Сервис для работы с карточками товаров."""

    @staticmethod
    async def get_vendor_codes_by_wild(wild: str, db=None) -> List[str]:
        """
        Получает список vendor_codes для указанного wild-кода из базы данных.
        
        Args:
            wild: Wild-код товара
            db: Соединение с базой данных
            
        Returns:
            Список vendor_codes, соответствующих указанному wild-коду
        """
        if not db:
            logger.warning(f"Не передано соединение с базой данных для получения vendor_codes по wild={wild}")
            return []
            
        card_data = CardData(db)
        vendor_codes = await card_data.get_vendor_codes_by_wild(wild)
        
        logger.info(f"Получено {len(vendor_codes)} vendor_codes для wild={wild}")
        return vendor_codes

    @staticmethod
    async def find_cards_by_wild(wild: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Находит карточки товаров по wild-коду, предварительно получив все связанные vendor_codes.
        
        Args:
            wild: Wild-код товара
            
        Returns:
            Кортеж (список карточек, сообщение об ошибке)
        """
        try:
            tokens = get_wb_tokens()
            if not tokens:
                return [], "Не настроены токены для доступа к Wildberries API"
                
            all_cards = []
            
            # Для каждого аккаунта ищем карточки
            for account, token in tokens.items():
                logger.info(f"Поиск карточек для wild={wild} в аккаунте {account}")
                cards_api = Cards(account, token)
                cards = await cards_api.get_cards_list([wild])
                all_cards.extend(cards)
                
            logger.info(f"Всего найдено {len(all_cards)} карточек для wild={wild}")
            return all_cards, None
            
        except Exception as e:
            error_message = f"Ошибка при поиске карточек для wild={wild}: {str(e)}"
            logger.error(error_message)
            return [], error_message

    @staticmethod
    async def find_cards_by_vendor_codes(vendor_codes: List[str]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Находит карточки товаров по списку vendor_codes.
        
        Args:
            vendor_codes: Список vendor_codes для поиска
            
        Returns:
            Кортеж (список карточек, сообщение об ошибке)
        """
        if not vendor_codes:
            return [], "Пустой список vendor_codes"
            
        try:
            tokens = get_wb_tokens()
            if not tokens:
                return [], "Не настроены токены для доступа к Wildberries API"
                
            all_cards = []
            
            # Для каждого аккаунта ищем карточки
            for account, token in tokens.items():
                logger.info(f"Поиск карточек для {len(vendor_codes)} vendor_codes в аккаунте {account}")
                cards_api = Cards(account, token)
                cards = await cards_api.get_cards_list(vendor_codes)
                all_cards.extend(cards)
                
            logger.info(f"Всего найдено {len(all_cards)} карточек для {len(vendor_codes)} vendor_codes")
            return all_cards, None
            
        except Exception as e:
            error_message = f"Ошибка при поиске карточек для vendor_codes: {str(e)}"
            logger.error(error_message)
            return [], error_message

    @staticmethod
    async def update_card_data(cards: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Обновляет данные карточек товаров в Wildberries.
        
        Args:
            cards: Список карточек для обновления
            
        Returns:
            Кортеж (результат обновления, сообщение об ошибке)
        """
        if not cards:
            return {}, "Пустой список карточек для обновления"
            
        try:
            # Группируем карточки по аккаунтам
            cards_by_account = {}
            for card in cards:
                account = card.get("account", "unknown")
                if account not in cards_by_account:
                    cards_by_account[account] = []
                cards_by_account[account].append(card)
                
            tokens = get_wb_tokens()
            results = {}
            
            # Обновляем карточки для каждого аккаунта
            for account, account_cards in cards_by_account.items():
                if account not in tokens:
                    logger.warning(f"Не найден токен для аккаунта {account}")
                    continue
                    
                logger.info(f"Обновление {len(account_cards)} карточек для аккаунта {account}")
                cards_api = Cards(account, tokens[account])
                result = await cards_api.update_cards(account_cards)
                results[account] = result
                
            return results, None
            
        except Exception as e:
            error_message = f"Ошибка при обновлении карточек: {str(e)}"
            logger.error(error_message)
            return {}, error_message

    @staticmethod
    async def get_cards_workflow(wild: str, db=None) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Комплексный рабочий процесс получения карточек товаров:
        1. Получает vendor_codes по wild-коду из базы данных
        2. Находит карточки по полученным vendor_codes
        
        Args:
            wild: Wild-код товара
            db: Соединение с базой данных
            
        Returns:
            Кортеж (список карточек, сообщение об ошибке)
        """
        # Шаг 1: Получаем vendor_codes из базы данных
        vendor_codes = await CardsService.get_vendor_codes_by_wild(wild, db)
        
        if not vendor_codes:
            logger.warning(f"Не найдены vendor_codes для wild={wild}")
            return [], f"Не найдены vendor_codes для wild={wild}"
            
        # Шаг 2: Получаем карточки по vendor_codes
        found_cards, error = await CardsService.find_cards_by_vendor_codes(vendor_codes)
        
        if error:
            logger.error(f"Ошибка при поиске карточек для wild={wild}: {error}")
            return [], error
            
        logger.info(f"Успешно получены {len(found_cards)} карточек для wild={wild}")
        return found_cards, None
