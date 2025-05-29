"""
Сервис для работы с карточками товаров Wildberries.
"""
from typing import List, Dict, Any, Optional

from src.logger import app_logger as logger
from src.wildberries_api.cards import Cards
from src.models.article import ArticleDB
from src.utils import get_wb_tokens


class CardsService:
    """Сервис для работы с карточками товаров Wildberries."""

    def __init__(self, db=None):
        """
        Инициализирует сервис для работы с карточками.
        Args:
            db: Соединение с базой данных (опционально)
        """
        self.db = db

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

    async def find_cards_by_wild(self, wild: str, vendor_codes: List[str]) -> List[Dict[str, Any]]:
        """
        Находит карточки товаров по артикулу продавца (wild).
        """
        tokens = get_wb_tokens()
        if not tokens:
            logger.error("Не найдены токены для аккаунтов Wildberries")
            return []

        found_cards = []

        for account, token in tokens.items():
            logger.info(f"Поиск карточек с артикулом {wild} в аккаунте {account}")
            
            try:
                cards_api = Cards(account, token)
                cards_data = await cards_api.get_cards_list(vendor_codes)
                
                if cards_data:
                    logger.info(f"Найдено {len(cards_data)} карточек с артикулом {wild} в аккаунте {account}")
                    for card in cards_data:
                        card["account"] = account
                        card["token"] = token
                        found_cards.append(card)
                else:
                    logger.info(f"Карточки с артикулом {wild} не найдены в аккаунте {account}")
                    
            except Exception as e:
                logger.error(f"Ошибка при получении карточек из аккаунта {account}: {str(e)}")
                continue

        return found_cards

    async def update_card_dimensions(self, cards: List[Dict[str, Any]], width: Optional[float], 
                                   length: Optional[float], height: Optional[float], 
                                   weight: Optional[float]) -> Dict[str, Any]:
        """
        Обновляет размеры и вес для списка карточек товаров.
        """
        updated_count = 0
        errors = []
        
        for card in cards:
            account = card["account"]
            token = card["token"]
            nm_id = card.get("nmID")
            
            characteristics = card.get("characteristics", [])
            changed_chars = 0
            
            # Обновляем характеристики
            for char in characteristics:
                name = char.get("name", "").lower()
                
                if width is not None and "ширина" in name:
                    char["value"] = str(width)
                    changed_chars += 1
                elif length is not None and "длина" in name:
                    char["value"] = str(length)
                    changed_chars += 1
                elif height is not None and "высота" in name:
                    char["value"] = str(height)
                    changed_chars += 1
                elif weight is not None and ("вес" in name or "масса" in name):
                    char["value"] = str(weight)
                    changed_chars += 1
                    
            if changed_chars == 0:
                logger.info(f"Не найдено соответствующих характеристик для обновления в карточке {nm_id}")
                continue
                
            try:
                cards_api = Cards(account, token)
                update_data = {"nmID": nm_id, "characteristics": characteristics}
                await cards_api.update_cards([update_data])
                
                logger.info(f"Обновлены размеры для карточки {nm_id} в аккаунте {account}")
                updated_count += 1
                
            except Exception as e:
                error_msg = f"Ошибка при обновлении карточки {nm_id} в аккаунте {account}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)

        return {
            "success": updated_count > 0,
            "updated_count": updated_count,
            "errors": errors if errors else None
        }
    async def update_dimensions(self, wild: str, width: Optional[float] = None, 
                              length: Optional[float] = None, height: Optional[float] = None, 
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
        
        # Обновляем размеры карточек
        result = await self.update_card_dimensions(found_cards, width, length, height, weight)
        
        result["found_cards_count"] = len(found_cards)
        result["vendor_codes"] = vendor_codes
        
        return result
