from typing import List, Dict, Any, Optional
from src.logger import app_logger as logger


class ArticleDB:
    """Модель для работы с таблицей article и card_data в базе данных."""

    def __init__(self, db):
        self.db = db

    async def get_article_info_by_nm_id(self, nm_id: int) -> Optional[Dict[str, Any]]:
        """
        Получает информацию о товаре по nm_id.
        Args:
            nm_id: Уникальный идентификатор товара
        Returns:
            Optional[Dict[str, Any]]: Словарь с информацией о товаре или None, если не найден
        """
        query = """
        SELECT a.nm_id, a.local_vendor_code, a.account,
               cd.photo_link, cd.subject_name
        FROM article a
        LEFT JOIN card_data cd ON a.nm_id = cd.article_id
        WHERE a.nm_id = $1
        """
        return await self.db.fetchrow(query, nm_id)


    async def get_articles_info_by_nm_ids(self, nm_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Получает информацию о товарах по списку nm_id с оптимизированной стратегией повторных попыток.
        Args:
            nm_ids: Список уникальных идентификаторов товаров
        Returns:
            List[Dict[str, Any]]: Список словарей с информацией о товарах
        """
        if not nm_ids:
            return []

        query = """
        SELECT a.nm_id, cd.photo_link
        FROM article a
        LEFT JOIN card_data cd ON a.nm_id = cd.article_id
        WHERE a.nm_id = ANY($1)
        """
        return await self.db.fetch(query, nm_ids)

    async def get_vendor_codes_by_local_vendor_code(self, local_vendor_code: str) -> List[str]:
        """
        Получает все уникальные vendor_code для товаров с указанным local_vendor_code (wild).
        Args:
            local_vendor_code: Локальный артикул продавца (wild)
        Returns:
            List[str]: Список уникальных vendor_code
        """
        query = """
        SELECT DISTINCT vendor_code
        FROM article
        WHERE local_vendor_code = $1
        """
        
        try:
            rows = await self.db.fetch(query, local_vendor_code)
            return [row['vendor_code'] for row in rows] if rows else []
        except Exception as e:
            logger.error(f"Ошибка при получении vendor_code по local_vendor_code {local_vendor_code}: {str(e)}")
            return []
