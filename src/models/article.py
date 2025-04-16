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
