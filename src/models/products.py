class Products:
    """Модель для работы с таблицей products"""

    def __init__(self, db):
        self.db = db

    async def get_kit_products(self):
        """Получение комплектов товаров с их компонентами"""
        query = "SELECT id, kit_components FROM products WHERE is_kit=TRUE"
        rows = await self.db.fetch(query)
        return [dict(row) for row in rows]