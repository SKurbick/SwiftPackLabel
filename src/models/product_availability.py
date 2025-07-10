class ProductAvailability:
    """Модель для работы с представлением product_availability"""

    def __init__(self, db):
        self.db = db

    async def get_all_products_availability(self):
        """Получение доступности всех товаров"""
        query = "SELECT * FROM product_availability"
        rows = await self.db.fetch(query)
        return [dict(row) for row in rows]


    async def get_kits_availability(self):
        """Получение доступности только комплектов"""
        query = "SELECT * FROM product_availability WHERE is_kit = TRUE"
        rows = await self.db.fetch(query)
        return [dict(row) for row in rows]
