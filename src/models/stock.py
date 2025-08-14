from typing import List, Dict


class StockDB:
    """
    Класс для работы с данными о складских запасах товаров в базе данных.
    Предоставляет методы для получения информации о количестве товаров 
    на складе по их артикулам (wild-кодам).
    """

    def __init__(self, db=None):
        self.db = db

    async def get_stocks_by_wilds(self, wilds: List[str]) -> Dict[str,int]:
        """
        Получает информацию о количестве товаров на складе по списку артикулов (wild-кодов).
        Args:
            wilds: Список артикулов (wild-кодов) для получения информации о запасах
        Returns:
            Dict[str, int]: Словарь, где ключи - артикулы товаров,
            а значения - количество единиц товара на складе. 
            Если количество не является целым числом, возвращается 0.
        """
        query = """SELECT local_vendor_code ,stocks_quantity from current_real_fbs_stocks_qty WHERE local_vendor_code = ANY($1)"""
        result = await self.db.fetch(query, wilds)
        return {res['local_vendor_code']: (res['stocks_quantity'] if isinstance(res['stocks_quantity'], int) else 0)
                for res in result}

    async def get_current_by_wilds(self, wilds: List[str]) -> Dict[str, int]:
        """
        Получает информацию о количестве товаров на складе по списку артикулов (wild-кодов).
        Args:
            wilds: Список артикулов (wild-кодов) для получения информации о запасах
        Returns:
            Dict[str, int]: Словарь, где ключи - артикулы товаров,
            а значения - количество единиц товара на складе.
            Если количество не является целым числом, возвращается 0.
        """

        query = """SELECT product_id ,available_quantity from current_balances WHERE product_id = ANY($1) AND warehouse_id = 1"""
        result = await self.db.fetch(query, wilds)
        return {res['product_id']: int(res['available_quantity'])
                for res in result}


    async def get_current_by_wilds_view(self, wilds: List[str]) -> Dict[str, int]:
        """
        Получает информацию о количестве товаров на складе по списку артикулов (wild-кодов).
        Args:
            wilds: Список артикулов (wild-кодов) для получения информации о запасах
        Returns:
            Dict[str, int]: Словарь, где ключи - артикулы товаров,
            а значения - количество единиц товара на складе.
            Если количество не является целым числом, возвращается 0.
        """

        query = """SELECT product_id ,available_stock from product_availability WHERE product_id = ANY($1)"""
        result = await self.db.fetch(query, wilds)
        return {res['product_id']: int(res['available_stock'])
                for res in result}