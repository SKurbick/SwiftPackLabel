class CardData:
    """Таблица card_data
        article_id
        barcode
        commission_wb
        height
        length
        width
        local_vendor_code
        logistic_from_wb_wh_to_opp
        photo
        discount
        price
        subject_name
        rating
    """

    def __init__(self, db):
        self.db = db

    async def get_subject_name_and_photo_to_article(self, article_ids: list):
        query = """
        SELECT subject_name,photo_link, article_id FROM card_data
        WHERE article_id = ANY($1)
        """
        return await self.db.fetch(query, article_ids)
