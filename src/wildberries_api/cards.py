from src.response import parse_json
from src.users.account import Account
from src.logger import app_logger as logger


class Cards(Account):
    """Класс для работы с карточками товаров Wildberries."""

    def __init__(self, account, token):
        """
        Инициализация класса для работы с карточками товаров.
        Args:
            account: Название аккаунта
            token: API-токен с правами на работу с контентом
        """
        super().__init__(account, token)
        self.base_url = "https://content-api.wildberries.ru/content/v2"

    async def _get_cards_by_wild(self, wild: str, with_photo=-1):
        """
        Получает список карточек товаров по конкретному wild-коду.
        Args:
            wild: Код товара для поиска
            with_photo: Фильтр по наличию фото (-1 - все, 0 - без фото, 1 - с фото)
        Returns:
            Список карточек товаров для указанного wild-кода
        """
        cards = []
        cursor = {"limit": 100}  # API ограничивает 100 карточками за запрос

        while True:
            payload = {"settings": {"cursor": cursor, "filter": {"withPhoto": with_photo, "textSearch": wild}}}

            response = await self.async_client.post(
                f"{self.base_url}/get/cards/list",
                json=payload,
                headers=self.headers
            )
            data = parse_json(response)

            batch = data.get("cards", [])
            cards.extend(batch)

            total = data.get("cursor", {}).get("total", 0)

            logger.info(f"{self.account}: wild={wild}, получено {len(batch)} карточек, всего {len(cards)}/{total}")

            # Проверяем условия выхода
            if not batch or total < cursor["limit"] or not data.get("cursor"):
                break

            cursor = {
                "limit": 100,
                "updatedAt": data["cursor"].get("updatedAt"),
                "nmID": data["cursor"].get("nmID")
            }

        return cards

    async def get_cards_list(self, vendor_codes, with_photo=-1):
        """
        Получает список карточек товаров по списку кодов vendor_codes.
        Args:
            vendor_codes: Список кодов товаров для поиска
            with_photo: Фильтр по наличию фото (-1 - все, 0 - без фото, 1 - с фото)
        Returns:
            Список карточек товаров
        """
        # Проверяем, что vendor_codes является списком
        if not isinstance(vendor_codes, list):
            vendor_codes = [vendor_codes]

        if not vendor_codes:
            logger.warning(f"{self.account}: Пустой список vendor_codes")
            return []

        logger.info(f"{self.account}: Получение карточек для {len(vendor_codes)} vendor_codes")

        # Получаем карточки для каждого кода последовательно
        all_cards = []
        for wild in vendor_codes:
            cards = await self._get_cards_by_wild(wild, with_photo)
            all_cards.extend(cards)

        logger.info(f"{self.account}: Всего получено {len(all_cards)} карточек для {len(vendor_codes)} vendor_codes")
        return all_cards

    async def update_cards(self, cards):
        """
        Обновляет карточки товаров. Если карточек больше 3000, разбивает на батчи.
        Args:
            cards: Список словарей с данными карточек для обновления
        Returns:
            Ответ от API с результатами обновления
        """
        if not isinstance(cards, list) or not cards:
            logger.warning(f"{self.account}: Пустой список карточек для обновления")
            return {"error": "Список карточек пуст"}

        # Если карточек меньше или равно 3000, обновляем одним запросом
        if len(cards) <= 3000:
            response = await self.async_client.post(
                f"{self.base_url}/cards/update",
                json=cards,
                headers=self.headers
            )
            result = parse_json(response)
            logger.info(f"{self.account}: Обновлено {len(cards)} карточек")
            return result

        batches = [cards[i:i + 3000] for i in range(0, len(cards), 3000)]
        logger.info(f"{self.account}: Разбиение {len(cards)} карточек на {len(batches)} батчей")

        results = []
        for batch in batches:
            response = await self.async_client.post(
                f"{self.base_url}/cards/update",
                json=batch,
                headers=self.headers
            )
            result = parse_json(response)
            logger.info(f"{self.account}: Обновлено {len(batch)} карточек")
            results.append(result)

        total_updated = sum(len(batch) for batch in batches)
        logger.info(f"{self.account}: Завершено обновление всех {total_updated} карточек")

        # Объединяем результаты всех батчей
        return {
            "total": len(cards),
            "updated": total_updated,
            "batches": len(batches),
            "results": results
        }
