import json
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

    def _filter_duplicate_cards(self, cards):
        """
        Удаляет дубликаты карточек из списка, оставляя только уникальные по nmID и vendorCode.
        Args:
            cards: Список карточек товаров
        Returns:
            Список карточек без дубликатов
        """
        if not cards:
            return []

        # Создаем словарь для отслеживания уникальных карточек
        unique_cards = {}
        duplicate_count = 0

        for card in cards:
            nm_id = card.get("nmID")
            vendor_code = card.get("vendorCode")
            
            if not nm_id or not vendor_code:
                logger.warning(f"Пропускаем карточку без nmID или vendorCode: {card}")
                continue

            # Создаем ключ для определения уникальности
            key = f"{nm_id}_{vendor_code}"
            
            if key not in unique_cards:
                unique_cards[key] = card
            else:
                duplicate_count += 1
                logger.debug(f"Найден дубликат карточки: nmID={nm_id}, vendorCode={vendor_code}")

        filtered_cards = list(unique_cards.values())
        
        if duplicate_count > 0:
            logger.info(f"Удалено {duplicate_count} дубликатов карточек. Осталось {len(filtered_cards)} уникальных.")
        
        return filtered_cards

    async def get_cards_list(self, vendor_codes, with_photo=-1):
        """
        Получает список карточек товаров по списку кодов vendor_codes.
        Args:
            vendor_codes: Список кодов товаров для поиска
            with_photo: Фильтр по наличию фото (-1 - все, 0 - без фото, 1 - с фото)
        Returns:
            Список карточек товаров
        """
        if not isinstance(vendor_codes, list):
            vendor_codes = [vendor_codes]

        if not vendor_codes:
            logger.warning(f"{self.account}: Пустой список vendor_codes")
            return []

        logger.info(f"{self.account}: Получение карточек для {len(vendor_codes)} vendor_codes")

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
        
        # Удаляем дубликаты по nmID и vendorCode
        filtered_cards = self._filter_duplicate_cards(cards)        
        if len(filtered_cards) < len(cards):
            logger.info(f"{self.account}: Удалено {len(cards) - len(filtered_cards)} дублирующихся карточек")
        
        # Используем отфильтрованный список карточек для дальнейшей работы
        cards = filtered_cards

        # Если карточек меньше или равно 3000, обновляем одним запросом
        if len(cards) <= 3000:
            try:
                logger.debug(f"{self.account}: Отправляем на обновление данные: {json.dumps(cards, ensure_ascii=False)[:500]}...")

                response = await self.async_client.post(
                    f"{self.base_url}/cards/update",
                    json=cards,  # Используем json параметр вместо data
                    headers=self.headers
                )
                
                # Проверяем, что получили ответ
                if response is None:
                    logger.error(f"{self.account}: Получен пустой ответ от API")
                    return {"error": "Пустой ответ от API", "success": False}
                
                # Пытаемся распарсить JSON
                try:
                    result = parse_json(response)
                    logger.info(f"{self.account}: Обновлено {len(cards)} карточек")
                    return result
                except ValueError as e:
                    # Если не удалось распарсить JSON, логируем сам ответ
                    logger.error(f"{self.account}: Ошибка парсинга ответа: {e}")
                    logger.error(f"{self.account}: Ответ сервера: {response[:1000]}")
                    return {"error": f"Ошибка парсинга ответа: {str(e)}", "response": response[:1000], "success": False}
                    
            except Exception as e:
                logger.error(f"{self.account}: Ошибка при обновлении карточек: {type(e).__name__}: {str(e)}")
                # Если это aiohttp ошибка с телом ответа, пытаемся его получить
                if hasattr(e, 'status'):
                    logger.error(f"{self.account}: HTTP статус: {e.status}")
                if hasattr(e, 'message'):
                    logger.error(f"{self.account}: Сообщение: {e.message}")
                return {"error": str(e), "success": False}

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
