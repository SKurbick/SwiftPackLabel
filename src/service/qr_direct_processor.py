import asyncio
from typing import Dict, List, Any

from src.logger import app_logger as logger
from src.models.qr_scan import QRScanCreate
from src.wildberries_api.orders import Orders
from src.utils import get_wb_tokens


class QRDirectProcessor:
    """Прямой обработчик QR-кодов для заказов"""
    
    def __init__(self, db_manager):
        """
        Инициализация процессора
        
        Args:
            db_manager: DatabaseManager для получения соединений из пула
        """
        self.tokens = get_wb_tokens()
        self.db_manager = db_manager
    
    def _validate_input(self, account: str, order_ids: List[int]) -> bool:
        """
        Валидация входных параметров
        
        Args:
            account: Название кабинета
            order_ids: Список ID сборочных заданий
            
        Returns:
            bool: True если все проверки пройдены, False иначе
        """
        # Проверяем наличие менеджера БД
        if not self.db_manager:
            logger.error("Не передан менеджер базы данных")
            return False
        
        # Проверяем наличие токена
        if account not in self.tokens:
            logger.error(f'Токен для кабинета {account} не найден')
            return False
        
        # Проверяем что есть заказы для обработки
        if not order_ids:
            logger.warning("Список сборочных заданий пуст")
            return False
            
        return True

    async def process_orders_qr(self, account: str, order_ids: List[int]):
        """
        Основная функция обработки QR для заказов
        
        Args:
            account: Название кабинета
            order_ids: Список ID сборочных заданий
        """
        logger.info(f"Заказов для обработки: {len(order_ids)}")
        
        # Валидация входных параметров
        if not self._validate_input(account, order_ids):
            return
        
        try:
            # 1. Получение стикеров из WB API
            logger.info(f"Получение стикеров для {len(order_ids)} заказов")
            stickers = await self._get_stickers_from_wb(account, order_ids)
            
            if not stickers:
                logger.warning("Не получено ни одного стикера")
                return
            
            logger.info(f"Получено {len(stickers)} стикеров")
            
            # 2. Извлечение QR-данных из стикеров
            logger.info(f"Извлечение QR-данных из {len(stickers)} стикеров")
            qr_data = await self._extract_qr_data(account, stickers)
            
            if not qr_data:
                logger.warning("Не извлечено ни одного QR-кода")
                return
            
            logger.info(f"Извлечено {len(qr_data)} QR-записей")
            
            # 3. Сохранение в базу данных
            logger.info(f"Сохранение {len(qr_data)} записей в БД")
            saved_count = await self._save_to_database(qr_data)

            logger.info(f"Сохранено {saved_count} записей в БД")
            
        except Exception as e:
            logger.error(f"Ошибка QR обработки для {account}: {e}")
    
    
    async def _get_stickers_from_wb(self, account: str, order_ids: List[int]) -> List[Dict[str, Any]]:
        """Получение стикеров из WB API"""
        try:
            token = self.tokens[account]
            orders_api = Orders(account=account, token=token)
            
            mock_supply = "direct_processing"
            
            logger.debug(f"Запрос стикеров через WB API для {len(order_ids)} заказов")
            stickers_data = await orders_api.get_stickers_to_orders(mock_supply, order_ids)
            
            # Извлекаем стикеры из ответа
            all_stickers = []
            if account in stickers_data and mock_supply in stickers_data[account]:
                all_stickers = stickers_data[account][mock_supply].get('stickers', [])
            
            logger.debug(f"Получено {len(all_stickers)} стикеров от WB API")
            return all_stickers
            
        except Exception as e:
            logger.error(f"Ошибка получения стикеров для {account}: {e}")
            return []
    
    async def _extract_qr_data(self, account: str, stickers: List[Dict[str, Any]]) -> List[QRScanCreate]:
        """Извлечение QR-данных из стикеров"""
        logger.debug(f"Извлечение QR-данных из {len(stickers)} стикеров")
        qr_data = []
        
        for i, sticker in enumerate(stickers):
            try:
                order_id = sticker.get('orderId')
                barcode = sticker.get('barcode')  # Это QR-код
                part_a = sticker.get('partA', '')  # Артикул товара
                part_b = sticker.get('partB', '')  # Суффикс
                
                if not order_id:
                    logger.warning(f"Стикер {i+1}: отсутствует orderId")
                    continue
                
                if not barcode:
                    logger.warning(f"Стикер {i+1}: отсутствует barcode для заказа {order_id}")
                    continue
                
                
                qr_record = QRScanCreate(
                    order_id=order_id,
                    qr_data=barcode,
                    account=account,
                    part_a=part_a or None,
                    part_b=part_b or None
                )
                
                qr_data.append(qr_record)
                logger.debug(f"QR извлечен для заказа {order_id}: barcode='{barcode[:20]}...'")
                
            except Exception as e:
                logger.error(f"Ошибка обработки стикера {i+1}: {e}")
                continue
        
        logger.info(f"Успешно извлечено {len(qr_data)} QR-записей из {len(stickers)} стикеров")
        return qr_data
    
    async def _save_to_database(self, qr_data: List[QRScanCreate]) -> int:
        """Сохранение QR-данных в базу"""
        if not qr_data:
            logger.info("Нет данных для сохранения в БД")
            return 0

        logger.info(f"Сохранение {len(qr_data)} записей в базу данных")

        try:
            # Получаем отдельное соединение из пула для этой операции
            async with self.db_manager.connection() as connection:
                # Создаем универсальный SQL запрос для любого количества записей
                values_placeholders = []
                all_values = []
                
                for i, record in enumerate(qr_data):
                    base_idx = i * 5
                    placeholders = f"(${base_idx + 1}, ${base_idx + 2}, ${base_idx + 3}, ${base_idx + 4}, ${base_idx + 5})"
                    values_placeholders.append(placeholders)
                    all_values.extend([record.order_id, record.qr_data, record.account,
                                       record.part_a, record.part_b])
                
                query = f"""
                INSERT INTO qr_scans (order_id, qr_data, account, part_a, part_b)
                VALUES {', '.join(values_placeholders)}
                ON CONFLICT (order_id, qr_data) DO NOTHING
                """
                
                await connection.execute(query, *all_values)
                saved_count = len(qr_data)

                logger.info(f"Вставка выполнена: {saved_count} записей обработано "
                           f"(дубликаты пропущены автоматически)")
                return saved_count

        except Exception as e:
            logger.error(f"Критическая ошибка сохранения в БД: {e}")
            return 0