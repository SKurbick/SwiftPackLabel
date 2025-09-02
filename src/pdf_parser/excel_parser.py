"""
Модуль для парсинга Excel листов подбора Wildberries в JSON формат
"""
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from io import BytesIO

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from src.logger import app_logger as logger


class ExcelParseError(Exception):
    """Исключение для ошибок парсинга Excel"""
    pass


class ExcelPickingListParser:
    """Парсер Excel листов подбора аналогичный PDF парсеру"""
    
    def __init__(self):
        """Инициализация парсера"""
        if not PANDAS_AVAILABLE:
            logger.error("Pandas не установлен. Установите: pip install pandas openpyxl")
            raise ImportError("Требуется установка pandas и openpyxl для работы с Excel")
    
    def extract_data_from_excel(self, excel_data) -> pd.DataFrame:
        """
        Извлекает данные из Excel файла
        
        Args:
            excel_data: Путь к Excel файлу или байты Excel
            
        Returns:
            pd.DataFrame: Данные из Excel
        """
        try:
            if isinstance(excel_data, (str, Path)):
                # Если это путь к файлу
                df = pd.read_excel(excel_data)
            else:
                # Если это байты
                df = pd.read_excel(BytesIO(excel_data))
            
            return df
        except Exception as e:
            logger.error(f"Ошибка чтения Excel файла: {e}")
            raise ExcelParseError(f"Не удалось прочитать Excel файл: {e}")
    
    def parse_header_info_from_excel(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Извлекает информацию заголовка из Excel (аналогично PDF)
        
        Args:
            df: DataFrame с данными Excel
            
        Returns:
            Dict: Информация из заголовка
        """
        header_info = {}
        
        # Попробуем найти supply_id в столбце "QR-код поставки"
        if 'QR-код поставки' in df.columns:
            supply_ids = df['QR-код поставки'].dropna().unique()
            if len(supply_ids) > 0:
                header_info['supply_id'] = str(supply_ids[0])
        
        # Попробуем извлечь дату из столбца "Дата создания"
        if 'Дата создания' in df.columns:
            dates = df['Дата создания'].dropna()
            if len(dates) > 0:
                try:
                    # Попробуем распарсить первую дату
                    first_date = str(dates.iloc[0])
                    if '.' in first_date:
                        # Формат DD.MM.YYYY
                        date_parts = first_date.split(' ')[0].split('.')
                        if len(date_parts) == 3:
                            parsed_date = datetime.strptime(date_parts[0] + '.' + date_parts[1] + '.' + date_parts[2], '%d.%m.%Y')
                            header_info['date'] = parsed_date.strftime('%Y-%m-%d')
                            header_info['date_original'] = first_date
                except Exception as e:
                    logger.warning(f"Не удалось распарсить дату: {e}")
        
        # Общее количество заказов
        header_info['total_quantity'] = len(df)
        
        return header_info
    
    def parse_excel_orders(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Парсит заказы из DataFrame (аналогично parse_picking_list_alternative)
        
        Args:
            df: DataFrame с данными Excel
            
        Returns:
            List[Dict]: Список заказов
        """
        orders = []
        
        # Определяем маппинг столбцов Excel на поля заказа
        column_mapping = {
            '№ задания': 'order_id',
            'Наименование': 'product_name', 
            'Размер': 'size',
            'Цвет': 'color',
            'Артикул продавца': 'seller_article',
            'Стикер': 'sticker_code'
        }
        
        for index, row in df.iterrows():
            try:
                order = {
                    'order_id': str(row.get('№ задания', '')),
                    'brand': '',  # В Excel WB обычно нет отдельного поля бренда
                    'product_name': str(row.get('Наименование', '')),
                    'size': str(row.get('Размер', '0')),
                    'color': str(row.get('Цвет', '')),
                    'seller_article': str(row.get('Артикул продавца', '')),
                    'sticker_code': str(row.get('Стикер', '')),
                    'sticker_number': ''  # В Excel WB обычно стикер - одно число
                }
                
                # Попробуем извлечь бренд из наименования товара
                product_name = order['product_name']
                if product_name and len(product_name.split()) > 2:
                    # Берем первые 2 слова как возможный бренд
                    words = product_name.split()
                    order['brand'] = ' '.join(words[:2])
                
                # Убираем NaN значения и пустые строки
                for key, value in order.items():
                    if pd.isna(value) or value == 'nan':
                        order[key] = ''
                
                # Добавляем заказ только если есть номер задания
                if order['order_id'] and order['order_id'] != '':
                    orders.append(order)
                    
            except Exception as e:
                logger.warning(f"Ошибка обработки строки {index}: {e}")
                continue
        
        return orders
    
    def parse_excel_to_json(self, excel_data, output_path: Optional[str] = None, source_filename: Optional[str] = None) -> Dict[str, Any]:
        """
        Парсит Excel файл листа подбора в JSON (аналогично parse_pdf_to_json)
        
        Args:
            excel_data: Путь к Excel файлу или байты Excel
            output_path: Путь для сохранения JSON (опционально)
            source_filename: Имя файла для метаданных (если передаются байты)
            
        Returns:
            Dict: Структурированные данные в виде словаря
        """
        try:
            if isinstance(excel_data, (str, Path)):
                source_name = str(Path(excel_data).name)
                logger.info(f"Начинаем парсинг Excel файла: {excel_data}")
            else:
                source_name = source_filename or "uploaded_file.xlsx"
                logger.info(f"Начинаем парсинг Excel из байтов: {source_name}")
            
            # Извлекаем данные из Excel
            df = self.extract_data_from_excel(excel_data)
            
            # Парсим заголовок
            header_info = self.parse_header_info_from_excel(df)
            
            # Парсим заказы
            orders = self.parse_excel_orders(df)
            
            # Подготавливаем метаданные для добавления в каждый заказ (аналогично PDF)
            metadata = {
                'source_file': source_name,
                'parsed_at': datetime.now().isoformat(),
                'parser_version': '1.0.0',
                **header_info
            }
            
            # Добавляем метаданные в каждый заказ
            enriched_orders = []
            for order in orders:
                enriched_order = {**order, **metadata}
                enriched_orders.append(enriched_order)
            
            # Формируем результат (точно такой же формат как у PDF)
            result = {
                'orders': enriched_orders,
                'statistics': {
                    'total_orders_found': len(orders),
                    'expected_quantity': header_info.get('total_quantity', 0),
                    'parsing_success': len(orders) > 0
                }
            }
            
            logger.info(f"Парсинг Excel завершен. Найдено заказов: {len(orders)}")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка парсинга Excel: {e}")
            raise ExcelParseError(f"Не удалось распарсить Excel: {e}")