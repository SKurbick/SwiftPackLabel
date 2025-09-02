"""
Модуль для парсинга PDF листов подбора Wildberries в JSON формат
"""
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

try:
    import PyPDF2
    PYPDF2_AVAILABLE = True
except ImportError:
    PYPDF2_AVAILABLE = False

try:
    import fitz  # PyMuPDF
    FITZ_AVAILABLE = True
except ImportError:
    FITZ_AVAILABLE = False

from src.logger import app_logger as logger


class PDFParseError(Exception):
    """Исключение для ошибок парсинга PDF"""
    pass


class PickingListParser:
    """Парсер листов подбора в формате PDF"""
    
    def __init__(self):
        """Инициализация парсера"""
        if not PYPDF2_AVAILABLE and not FITZ_AVAILABLE:
            logger.error("Не установлены библиотеки для работы с PDF. Установите: pip install PyPDF2 или pip install PyMuPDF")
            raise ImportError("Требуется установка PyPDF2 или PyMuPDF для работы с PDF")
        
        # Регулярные выражения для парсинга
        self.patterns = {
            'header': re.compile(r'Лист подбора (WB-[A-Z]+-\d+)'),
            'date': re.compile(r'Дата: (\d{2}\.\d{2}\.\d{4})'),
            'quantity': re.compile(r'Количество товаров: (\d+)'),
            'order_line': re.compile(
                r'(\d+)\s+'  # Номер задания
                r'([А-Яа-я\s]+)\s+'  # Бренд
                r'([А-Яа-я\s]+)\s+'  # Наименование
                r'(\d+)\s+'  # Размер
                r'([а-я]+)\s+'  # Цвет
                r'([a-z\d]+)\s+'  # Артикул продавца
                r'(\d+)\s+(\d+)'  # Стикер (две части)
            )
        }
    
    def extract_text_pypdf2(self, pdf_data) -> str:
        """
        Извлекает текст из PDF с помощью PyPDF2
        
        Args:
            pdf_data: Путь к PDF файлу или байты PDF
            
        Returns:
            str: Извлеченный текст
        """
        text = ""
        try:
            if isinstance(pdf_data, (str, Path)):
                # Если это путь к файлу
                with open(pdf_data, 'rb') as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    for page in pdf_reader.pages:
                        text += page.extract_text() + "\n"
            else:
                # Если это байты
                from io import BytesIO
                pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_data))
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
            return text
        except Exception as e:
            logger.error(f"Ошибка извлечения текста PyPDF2: {e}")
            raise PDFParseError(f"Не удалось извлечь текст из PDF: {e}")
    
    def extract_text_fitz(self, pdf_data) -> str:
        """
        Извлекает текст из PDF с помощью PyMuPDF (fitz)
        
        Args:
            pdf_data: Путь к PDF файлу или байты PDF
            
        Returns:
            str: Извлеченный текст
        """
        text = ""
        try:
            if isinstance(pdf_data, (str, Path)):
                # Если это путь к файлу
                doc = fitz.open(pdf_data)
            else:
                # Если это байты, используем stream
                doc = fitz.open(stream=pdf_data, filetype="pdf")
            
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                text += page.get_text() + "\n"
            doc.close()
            return text
        except Exception as e:
            logger.error(f"Ошибка извлечения текста PyMuPDF: {e}")
            raise PDFParseError(f"Не удалось извлечь текст из PDF: {e}")
    
    def extract_text_from_pdf(self, pdf_data) -> str:
        """
        Извлекает текст из PDF файла или байтов
        
        Args:
            pdf_data: Путь к PDF файлу или байты PDF
            
        Returns:
            str: Извлеченный текст
        """
        if isinstance(pdf_data, (str, Path)) and not Path(pdf_data).exists():
            raise PDFParseError(f"Файл не найден: {pdf_data}")
        
        # Сначала пробуем PyMuPDF (лучше работает с кириллицей)
        if FITZ_AVAILABLE:
            return self.extract_text_fitz(pdf_data)
        elif PYPDF2_AVAILABLE:
            return self.extract_text_pypdf2(pdf_data)
        else:
            raise PDFParseError("Нет доступных библиотек для работы с PDF")
    
    def parse_header_info(self, text: str) -> Dict[str, Any]:
        """
        Парсит информацию из заголовка документа
        
        Args:
            text: Текст документа
            
        Returns:
            Dict: Информация из заголовка
        """
        header_info = {}
        
        # Поиск ID поставки
        supply_match = self.patterns['header'].search(text)
        if supply_match:
            header_info['supply_id'] = supply_match.group(1)
        
        # Поиск даты
        date_match = self.patterns['date'].search(text)
        if date_match:
            date_str = date_match.group(1)
            try:
                parsed_date = datetime.strptime(date_str, '%d.%m.%Y')
                header_info['date'] = parsed_date.strftime('%Y-%m-%d')
                header_info['date_original'] = date_str
            except ValueError:
                header_info['date_original'] = date_str
        
        # Поиск количества товаров
        quantity_match = self.patterns['quantity'].search(text)
        if quantity_match:
            header_info['total_quantity'] = int(quantity_match.group(1))
        
        return header_info
    
    def parse_order_lines(self, text: str) -> List[Dict[str, Any]]:
        """
        Парсит строки с заказами
        
        Args:
            text: Текст документа
            
        Returns:
            List[Dict]: Список заказов
        """
        orders = []
        lines = text.split('\n')
        
        current_order = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Поиск номера задания (начало новой записи)
            order_id_match = re.match(r'^(\d{10,})', line)
            if order_id_match:
                if current_order:
                    orders.append(current_order)
                
                current_order = {
                    'order_id': order_id_match.group(1),
                    'brand': '',
                    'product_name': '',
                    'size': '',
                    'color': '',
                    'seller_article': '',
                    'sticker_code': '',
                    'sticker_number': ''
                }
                continue
            
            if not current_order:
                continue
            
            # Парсинг данных товара
            if 'Аппликатор' in line and not current_order['brand']:
                current_order['brand'] = line.strip()
            elif 'Кузнецова' in line and not current_order['brand']:
                current_order['brand'] += f" {line.strip()}"
            elif 'Массажный коврик' in line and not current_order['product_name']:
                current_order['product_name'] = line.strip()
            elif line.strip() in ['0', '1', '2', '3'] and not current_order['size']:
                current_order['size'] = line.strip()
            elif line.strip() in ['фиолетовый', 'синий', 'зеленый', 'красный'] and not current_order['color']:
                current_order['color'] = line.strip()
            elif line.startswith('wild') and not current_order['seller_article']:
                current_order['seller_article'] = line.strip()
            elif re.match(r'^\d{7}\s+\d{4}$', line) and not current_order['sticker_code']:
                parts = line.split()
                current_order['sticker_code'] = parts[0]
                current_order['sticker_number'] = parts[1]
        
        # Добавляем последний заказ
        if current_order:
            orders.append(current_order)
        
        return orders
    
    def parse_picking_list_alternative(self, text: str) -> List[Dict[str, Any]]:
        """
        Альтернативный метод парсинга с использованием более гибкого подхода
        
        Args:
            text: Текст документа
            
        Returns:
            List[Dict]: Список заказов
        """
        orders = []
        
        # Разбиваем текст на блоки по номерам заданий
        order_blocks = re.split(r'(\d{10,})', text)[1:]  # Убираем первый пустой элемент
        
        for i in range(0, len(order_blocks), 2):
            if i + 1 >= len(order_blocks):
                break
            
            order_id = order_blocks[i].strip()
            order_data = order_blocks[i + 1] if i + 1 < len(order_blocks) else ""
            
            # Извлекаем данные заказа
            order = {
                'order_id': order_id,
                'brand': '',
                'product_name': '',
                'size': '0',
                'color': '',
                'seller_article': '',
                'sticker_code': '',
                'sticker_number': ''
            }
            
            # Поиск бренда
            brand_match = re.search(r'(Аппликатор\s+Кузнецова)', order_data)
            if brand_match:
                order['brand'] = brand_match.group(1).replace('\n', ' ').strip()
            
            # Поиск наименования товара
            product_match = re.search(r'(Массажный коврик[^0-9]+)', order_data)
            if product_match:
                order['product_name'] = product_match.group(1).replace('\n', ' ').strip()
            
            # Поиск цвета
            color_match = re.search(r'(фиолетовый|синий|зеленый|красный)', order_data)
            if color_match:
                order['color'] = color_match.group(1)
            
            # Поиск артикула продавца
            article_match = re.search(r'(wild\d+[a-z]*)', order_data)
            if article_match:
                order['seller_article'] = article_match.group(1)
            
            # Поиск стикера
            sticker_match = re.search(r'(\d{7})\s+(\d{4})', order_data)
            if sticker_match:
                order['sticker_code'] = sticker_match.group(1)
                order['sticker_number'] = sticker_match.group(2)
            
            orders.append(order)
        
        return orders
    
    def parse_pdf_to_json(self, pdf_data, output_path: Optional[str] = None, source_filename: Optional[str] = None) -> Dict[str, Any]:
        """
        Парсит PDF файл листа подбора в JSON
        
        Args:
            pdf_data: Путь к PDF файлу или байты PDF
            output_path: Путь для сохранения JSON (опционально)
            source_filename: Имя файла для метаданных (если передаются байты)
            
        Returns:
            Dict: Структурированные данные в виде словаря
        """
        try:
            if isinstance(pdf_data, (str, Path)):
                source_name = str(Path(pdf_data).name)
                logger.info(f"Начинаем парсинг PDF файла: {pdf_data}")
            else:
                source_name = source_filename or "uploaded_file.pdf"
                logger.info(f"Начинаем парсинг PDF из байтов: {source_name}")
            
            # Извлекаем текст из PDF
            text = self.extract_text_from_pdf(pdf_data)
            
            # Парсим заголовок
            header_info = self.parse_header_info(text)
            
            # Парсим заказы (используем альтернативный метод)
            orders = self.parse_picking_list_alternative(text)
            
            # Подготавливаем метаданные для добавления в каждый заказ
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
            
            # Формируем результат
            result = {
                'orders': enriched_orders,
                'statistics': {
                    'total_orders_found': len(orders),
                    'expected_quantity': header_info.get('total_quantity', 0),
                    'parsing_success': len(orders) > 0
                }
            }
            
            
            logger.info(f"Парсинг завершен. Найдено заказов: {len(orders)}")
            return result
            
        except Exception as e:
            logger.error(f"Ошибка парсинга PDF: {e}")
            raise PDFParseError(f"Не удалось распарсить PDF: {e}")