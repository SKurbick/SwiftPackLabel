"""
Сервис для обработки PDF и Excel листов подбора
"""
from typing import Dict, Any, List, Tuple
from fastapi import HTTPException, status

from .pdf_parser import PickingListParser, PDFParseError
from .excel_parser import ExcelPickingListParser, ExcelParseError
from src.supplies.supplies import SuppliesService
from src.logger import app_logger as logger
from src.utils import process_local_vendor_code


class DocumentProcessingService:
    """Сервис для обработки PDF и Excel листов подбора и интеграции с системой отгрузки."""
    
    def __init__(self, db):
        self.db = db
        self.pdf_parser = PickingListParser()
        self.excel_parser = ExcelPickingListParser()
    
    def _detect_file_type(self, filename: str) -> str:
        """
        Определяет тип файла по расширению
        
        Args:
            filename: Имя файла
            
        Returns:
            str: 'pdf' или 'excel'
        """
        filename_lower = filename.lower()
        if filename_lower.endswith('.pdf'):
            return 'pdf'
        elif filename_lower.endswith(('.xlsx', '.xls')):
            return 'excel'
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Неподдерживаемый тип файла: {filename}. Поддерживаются только PDF и Excel файлы."
            )
        
    def convert_orders_to_fictitious_format(self, orders: List[Dict[str, Any]], 
                                           supply_account_map: Dict[str, str]) -> Tuple[List[Dict], Dict[str, str]]:
        """
        Преобразует заказы из PDF/Excel в формат для _send_fictitious_shipment_data
        
        Args:
            orders: Заказы из PDF/Excel парсера
            supply_account_map: Маппинг {supply_id: account}
            
        Returns:
            tuple: (selected_orders, supplies) для метода _send_fictitious_shipment_data
        """
        selected_orders = []
        supplies = {}
        
        for order in orders:
            supply_id = order.get('supply_id')
            if not supply_id:
                continue
                
            # Преобразуем в формат для selected_orders
            selected_orders.append({
                'id': int(order['order_id']),
                'supply_id': supply_id,
                'article': process_local_vendor_code(order['seller_article'])
            })
            
            # Добавляем в supplies если еще нет
            if supply_id not in supplies:
                supplies[supply_id] = supply_account_map.get(supply_id, 'unknown')
        
        return selected_orders, supplies
    
    async def _validate_supplies_belong_to_account(self, supply_account_map: Dict[str, str], supplies_service: SuppliesService):
        """
        Проверяет что все поставки действительно принадлежат указанному аккаунту.
        
        Args:
            supply_account_map: Словарь {supply_id: account}
            supplies_service: Сервис для работы с поставками
            
        Raises:
            HTTPException: Если поставка не найдена или принадлежит другому аккаунту
        """
        for supply_id, expected_account in supply_account_map.items():
            try:
                supply_info = await supplies_service.get_supply_detailed_info(supply_id, expected_account)
                
                if not supply_info:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Поставка {supply_id} не найдена в аккаунте {expected_account}"
                    )

                    
                logger.info(f"Поставка {supply_id} подтверждена для аккаунта {expected_account}")
                
            except HTTPException:
                raise
            except Exception as e:
                logger.warning(f"Не удалось проверить поставку {supply_id}: {e}")
    
    
    async def parse_and_ship(self, content: bytes, filename: str, account: str, user: dict) -> Dict[str, Any]:
        """
        Парсит PDF или Excel лист подбора и сразу отправляет данные в фиктивную отгрузку.
        
        Args:
            content: Содержимое PDF/Excel файла
            filename: Имя файла
            account: Аккаунт WB для всех поставок в файле
            user: Данные пользователя
            
        Returns:
            Dict: Результат операции фиктивной отгрузки
            
        Raises:
            HTTPException: В случае ошибки обработки
        """
        try:
            logger.info(f"Пользователь {user.get('username', 'unknown')} запустил парсинг с отгрузкой: {filename}")

            # 1. Определяем тип файла и парсим соответствующим парсером
            file_type = self._detect_file_type(filename)
            
            if file_type == 'pdf':
                result = self.pdf_parser.parse_pdf_to_json(content, source_filename=filename)
                file_type_name = "PDF"
            else:  # excel
                result = self.excel_parser.parse_excel_to_json(content, source_filename=filename)
                file_type_name = "Excel"

            if not result['orders']:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"В {file_type_name} не найдено заказов для обработки"
                )

            # 2. Создаем маппинг аккаунтов (используем один аккаунт для всех поставок)
            supply_account_map = {}
            for order in result['orders']:
                if supply_id := order.get('supply_id'):
                    supply_account_map[supply_id] = account
            
            # 3. Проверяем что поставки относятся к указанному кабинету
            supplies_service = SuppliesService(self.db)
            await self._validate_supplies_belong_to_account(supply_account_map, supplies_service)

            # 4. Преобразуем данные в формат для фиктивной отгрузки
            selected_orders, supplies = self.convert_orders_to_fictitious_format(
                result['orders'], 
                supply_account_map
            )

            if not selected_orders:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Не удалось подготовить заказы для отгрузки (отсутствует supply_id)"
                )

            # 5. Отправляем в фиктивную отгрузку
            shipment_success = await supplies_service._send_shipment_data_to_external_systems(
                selected_orders=selected_orders,
                supplies=supplies,
                operator=user.get('username', 'unknown')
            )

            # 6. Формируем ответ
            response_data = {
                "success": shipment_success,
                "message": "Отгрузка выполнена успешно" if shipment_success else "Ошибка при выполнении отгрузки",
                "processed_orders": len(selected_orders),
                "processed_supplies": len(supplies),
                "supplies_info": supplies,
                "file_metadata": {
                    "source_file": filename,
                    "file_type": file_type_name,
                    "total_orders_parsed": len(result['orders']),
                    "parsing_success": result['statistics']['parsing_success']
                }
            }

            logger.info(f"Парсинг с отгрузкой завершен: {len(selected_orders)} заказов, успех={shipment_success}")

            return response_data

        except (PDFParseError, ExcelParseError) as e:
            logger.error(f"Ошибка парсинга файла {filename}: {e}")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Ошибка парсинга файла: {str(e)}"
            )
        except HTTPException:
            # Перепроброс уже созданных HTTPException
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при парсинге с отгрузкой {filename}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Произошла ошибка: {str(e)}"
            )

