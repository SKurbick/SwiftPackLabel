import json
import os
from typing import List, Dict, Tuple, Optional
import pandas as pd
from io import BytesIO
from fastapi import HTTPException, status
from pathlib import Path
from threading import Lock

from src.excel_data.schema import (
    WildModelPair, WildModelRecord, WildModelCreate, WildModelUpdate, 
    WildModelListResponse, WildModelResponse
)
from src.logger import app_logger as logger


class ExcelDataService:
    """Минимальный сервис для CRUD операций с Excel данными."""

    def __init__(self, storage_path: str = None):
        """
        Инициализирует сервис для работы с Excel-данными.
        Args:
            storage_path: Путь к файлу для хранения данных в формате JSON
        """
        if storage_path is None:
            project_root = Path(__file__).parent.parent.parent
            self.storage_path = os.path.join(project_root, "src", "excel_data", "data", "data.json")
        else:
            self.storage_path = storage_path
        
        self._file_lock = Lock()
        self._ensure_storage_exists()

    def _ensure_storage_exists(self) -> None:
        """Убеждается, что файл существует."""
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        if not os.path.exists(self.storage_path):
            with open(self.storage_path, 'w', encoding='utf-8') as f:
                json.dump({"data": []}, f, ensure_ascii=False, indent=2)

    def _read_data(self) -> List[Dict[str, str]]:
        """Читает данные из JSON-файла."""
        try:
            with self._file_lock:
                with open(self.storage_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get("data", [])
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Ошибка при чтении данных: {e}")
            return []

    def _write_data(self, data: List[Dict[str, str]]) -> None:
        """Записывает данные в JSON-файл."""
        with self._file_lock:
            with open(self.storage_path, 'w', encoding='utf-8') as f:
                json.dump({"data": data}, f, ensure_ascii=False, indent=2)

    def validate_excel(self, file_content: bytes) -> Tuple[bool, Optional[str], List[Dict[str, str]]]:
        """
        Валидирует Excel-файл и возвращает данные, если они корректны.
        
        Args:
            file_content: Содержимое Excel-файла в виде байтов
            
        Returns:
            Tuple[bool, Optional[str], List[Dict[str, str]]]: (успешна ли валидация, 
                                                            сообщение об ошибке если нет,
                                                            данные из Excel-файла)
        """
        try:
            df = pd.read_excel(BytesIO(file_content))
            required_columns = ['Вилд', 'Модель']
            if any(col not in df.columns for col in required_columns):
                return False, "Excel-файл должен содержать столбцы 'Вилд' и 'модель'", []
            if len(df.columns) != 2:
                return False, "Excel-файл должен содержать только два столбца: 'Вилд' и 'модель'", []
            for col in required_columns:
                df[col] = df[col].fillna('').astype(str)
                if df[col].str.strip().eq('').any():
                    return False, f"Столбец '{col}' содержит пустые значения", []
            data = df[required_columns].to_dict('records')
            return True, None, data

        except Exception as e:
            logger.error(f"Ошибка при валидации Excel-файла: {e}")
            return False, f"Ошибка при обработке Excel-файла: {str(e)}", []

    def upload_excel(self, file_content: bytes) -> None:
        """
        Загружает и валидирует Excel-файл, затем сохраняет данные.
        
        Args:
            file_content: Содержимое Excel-файла в виде байтов
            
        Raises:
            HTTPException: Если файл не проходит валидацию
        """
        valid, error_message, data = self.validate_excel(file_content)
        
        if not valid:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=error_message
            )

        self._write_data(data)
        logger.info(f"Загружены данные из Excel: {len(data)} записей")

    def get_all_records(self) -> WildModelListResponse:
        """
        Возвращает все записи с индексами.
        
        Returns:
            WildModelListResponse: Список всех записей
        """
        data_list = self._read_data()
        
        records = [
            WildModelRecord(
                index=i,
                wild=item.get("Вилд", ""),
                model=item.get("Модель", "")
            )
            for i, item in enumerate(data_list)
        ]
        
        return WildModelListResponse(
            data=records,
            total=len(records)
        )

    def update_record(self, index: int, record: WildModelUpdate) -> WildModelRecord:
        """
        Обновляет запись по индексу.
        
        Args:
            index: Индекс записи для обновления
            record: Новые данные (wild и model)
            
        Returns:
            WildModelRecord: Обновленная запись
            
        Raises:
            HTTPException: Если индекс не найден
        """
        data_list = self._read_data()
        
        if not (0 <= index < len(data_list)):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Запись с индексом {index} не найдена"
            )
        
        # Обновляем запись
        data_list[index] = {
            "Вилд": record.wild,
            "Модель": record.model
        }
        
        self._write_data(data_list)
        logger.info(f"Обновлена запись index={index}: wild='{record.wild}', model='{record.model}'")
        
        return WildModelRecord(
            index=index,
            wild=record.wild,
            model=record.model
        )

    def create_record(self, record: WildModelCreate) -> WildModelRecord:
        """
        Добавляет новую запись в конец файла.
        
        Args:
            record: Данные для создания (wild и model)
            
        Returns:
            WildModelRecord: Созданная запись с индексом
        """
        data_list = self._read_data()
        
        # Добавляем новую запись в конец
        new_item = {
            "Вилд": record.wild,
            "Модель": record.model
        }
        data_list.append(new_item)
        
        self._write_data(data_list)
        
        new_index = len(data_list) - 1
        logger.info(f"Создана запись index={new_index}: wild='{record.wild}', model='{record.model}'")
        
        return WildModelRecord(
            index=new_index,
            wild=record.wild,
            model=record.model
        )

    def delete_record(self, index: int) -> bool:
        """
        Удаляет запись по индексу.
        
        Args:
            index: Индекс записи для удаления
            
        Returns:
            bool: True если запись удалена, False если не найдена
        """
        data_list = self._read_data()
        
        if not (0 <= index < len(data_list)):
            return False
        
        # Удаляем запись
        removed_item = data_list.pop(index)
        self._write_data(data_list)
        
        logger.info(f"Удалена запись index={index}: wild='{removed_item.get('Вилд')}'")
        return True

    # Оставляем существующие методы для совместимости
    def get_model_by_wild(self, wild_value: str) -> Optional[WildModelResponse]:
        """Возвращает модель по wild коду (для совместимости)."""
        data_list = self._read_data()
        for item in data_list:
            if item.get("Вилд") == wild_value:
                return WildModelResponse(
                    wild=item.get("Вилд", ""),
                    model=item.get("Модель", "")
                )
        return None

    def get_all_data(self) -> List[WildModelPair]:
        """Возвращает все данные (для совместимости)."""
        data_list = self._read_data()
        return [WildModelPair(**item) for item in data_list]

    def download_excel(self) -> BytesIO:
        """
        Создает Excel-файл с текущими данными.
        
        Returns:
            BytesIO: Excel-файл в виде байтового потока
        """
        data = self._read_data()
        df = pd.DataFrame(data)
        
        # Создаем Excel-файл в памяти
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='WildModel')
        
        # Перемещаем указатель в начало буфера для последующего чтения
        output.seek(0)
        return output
