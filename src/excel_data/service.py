import json
import os
from typing import List, Dict, Tuple, Optional
import pandas as pd
from io import BytesIO
from fastapi import HTTPException, status

from src.excel_data.schema import WildModelPair
from src.logger import app_logger as logger


class ExcelDataService:
    """Сервис для работы с Excel-данными."""

    def __init__(self, storage_path: str = "src/excel_data/data.json"):
        """
        Инициализирует сервис для работы с Excel-данными.
        
        Args:
            storage_path: Путь к файлу для хранения данных в формате JSON
        """
        self.storage_path = storage_path
        self._ensure_storage_exists()

    def _ensure_storage_exists(self) -> None:
        """Убеждается, что директория для хранения существует."""
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        
        # Создаем пустой JSON-файл, если он не существует
        if not os.path.exists(self.storage_path):
            with open(self.storage_path, 'w', encoding='utf-8') as f:
                json.dump({"data": []}, f, ensure_ascii=False, indent=2)

    def _read_data(self) -> List[Dict[str, str]]:
        """Читает данные из JSON-файла."""
        try:
            with open(self.storage_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("data", [])
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Ошибка при чтении данных: {e}")
            return []

    def _write_data(self, data: List[Dict[str, str]]) -> None:
        """Записывает данные в JSON-файл."""
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
            required_columns = ['wild', 'модель']
            if any(col not in df.columns for col in required_columns):
                return False, "Excel-файл должен содержать столбцы 'wild' и 'модель'", []
            if len(df.columns) != 2:
                return False, "Excel-файл должен содержать только два столбца: 'wild' и 'модель'", []
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

    def get_model_by_wild(self, wild_value: str) -> Optional[WildModelPair]:
        """
        Возвращает модель по значению wild.
        
        Args:
            wild_value: Значение wild для поиска
            
        Returns:
            Optional[WildModelPair]: Найденная пара wild-модель или None, если не найдена
        """
        data = self._read_data()
        for item in data:
            if item.get("wild") == wild_value:
                return WildModelPair(**item)
        return None

    def get_all_data(self) -> List[WildModelPair]:
        """
        Возвращает все пары wild-модель.
        
        Returns:
            List[WildModelPair]: Список всех пар wild-модель
        """
        data = self._read_data()
        return [WildModelPair(**item) for item in data]

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
