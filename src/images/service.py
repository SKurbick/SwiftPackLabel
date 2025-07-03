import os
import uuid
from pathlib import Path
from typing import Optional
from PIL import Image
import io
from datetime import datetime

from src.logger import app_logger as logger


class ImageService:
    """
    Сервис для работы с изображениями.
    Предоставляет методы для сохранения и получения изображений.
    """
    
    def __init__(self, base_path: str = "src/images/uploads"):
        """
        Инициализация сервиса изображений.
        
        Args:
            base_path: Базовый путь для сохранения изображений
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Поддерживаемые расширения файлов
        self.supported_extensions = {'.jpg', '.jpeg', '.png', '.webp', '.bmp'}
    
    def _validate_and_get_format(self, image_data: bytes) -> Optional[str]:
        """
        Валидация изображения и определение формата.
        
        Args:
            image_data: Бинарные данные изображения
            
        Returns:
            Optional[str]: Расширение файла или None если изображение невалидно или формат не поддерживается
        """
        try:
            image = Image.open(io.BytesIO(image_data))
            image.verify()
            
            # Повторно открываем изображение для получения формата (verify() закрывает его)
            image = Image.open(io.BytesIO(image_data))
            format_name = image.format.lower()
            
            if format_name == 'jpeg':
                return '.jpg'
            elif format_name in ['png', 'webp', 'bmp']:
                return f'.{format_name}'
            else:
                logger.error(f"Неподдерживаемый формат изображения: {format_name}")
                return None
        except Exception as e:
            logger.error(f"Ошибка валидации/определения формата изображения: {str(e)}")
            return None
    
    async def save_image(self, image_data: bytes, filename: Optional[str] = None) -> Optional[str]:
        """
        Сохранение изображения на сервере.
        
        Args:
            image_data: Бинарные данные изображения
            filename: Игнорируется. Имя файла генерируется автоматически на основе даты и времени
            
        Returns:
            Optional[str]: Путь к сохраненному файлу или None в случае ошибки
        """
        try:
            # Валидация изображения и определение формата
            file_extension = self._validate_and_get_format(image_data)
            if not file_extension:
                return None
            
            # Генерация имени файла на основе даты и времени
            current_time = datetime.now()
            timestamp = current_time.strftime("%Y%m%d_%H%M%S_%f")[:-3]  # убираем последние 3 символа из микросекунд
            filename = f"img_{timestamp}{file_extension}"
            
            # Полный путь к файлу
            file_path = self.base_path / filename
            
            # Сохранение файла
            with open(file_path, 'wb') as f:
                f.write(image_data)
            
            logger.info(f"Изображение сохранено: {file_path}")
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении изображения: {str(e)}")
            return None
    
    async def get_image(self, filename: str) -> Optional[bytes]:
        """
        Получение изображения по имени файла.
        
        Args:
            filename: Имя файла изображения
            
        Returns:
            Optional[bytes]: Бинарные данные изображения или None если файл не найден
        """
        try:
            file_path = self.base_path / filename
            
            if not file_path.exists():
                logger.warning(f"Файл не найден: {file_path}")
                return None
            
            with open(file_path, 'rb') as f:
                image_data = f.read()
            
            logger.info(f"Изображение получено: {file_path}")
            return image_data
            
        except Exception as e:
            logger.error(f"Ошибка при получении изображения: {str(e)}")
            return None
    
    async def delete_image(self, filename: str) -> bool:
        """
        Удаление изображения.
        
        Args:
            filename: Имя файла изображения
            
        Returns:
            bool: True если файл успешно удален, False иначе
        """
        try:
            file_path = self.base_path / filename
            
            if not file_path.exists():
                logger.warning(f"Файл не найден для удаления: {file_path}")
                return False
            
            os.remove(file_path)
            logger.info(f"Изображение удалено: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при удалении изображения: {str(e)}")
            return False
    
    async def get_image_info(self, filename: str) -> Optional[dict]:
        """
        Получение информации об изображении.
        
        Args:
            filename: Имя файла изображения
            
        Returns:
            Optional[dict]: Информация об изображении или None если файл не найден
        """
        try:
            file_path = self.base_path / filename
            
            if not file_path.exists():
                logger.warning(f"Файл не найден: {file_path}")
                return None
            
            # Получение информации о файле
            stat = file_path.stat()
            
            # Получение размеров изображения
            with Image.open(file_path) as img:
                width, height = img.size
                format_name = img.format
            
            return {
                'filename': filename,
                'path': str(file_path),
                'size_bytes': stat.st_size,
                'width': width,
                'height': height,
                'format': format_name,
                'created_at': stat.st_ctime,
                'modified_at': stat.st_mtime
            }
            
        except Exception as e:
            logger.error(f"Ошибка при получении информации об изображении: {str(e)}")
            return None
    
    async def list_images(self) -> list[str]:
        """
        Получение списка всех изображений в директории.
        
        Returns:
            list[str]: Список названий файлов изображений
        """
        try:
            if not self.base_path.exists():
                logger.warning(f"Директория не найдена: {self.base_path}")
                return []
            
            # Получение всех файлов в директории
            image_files = []
            for file_path in self.base_path.iterdir():
                if file_path.is_file():
                    # Проверка расширения файла
                    file_extension = file_path.suffix.lower()
                    if file_extension in self.supported_extensions:
                        image_files.append(file_path.name)
            
            # Сортировка по времени создания (новые первыми)
            image_files.sort(key=lambda x: (self.base_path / x).stat().st_ctime, reverse=True)
            
            logger.info(f"Найдено {len(image_files)} изображений")
            return image_files
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка изображений: {str(e)}")
            return []