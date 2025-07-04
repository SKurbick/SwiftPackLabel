from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from fastapi.responses import Response
from pathlib import Path
import urllib.parse

from src.auth.dependencies import get_current_user,get_current_superuser
from src.images.service import ImageService
from src.images.schema import ImageUploadResponse, ImageInfoResponse, ImageDeleteResponse, ImageListResponse
from src.logger import app_logger as logger

images = APIRouter(prefix='/images', tags=['Images'])


@images.post("/upload", response_model=ImageUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_image(
    file: UploadFile = File(..., description="Файл изображения для загрузки"),
    user: dict = Depends(get_current_user)
) -> ImageUploadResponse:
    """
    Загрузка изображения на сервер.
    Имя файла генерируется автоматически на основе текущей даты и времени.
    
    Args:
        file: Файл изображения
        user: Данные текущего пользователя
        
    Returns:
        ImageUploadResponse: Результат загрузки изображения
    """
    logger.info(f"Запрос на загрузку изображения от {user.get('username', 'unknown')}")
    
    try:
        if not file.content_type or not file.content_type.startswith('image/'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Файл должен быть изображением"
            )

        image_data = await file.read()
        
        if not image_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Файл пуст"
            )

        max_size = 10 * 1024 * 1024
        if len(image_data) > max_size:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail="Файл слишком большой. Максимальный размер: 10MB"
            )

        image_service = ImageService()
        saved_path = await image_service.save_image(image_data)
        
        if not saved_path:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Ошибка при сохранении изображения"
            )

        saved_filename = Path(saved_path).name
        
        logger.info(f"Изображение успешно загружено: {saved_filename}")
        
        return ImageUploadResponse(
            success=True,
            message="Изображение успешно загружено",
            filename=saved_filename,
            file_path=saved_path
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при загрузке изображения: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Произошла ошибка при загрузке изображения"
        )


@images.get("/{filename}", status_code=status.HTTP_200_OK)
async def get_image(
    filename: str,
    user: dict = Depends(get_current_superuser)
) -> Response:
    """
    Получение изображения по имени файла.
    
    Args:
        filename: Имя файла изображения
        user: Данные текущего пользователя
        
    Returns:
        Response: Изображение в бинарном формате
    """
    logger.info(f"Запрос на получение изображения {filename} от {user.get('username', 'unknown')}")
    
    try:
        image_service = ImageService()
        image_data = await image_service.get_image(filename)
        
        if not image_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Изображение не найдено"
            )
        
        # Определение MIME типа по расширению файла
        file_path = Path(filename)
        file_extension = file_path.suffix.lower()
        mime_type_map = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.webp': 'image/webp',
            '.bmp': 'image/bmp'
        }
        
        mime_type = mime_type_map.get(file_extension, 'application/octet-stream')
        
        # Кодирование имени файла для HTTP заголовка (только ASCII символы)
        try:
            # Попробуем использовать оригинальное имя, если оно содержит только ASCII
            ascii_filename = filename.encode('ascii').decode('ascii')
            content_disposition = f'inline; filename="{ascii_filename}"'
        except UnicodeEncodeError:
            # Если есть не-ASCII символы, используем только закодированную версию
            encoded_filename = urllib.parse.quote(filename, safe='')
            content_disposition = f'inline; filename*=UTF-8\'\'{encoded_filename}'
        
        return Response(
            content=image_data,
            media_type=mime_type,
            headers={
                'Content-Disposition': content_disposition
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении изображения: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Произошла ошибка при получении изображения"
        )


@images.get("/{filename}/info", response_model=ImageInfoResponse, status_code=status.HTTP_200_OK)
async def get_image_info(
    filename: str,
    user: dict = Depends(get_current_superuser)
) -> ImageInfoResponse:
    """
    Получение информации об изображении.
    
    Args:
        filename: Имя файла изображения
        user: Данные текущего пользователя
        
    Returns:
        ImageInfoResponse: Информация об изображении
    """
    logger.info(f"Запрос на получение информации об изображении {filename} от {user.get('username', 'unknown')}")
    
    try:
        image_service = ImageService()
        image_info = await image_service.get_image_info(filename)
        
        if not image_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Изображение не найдено"
            )
        
        return ImageInfoResponse(**image_info)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении информации об изображении: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Произошла ошибка при получении информации об изображении"
        )


@images.delete("/{filename}", response_model=ImageDeleteResponse, status_code=status.HTTP_200_OK)
async def delete_image(
    filename: str,
    user: dict = Depends(get_current_superuser)
) -> ImageDeleteResponse:
    """
    Удаление изображения.
    
    Args:
        filename: Имя файла изображения
        user: Данные текущего пользователя
        
    Returns:
        ImageDeleteResponse: Результат удаления изображения
    """
    logger.info(f"Запрос на удаление изображения {filename} от {user.get('username', 'unknown')}")
    
    try:
        image_service = ImageService()
        deleted = await image_service.delete_image(filename)
        
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Изображение не найдено"
            )
        
        return ImageDeleteResponse(
            success=True,
            message="Изображение успешно удалено",
            filename=filename
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при удалении изображения: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Произошла ошибка при удалении изображения"
        )


@images.get("/", response_model=ImageListResponse, status_code=status.HTTP_200_OK)
async def list_images(
    user: dict = Depends(get_current_superuser)
) -> ImageListResponse:
    """
    Получение списка всех изображений.
    
    Args:
        user: Данные текущего пользователя
        
    Returns:
        ImageListResponse: Список названий файлов изображений
    """
    logger.info(f"Запрос на получение списка изображений от {user.get('username', 'unknown')}")
    
    try:
        image_service = ImageService()
        filenames = await image_service.list_images()
        
        return ImageListResponse(
            total=len(filenames),
            filenames=filenames
        )
        
    except Exception as e:
        logger.error(f"Ошибка при получении списка изображений: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Произошла ошибка при получении списка изображений"
        )