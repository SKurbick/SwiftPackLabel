from celery import Celery
from src.settings import settings
from src.logger import app_logger as logger


def create_celery_app() -> Celery:
    """
    Создание и настройка Celery приложения.
    
    Returns:
        Celery: Настроенное Celery приложение
    """
    
    # Создаем Celery приложение
    celery_app = Celery(
        "swiftpacklabel",
        broker=settings.CELERY_BROKER_URL,
        backend=settings.CELERY_RESULT_BACKEND,
    )
    
    # Обновляем конфигурацию
    celery_app.conf.update(
        # Часовой пояс
        timezone=settings.CELERY_TIMEZONE,
        enable_utc=True,
        
        # Результаты выполнения задач
        result_expires=settings.CELERY_RESULT_EXPIRES,
        
        # Сериализация
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        
        # Настройки воркера
        worker_prefetch_multiplier=settings.CELERY_WORKER_PREFETCH_MULTIPLIER,
        worker_max_tasks_per_child=settings.CELERY_WORKER_MAX_TASKS_PER_CHILD,
        
        # Настройки задач
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        task_default_retry_delay=60,
        task_max_retries=3,
        
        # Мониторинг
        worker_send_task_events=True,
        task_send_sent_event=True,
        
        # Периодические задачи
        beat_schedule={
            'sync-orders-every-10-minutes': {
                'task': 'sync_orders_periodic',
                'schedule': 600.0,  # каждые 10 минут (600 секунд)
            },
        },
    )
    
    logger.info("Celery приложение создано и настроено")
    return celery_app


# Создаем единственный экземпляр Celery приложения
celery_app = create_celery_app()


# Автоматическое обнаружение задач
celery_app.autodiscover_tasks([
    'src.celery_app.tasks',
])


@celery_app.task(bind=True)
def debug_task(self):
    """Отладочная задача для проверки работы Celery."""
    logger.info(f'Request: {self.request!r}')
    return "Debug task completed successfully"