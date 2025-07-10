"""
Модуль содержит все Celery задачи для SwiftPackLabel.

Структура задач:
- orders_sync.py - задачи для синхронизации заказов Wildberries
"""

# Импортируем все задачи для регистрации в Celery
from .orders_sync import sync_orders_periodic

__all__ = ['sync_orders_periodic']