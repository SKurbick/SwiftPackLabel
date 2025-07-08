"""
Celery приложение для фоновых задач SwiftPackLabel.
"""

from .celery import celery_app

__all__ = ["celery_app"]