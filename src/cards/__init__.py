"""
Модуль для работы с карточками товаров Wildberries.
"""

from .cards import CardsService
from .schema import DimensionsUpdateRequest
from .router import cards

__all__ = [
    'CardsService',
    'DimensionsUpdateRequest', 
    'cards'
]
