"""
Модуль глобального кэширования для SwiftPackLabel.

Простая система глобального кэширования с автоматическим фоновым обновлением каждые 5 минут.
"""

from .global_cache import (
    global_cache,
    global_cached,
    invalidate_cache,
    clear_function_cache
)

# Для обратной совместимости
hybrid_cached = global_cached
hybrid_cache = global_cache

__all__ = [
    "global_cache",
    "global_cached", 
    "hybrid_cached",  # Обратная совместимость
    "hybrid_cache",   # Обратная совместимость
    "invalidate_cache",
    "clear_function_cache"
]