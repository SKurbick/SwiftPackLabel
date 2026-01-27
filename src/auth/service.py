from typing import Literal

import jwt

from src.settings import settings


class JWTService:
    """Обьект для JWT токенов с методами реализации"""

    def __init__(self):
        self._access_key = settings.SECRET_KEY
        self._alg = settings.ALGORITHM

    def decode_token(
            self,
            token: str,
            token_type: Literal["ACCESS"] = "ACCESS"
    ) -> dict | None:
        """Декодировка токена"""
        match token_type:
            case "ACCESS":
                return jwt.decode(token, self._access_key, algorithms=self._alg)
            case _:
                return None
