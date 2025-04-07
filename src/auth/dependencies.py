import base64
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials, OAuth2PasswordBearer
from typing import Dict, Optional

from src.auth.service import AuthService


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login", auto_error=False)


async def get_current_user(
        token: str = Depends(oauth2_scheme),
        auth_service: AuthService = Depends()
):
    """Dependency to authenticate user from either JWT token or HTTP Basic Auth credentials"""
    if token:
        user = await auth_service.verify_token(token)
        if user:
            return user

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid authentication credentials or token",
        headers={"WWW-Authenticate": "Bearer"},
    )


async def get_current_superuser(
        current_user: Dict = Depends(get_current_user)
):
    """Dependency to check if the current user is a superuser"""
    if not current_user.get("is_superuser"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions. Only superusers can perform this action",
        )
    return current_user