import time
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from typing import Dict, List

from src.auth.schema import UserCreate, UserUpdate, UserResponse, TokenResponse
from src.auth.service import AuthService
from src.auth.dependencies import get_current_user, get_current_superuser
from src.diagnostics import diagnostics, get_request_id, mask_username, safe_error

auth = APIRouter(tags=["Auth"],prefix='/auth')

@auth.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register_user(
    user_data: UserCreate,
    current_user: Dict = Depends(get_current_superuser),
    auth_service: AuthService = Depends()
):
    """Register a new user (only available to superusers)"""
    try:
        return await auth_service.create_user(
            username=user_data.username,
            password=user_data.password,
            email=user_data.email,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@auth.post("/login", response_model=TokenResponse)
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    auth_service: AuthService = Depends()
):
    """Authenticate user and return access token"""
    total_started = time.monotonic()
    request_id = get_request_id()
    username_masked = mask_username(form_data.username)
    diagnostics.record_event("LOGIN_START", request_id=request_id, username_masked=username_masked)
    try:
        diagnostics.record_event("LOGIN_BEFORE_AUTHENTICATE", request_id=request_id)
        auth_started = time.monotonic()
        user = await auth_service.authenticate_user(
            username=form_data.username,
            password=form_data.password
        )
        auth_duration_ms = round((time.monotonic() - auth_started) * 1000, 3)
        diagnostics.record_event(
            "LOGIN_AFTER_AUTHENTICATE",
            request_id=request_id,
            duration_ms=auth_duration_ms,
            result="success" if user else "fail",
        )

        if not user:
            diagnostics.record_event(
                "LOGIN_FAILED",
                level="warning",
                request_id=request_id,
                reason="invalid_credentials",
                total_duration_ms=round((time.monotonic() - total_started) * 1000, 3),
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        diagnostics.record_event("LOGIN_BEFORE_CREATE_TOKEN", request_id=request_id)
        token_started = time.monotonic()
        token = await auth_service.create_access_token(data={"sub": user["username"]})
        diagnostics.record_event(
            "LOGIN_AFTER_CREATE_TOKEN",
            request_id=request_id,
            duration_ms=round((time.monotonic() - token_started) * 1000, 3),
        )

        diagnostics.record_event(
            "LOGIN_SUCCESS",
            request_id=request_id,
            total_duration_ms=round((time.monotonic() - total_started) * 1000, 3),
        )
        return {
            "access_token": token,
            "token_type": "bearer",
            "user_id": user["id"],
            "username": user["username"],
            "is_superuser": user["is_superuser"]
        }
    except HTTPException:
        raise
    except Exception as exc:
        diagnostics.record_event(
            "LOGIN_FAILED",
            level="error",
            request_id=request_id,
            reason=safe_error(exc),
            total_duration_ms=round((time.monotonic() - total_started) * 1000, 3),
        )
        raise

@auth.get("/me", response_model=UserResponse,status_code=status.HTTP_200_OK)
async def get_current_user_info(
    current_user: Dict = Depends(get_current_user)
):
    """Get current user information"""
    return current_user

# @auth.patch("/update_user", response_model=UserResponse,status_code=status.HTTP_200_OK)
# async def update_current_user(
#     user_update: UserUpdate,
#     current_user: Dict = Depends(get_current_user),
#     auth_service: AuthService = Depends()
# ):
#     """Update current user information"""
#     return await auth_service.update_user(
#         user_id=current_user["id"],
#         update_data=user_update.model_dump(exclude_unset=True),
#     )


@auth.get("/users", response_model=List[UserResponse], status_code=status.HTTP_200_OK)
async def get_all_users(
    current_user: Dict = Depends(get_current_superuser),
    auth_service: AuthService = Depends()
):
    """Get all users (only available to superusers)"""
    return await auth_service.get_all_users()


@auth.delete("/users/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    id_user: int,
    current_user: Dict = Depends(get_current_superuser),
    auth_service: AuthService = Depends()
):
    """Delete user by username (only available to superusers)"""
    if id_user == current_user["id"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account"
        )
        
    deleted = await auth_service.delete_user_by_username(id_user)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"User with username {id_user} not found"
        )
    return None
