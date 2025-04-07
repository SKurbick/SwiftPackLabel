from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from typing import Dict, List

from src.auth.schema import UserCreate, UserUpdate, UserResponse, TokenResponse
from src.auth.service import AuthService
from src.auth.dependencies import get_current_user, get_current_superuser

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
    user = await auth_service.authenticate_user(
        username=form_data.username,
        password=form_data.password
    )
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    token = await auth_service.create_access_token(data={"sub": user["username"]})
    
    return {
        "access_token": token,
        "token_type": "bearer",
        "user_id": user["id"],
        "username": user["username"],
        "is_superuser": user["is_superuser"]
    }

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