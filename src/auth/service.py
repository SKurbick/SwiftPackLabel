import bcrypt
import time
from datetime import datetime, timedelta, timezone
from jose import jwt
from typing import Dict, Optional, List, Any
from src.db import db
from src.diagnostics import diagnostics, get_request_id, mask_username
from src.settings import settings

SECRET_KEY = settings.SECRET_KEY
ALGORITHM = settings.ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES


class AuthService:
    """Service for user authentication and management"""

    async def create_user(self, username: str, password: str, email: str, is_superuser: bool = False) -> Dict:
        """Register a new user"""
        hashed_password = self._hash_password(password)
        user = await db.fetchrow("SELECT * FROM users WHERE username = $1", username)

        if user:
            raise ValueError("Username already exists")

        user_id = await db.fetchrow(
            """
            INSERT INTO users (username, hashed_password, email, is_superuser) 
            VALUES ($1, $2, $3, $4) 
            RETURNING id
            """,
            username, hashed_password, email, is_superuser
        )
        user_id = user_id["id"]
        user = await db.fetchrow("SELECT id, username, email, full_name, is_superuser FROM users WHERE id = $1",
                                 user_id)

        return dict(user)

    async def authenticate_user(self, username: str, password: str) -> Optional[Dict]:
        """Authenticate user by username and password"""
        request_id = get_request_id()
        diagnostics.record_event("AUTH_DB_FETCH_START", request_id=request_id, username_masked=mask_username(username))
        db_started = time.monotonic()
        user = await db.fetchrow("SELECT * FROM users WHERE username = $1", username)
        diagnostics.record_event(
            "AUTH_DB_FETCH_END",
            request_id=request_id,
            duration_ms=round((time.monotonic() - db_started) * 1000, 3),
            found=bool(user),
        )

        if not user:
            return None

        diagnostics.record_event("AUTH_BCRYPT_START", request_id=request_id)
        bcrypt_started = time.monotonic()
        password_ok = self._verify_password(password, user["hashed_password"])
        diagnostics.record_event(
            "AUTH_BCRYPT_END",
            request_id=request_id,
            duration_ms=round((time.monotonic() - bcrypt_started) * 1000, 3),
            result=password_ok,
        )
        if not password_ok:
            return None

        return {
            "id": user["id"],
            "username": user["username"],
            "email": user["email"],
            "full_name": user["full_name"],
            "is_superuser": user["is_superuser"]
        }

    # async def update_user(self, user_id: int, update_data: Dict) -> Dict:
    #     """Update user data"""
    #     data = {k: v for k, v in update_data.items() if v is not None}
    #
    #     if not data:
    #         return await self.get_user_by_id(user_id)
    #
    #     set_clause = ", ".join(f"{key} = ${i + 2}" for i, key in enumerate(data))
    #     query = f"UPDATE users SET {set_clause} WHERE id = $1 RETURNING id, username, email, full_name"
    #
    #     user = await db.fetchrow(query, user_id, *data.values())
    #
    #     return dict(user)

    async def get_user_by_id(self, user_id: int) -> Dict:
        """Get user by ID"""
        user = await db.fetchrow("SELECT id, username, email, full_name, is_superuser FROM users WHERE id = $1",
                                 user_id)
        return dict(user) if user else None

    async def get_all_users(self) -> List[Dict]:
        """Get all users"""
        users = await db.fetch("SELECT id, username, email, full_name, is_superuser FROM users ORDER BY id")
        return [dict(user) for user in users]

    async def delete_user_by_username(self, id_user: int) -> bool:
        """Delete user by username"""
        result = await db.execute("DELETE FROM users WHERE id = $1", id_user)
        # Check if any rows were affected
        return result and "DELETE" in result

    def _hash_password(self, password: str) -> str:
        """Hash a password using bcrypt"""
        if isinstance(password, str):
            password = password.encode('utf-8')

        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password, salt)

        return hashed.decode('utf-8')

    def _verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against a hash using bcrypt"""
        if isinstance(plain_password, str):
            plain_password = plain_password.encode('utf-8')
        if isinstance(hashed_password, str):
            hashed_password = hashed_password.encode('utf-8')

        return bcrypt.checkpw(plain_password, hashed_password)

    async def create_access_token(self, data: Dict[str, Any]) -> str:
        """Create JWT access token"""
        to_encode = data.copy()
        expire = datetime.now(timezone.utc) + timedelta(
            minutes=ACCESS_TOKEN_EXPIRE_MINUTES
        )
        to_encode["exp"] = expire
        return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    async def verify_token(self, token: str) -> Optional[Dict]:
        """Verify JWT token and return user data"""
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            username = payload.get("sub")
            if username is None:
                return None
        except jwt.JWTError:
            return None

        user = await db.fetchrow("SELECT id, username, email, full_name, is_superuser FROM users WHERE username = $1",
                                 username)
        return None if user is None else dict(user)
