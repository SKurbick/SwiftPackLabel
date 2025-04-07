from fastapi import APIRouter
from src.supplies.router import supply
from src.archives.router import archive
from src.auth.router import auth

router = APIRouter(prefix='/api/v1')
router.include_router(supply)
router.include_router(archive)
router.include_router(auth)
