from fastapi import APIRouter
from src.supplies.router import supply
from src.archives.router import archive

router = APIRouter(prefix='/api/v1')
router.include_router(supply)
router.include_router(archive)
