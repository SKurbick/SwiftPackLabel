from fastapi import APIRouter
from src.supplies.router import supply
from src.archives.router import archive
from src.auth.router import auth
from src.excel_data.router import excel_data

router = APIRouter(prefix='/api/v1')
router.include_router(supply)
router.include_router(archive)
router.include_router(auth)
router.include_router(excel_data)
