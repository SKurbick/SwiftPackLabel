from fastapi import APIRouter
from src.supplies.router import supply

router = APIRouter(prefix='/api/v1')
router.include_router(supply)
