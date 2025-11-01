from fastapi import APIRouter
from src.supplies.router import supply
from src.archives.router import archive
from src.auth.router import auth
from src.excel_data.router import excel_data
from src.orders.router import orders
from src.wild_logs.router import wild_logs
from src.qr_parser.router import qr_parser
from src.cards.router import cards
from src.images.router import images
from src.pdf_parser.router import document_parser_router
from src.cache.router import cache
from src.canceled_orders.router import canceled_orders

router = APIRouter(prefix='/api/v1')
router.include_router(supply)
router.include_router(archive)
router.include_router(auth)
router.include_router(excel_data)
router.include_router(orders)
router.include_router(wild_logs)
router.include_router(qr_parser)
router.include_router(cards)
router.include_router(images)
router.include_router(document_parser_router)
router.include_router(cache)
router.include_router(canceled_orders)