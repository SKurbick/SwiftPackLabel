from datetime import date
from fastapi import APIRouter, Depends, Query

from src.available_quantity.service import AvailableQuantityService, get_available_quantity_service
from src.available_quantity.schema import AvailableQuantity


available_quantity = APIRouter(prefix="/available_quantity", tags=["Available Quantity"])


@available_quantity.get("/", status_code=200)
async def get_available_quantity(
        service: AvailableQuantityService = Depends(get_available_quantity_service),
        start_date: date | None = Query(None),
        end_date: date | None = Query(None),
        product_id: str | None = Query(None)
) -> list[AvailableQuantity]:

    return await service.get_available_quantity(
        start_date=start_date,
        end_date=end_date,
        product_id=product_id
    )