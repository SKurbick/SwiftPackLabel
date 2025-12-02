from datetime import date
from fastapi import HTTPException, status, Depends

from src.available_quantity.repository import AvailableQuantityRepository, get_available_quantity_repository
from src.available_quantity.schema import AvailableQuantity


class AvailableQuantityService:
    def __init__(
            self,
            repository: AvailableQuantityRepository
    ):
        self.repository = repository

    async def get_available_quantity(
            self,
            start_date: date | None,
            end_date: date | None,
            product_id: str | None
    ) -> list[AvailableQuantity]:

        if start_date and start_date > date.today():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Start date mustn't be greater than today's date!"
            )

        if start_date and end_date and start_date > end_date:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Start date must be less than end date!"
            )

        available_quantities = await self.repository.get_available_quantity(
            start_date=start_date,
            end_date=end_date,
            product_id=product_id
        )

        return [AvailableQuantity(
            product_id=row["product_id"],
            warehouse_id=row["warehouse_id"],
            available_quantity=row["available_quantity"]
        ) for row in available_quantities]


def get_available_quantity_service(
    repository: AvailableQuantityRepository = Depends(get_available_quantity_repository)
) -> AvailableQuantityService:
    return AvailableQuantityService(repository)