
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from sqlalchemy import text

from app.core.database_pool import db_pool


async def calculate_monthly_revenue(
    property_id: str,
    month: int,
    year: int,
    db_session=None,
    tenant_id: Optional[str] = None,
) -> Decimal:
    """Calculate revenue for a property during a specific month."""

    if not 1 <= month <= 12:
        raise ValueError("month must be between 1 and 12")

    if not 1 <= year < 9999:
        raise ValueError("year must be between 1 and 9999")

    start_date = datetime(year, month, 1)

    if month == 12:
        end_date = datetime(year + 1, 1, 1)
    else:
        end_date = datetime(year, month + 1, 1)

    query = """
        SELECT COALESCE(SUM(total_amount), 0) AS total
        FROM reservations
        WHERE property_id = :property_id
          AND check_in_date >= :start_date
          AND check_in_date < :end_date
    """

    params = {
        "property_id": property_id,
        "start_date": start_date,
        "end_date": end_date,
    }

    # Apply tenant filtering whenever tenant_id is supplied.
    if tenant_id is not None:
        query += " AND tenant_id = :tenant_id"
        params["tenant_id"] = tenant_id

    if db_session is not None:
        result = await db_session.execute(text(query), params)
        total = result.scalar_one()
    else:
        if db_pool.session_factory is None:
            await db_pool.initialize()

        async with db_pool.get_session() as session:
            result = await session.execute(text(query), params)
            total = result.scalar_one()

    return Decimal(str(total or 0))


async def calculate_total_revenue(
    property_id: str,
    tenant_id: str,
) -> Dict[str, Any]:
    """Calculate total revenue and reservation count for a tenant."""

    if db_pool.session_factory is None:
        await db_pool.initialize()

    query = text("""
        SELECT
            COALESCE(SUM(total_amount), 0) AS total_revenue,
            COUNT(*) AS reservation_count
        FROM reservations
        WHERE property_id = :property_id
          AND tenant_id = :tenant_id
    """)

    async with db_pool.get_session() as session:
        result = await session.execute(
            query,
            {
                "property_id": property_id,
                "tenant_id": tenant_id,
            },
        )

        row = result.one()
        total_revenue = Decimal(str(row.total_revenue or 0))

        return {
            "property_id": property_id,
            "tenant_id": tenant_id,
            "total": f"{total_revenue:.2f}",
            "currency": "USD",
            "count": row.reservation_count,
        }
