
import json
import os
from decimal import Decimal
from typing import Any, Dict

import redis.asyncio as redis

redis_client = redis.Redis.from_url(
    os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    decode_responses=True,
)


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


async def get_revenue_summary(
    property_id: str,
    tenant_id: str,
) -> Dict[str, Any]:
    if not tenant_id:
        raise ValueError("tenant_id is required")

    # Keep each tenant's cached revenue separate.
    cache_key = f"revenue:{tenant_id}:{property_id}"

    cached = await redis_client.get(cache_key)
    if cached is not None:
        return json.loads(cached)

    from app.services.reservations import calculate_total_revenue

    result = await calculate_total_revenue(property_id, tenant_id)

    await redis_client.setex(
        cache_key,
        300,
        json.dumps(result, default=_json_default),
    )

    return result
