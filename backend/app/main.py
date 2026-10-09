
import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.core.redis_client import redis_client
from app.core.database_pool import db_pool

from .api.v1 import (
    users_lightning,
    cities,
    city_access_fast,
    city_access_fixed,
    departments,
    profile,
    company_settings,
    auth_info,
    bootstrap,
    health,
    persistent_auth,
    dashboard,
    login,
)
from .monitoring.middleware import PerformanceMonitoringMiddleware


# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Redis cache invalidation listener
# -------------------------------------------------------------------

async def cache_invalidation_listener():
    """
    Listen for Redis Pub/Sub cache invalidation messages and invalidate
    the authentication cache for the specified user.
    """
    from .core.auth import invalidate_user_cache

    pubsub = None

    if not redis_client.is_connected:
        logger.info(
            "Redis is not connected; cache invalidation listener will not start"
        )
        return

    try:
        pubsub = await redis_client.subscribe("auth_cache_invalidate")

        if pubsub is None:
            logger.warning(
                "Failed to subscribe to auth_cache_invalidate channel"
            )
            return

        logger.info(
            "Cache invalidation listener started on auth_cache_invalidate"
        )

        async for message in pubsub.listen():
            try:
                if not message or message.get("type") != "message":
                    continue

                user_id = message.get("data")

                if isinstance(user_id, bytes):
                    user_id = user_id.decode("utf-8")

                if user_id:
                    invalidated_count = invalidate_user_cache(user_id)

                    logger.info(
                        "Cache invalidation received for user %s; "
                        "cleared %s entries in this worker",
                        user_id,
                        invalidated_count,
                    )

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception(
                    "Error processing cache invalidation message"
                )

    except asyncio.CancelledError:
        logger.info("Cache invalidation listener cancelled")
        raise

    except Exception:
        logger.exception("Cache invalidation listener error")

    finally:
        if pubsub is not None:
            try:
                await pubsub.unsubscribe("auth_cache_invalidate")
            except Exception:
                logger.warning(
                    "Failed to unsubscribe from Redis Pub/Sub",
                    exc_info=True,
                )

            try:
                close_result = pubsub.close()

                if asyncio.iscoroutine(close_result):
                    await close_result

            except Exception:
                logger.warning(
                    "Failed to close Redis Pub/Sub connection",
                    exc_info=True,
                )


# -------------------------------------------------------------------
# Application lifecycle
# -------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Initialize shared application resources on startup and release them
    safely during shutdown.
    """
    logger.info("Starting up...")

    listener_task = None
    async_processor = None

    # Initialize the existing Supabase connection pool.
    try:
        from .core.supabase_connection_pool import supabase_pool

        await supabase_pool.initialize()
        logger.info("Supabase connection pool initialized")

    except Exception:
        logger.exception(
            "Supabase connection pool initialization failed"
        )

    # Initialize the SQLAlchemy database pool.
    try:
        await db_pool.initialize()
        logger.info("SQLAlchemy database pool initialized")

    except Exception:
        logger.exception(
            "SQLAlchemy database pool initialization failed; "
            "SQLAlchemy database operations may be unavailable"
        )

    # Initialize Redis.
    try:
        await redis_client.initialize()

    except Exception:
        logger.exception(
            "Redis initialization failed; continuing without Redis"
        )

    # Start cache invalidation listener only when Redis is connected.
    if redis_client.is_connected:
        listener_task = asyncio.create_task(
            cache_invalidation_listener(),
            name="cache-invalidation-listener",
        )

        logger.info("Cache invalidation listener task created")

    else:
        logger.info(
            "Redis is unavailable; cross-worker cache invalidation "
            "will not be available"
        )

    # Import and start the asynchronous processor.
    from .core.async_processing import async_processor

    try:
        async_processor.start_background_cleanup()
        logger.info("Async processor background cleanup started")

    except Exception:
        logger.exception(
            "Failed to start async processor background cleanup"
        )
        raise

    try:
        yield

    finally:
        logger.info("Shutting down...")

        # Stop the Redis listener before closing application resources.
        if listener_task is not None:
            listener_task.cancel()

            try:
                await listener_task

            except asyncio.CancelledError:
                pass

            except Exception:
                logger.exception(
                    "Error while stopping cache invalidation listener"
                )

        # Stop background processing.
        if async_processor is not None:
            try:
                await async_processor.shutdown()
                logger.info("Async processor shutdown completed")

            except Exception:
                logger.exception("Error shutting down async processor")

        # Close the SQLAlchemy database pool.
        try:
            await db_pool.close()
            logger.info("SQLAlchemy database pool closed")

        except Exception:
            logger.exception("Error closing SQLAlchemy database pool")

        # Close the existing Supabase connection pool.
        try:
            from .core.supabase_connection_pool import supabase_pool

            await supabase_pool.close()
            logger.info("Supabase connection pool closed")

        except Exception:
            logger.exception("Error closing Supabase connection pool")

        logger.info("Shutdown completed")


# -------------------------------------------------------------------
# FastAPI application
# -------------------------------------------------------------------

app = FastAPI(
    title="Auth Skeleton API",
    description=(
        "Authentication and User Management API - "
        "Developer Testing Skeleton"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# -------------------------------------------------------------------
# Middleware
# -------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    GZipMiddleware,
    minimum_size=1000,
    compresslevel=6,
)

app.add_middleware(PerformanceMonitoringMiddleware)


# -------------------------------------------------------------------
# API routers
# -------------------------------------------------------------------

# Authentication
app.include_router(login.router, prefix="/api/v1", tags=["auth"])
app.include_router(auth_info.router, prefix="/api/v1", tags=["auth"])
app.include_router(
    persistent_auth.router,
    prefix="/api/v1/auth",
    tags=["persistent-auth"],
)

# User management
app.include_router(users_lightning.router, prefix="/api/v1", tags=["users"])
app.include_router(profile.router, prefix="/api/v1", tags=["profile"])

# Dashboard
app.include_router(dashboard.router, prefix="/api/v1", tags=["dashboard"])

# Bootstrap and company settings
app.include_router(
    company_settings.router,
    prefix="/api/v1",
    tags=["company-settings"],
)
app.include_router(
    bootstrap.router,
    prefix="/api/v1",
    tags=["bootstrap"],
)

# Departments and permissions
app.include_router(
    departments.router,
    prefix="/api/v1",
    tags=["departments"],
)

# Cities and city access
app.include_router(cities.router, prefix="/api/v1", tags=["cities"])
app.include_router(
    city_access_fast.router,
    prefix="/api/v1",
    tags=["city-access-fast"],
)
app.include_router(
    city_access_fixed.router,
    prefix="/api/v1",
    tags=["city-access-fixed"],
)

# Infrastructure
app.include_router(health.router, prefix="/api/v1", tags=["health"])


# -------------------------------------------------------------------
# Health endpoints
# -------------------------------------------------------------------

@app.api_route("/health", methods=["GET", "HEAD"])
async def health_check():
    try:
        from .database import supabase

        db_health = await supabase.health_check()

        return {
            "status": "healthy",
            "database": db_health,
        }

    except Exception as e:
        return {
            "status": "degraded",
            "database": {
                "status": "unhealthy",
                "error": str(e),
            },
        }


@app.get("/up")
async def up_check():
    return {"status": "up"}


@app.get("/pool-status")
async def pool_status():
    """Get the existing Supabase connection pool status."""
    try:
        from .database import supabase

        return await supabase.get_pool_status()

    except Exception as e:
        return {
            "error": f"Failed to get pool status: {str(e)}"
        }


@app.get("/database-health")
async def database_health():
    """Get the existing Supabase database health status."""
    try:
        from .database import supabase

        return await supabase.health_check()

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "timestamp": time.time(),
        }


@app.api_route("/api/v1/health", methods=["GET", "HEAD"])
async def api_health_check():
    try:
        from .database import supabase

        db_health = await supabase.health_check()

        return {
            "status": "healthy",
            "database": db_health,
        }

    except Exception as e:
        return {
            "status": "degraded",
            "database": {
                "status": "unhealthy",
                "error": str(e),
            },
        }


@app.get("/api/v1/up")
async def api_up_check():
    return {"status": "up"}


@app.get("/api/v1/pool-status")
async def api_pool_status():
    """Get the Supabase pool status through the API."""
    try:
        from .database import supabase

        return await supabase.get_pool_status()

    except Exception as e:
        return {
            "error": f"Failed to get pool status: {str(e)}"
        }


@app.get("/api/v1/database-health")
async def api_database_health():
    """Get the database health status through the API."""
    try:
        from .database import supabase

        return await supabase.health_check()

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "timestamp": time.time(),
        }


# -------------------------------------------------------------------
# Circuit breaker management
# -------------------------------------------------------------------

@app.post("/api/v1/circuit-breaker/reset")
async def reset_circuit_breaker():
    """Reset circuit breakers to allow operations to resume."""
    try:
        from .core.supabase_connection_pool import supabase_pool
        from .core.async_supabase import connection_tracker

        if supabase_pool._initialized:
            supabase_pool._circuit_breaker_open = False
            supabase_pool._failed_operations_count = 0
            supabase_pool._circuit_breaker_opened_at = None

        connection_tracker.failed_connections = 0
        connection_tracker.last_failure = None
        connection_tracker.retry_counts.clear()
        connection_tracker.operation_timeouts.clear()

        logger.info("Circuit breakers have been manually reset")

        return {
            "status": "success",
            "message": "Circuit breakers reset successfully",
            "timestamp": time.time(),
        }

    except Exception as e:
        logger.exception("Failed to reset circuit breakers")

        return {
            "status": "error",
            "error": str(e),
            "timestamp": time.time(),
        }


@app.get("/api/v1/circuit-breaker/status")
async def circuit_breaker_status():
    """Get the current circuit breaker status."""
    try:
        from .core.supabase_connection_pool import supabase_pool
        from .core.async_supabase import connection_tracker

        status = {
            "connection_pool": {
                "circuit_breaker_open": False,
                "failed_operations_count": 0,
                "circuit_breaker_opened_at": None,
                "threshold": 10,
                "timeout": 60,
            },
            "async_tracker": {
                "failed_connections": connection_tracker.failed_connections,
                "last_failure": connection_tracker.last_failure,
                "should_throttle": connection_tracker.should_throttle(),
                "active_retry_operations": len(
                    connection_tracker.retry_counts
                ),
                "failure_threshold": connection_tracker.failure_threshold,
                "throttle_duration": connection_tracker.throttle_duration,
            },
            "timestamp": time.time(),
        }

        if supabase_pool._initialized:
            status["connection_pool"]["circuit_breaker_open"] = (
                supabase_pool._circuit_breaker_open
            )
            status["connection_pool"]["failed_operations_count"] = (
                supabase_pool._failed_operations_count
            )
            status["connection_pool"]["circuit_breaker_opened_at"] = (
                supabase_pool._circuit_breaker_opened_at
            )

        return status

    except Exception as e:
        logger.exception("Failed to get circuit breaker status")

        return {
            "status": "error",
            "error": str(e),
            "timestamp": time.time(),
        }


@app.post("/api/v1/circuit-breaker/configure")
async def configure_circuit_breaker(request: Request):
    """Configure circuit breaker thresholds and timeouts."""
    try:
        body = await request.json()

        from .core.supabase_connection_pool import supabase_pool
        from .core.async_supabase import connection_tracker

        changes_made = []

        if "pool_threshold" in body:
            supabase_pool._circuit_breaker_threshold = body["pool_threshold"]
            changes_made.append(
                f"Pool threshold set to {body['pool_threshold']}"
            )

        if "pool_timeout" in body:
            supabase_pool._circuit_breaker_timeout = body["pool_timeout"]
            changes_made.append(
                f"Pool timeout set to {body['pool_timeout']}s"
            )

        if "tracker_threshold" in body:
            connection_tracker.failure_threshold = body["tracker_threshold"]
            changes_made.append(
                f"Tracker threshold set to {body['tracker_threshold']}"
            )

        if "tracker_timeout" in body:
            connection_tracker.throttle_duration = body["tracker_timeout"]
            changes_made.append(
                f"Tracker timeout set to {body['tracker_timeout']}s"
            )

        logger.info(
            "Circuit breaker configuration updated: %s",
            changes_made,
        )

        return {
            "status": "success",
            "changes": changes_made,
            "timestamp": time.time(),
        }

    except Exception as e:
        logger.exception("Failed to configure circuit breakers")

        return {
            "status": "error",
            "error": str(e),
            "timestamp": time.time(),
        }


# -------------------------------------------------------------------
# Fallback service management
# -------------------------------------------------------------------

@app.get("/api/v1/fallback/status")
async def fallback_status():
    """Get fallback service status and cache information."""
    try:
        from .core.circuit_breaker_fallback import fallback_service

        return {
            "status": "active",
            "cache": fallback_service.get_cache_status(),
            "timestamp": time.time(),
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "timestamp": time.time(),
        }


@app.post("/api/v1/fallback/clear-cache")
async def clear_fallback_cache():
    """Clear the fallback service cache."""
    try:
        from .core.circuit_breaker_fallback import fallback_service

        fallback_service.clear_cache()
        logger.info("Fallback cache cleared manually")

        return {
            "status": "success",
            "message": "Fallback cache cleared successfully",
            "timestamp": time.time(),
        }

    except Exception as e:
        logger.exception("Failed to clear fallback cache")

        return {
            "status": "error",
            "error": str(e),
            "timestamp": time.time(),
        }


# -------------------------------------------------------------------
# Serve frontend static files (SPA)
# -------------------------------------------------------------------

static_dir = None

if os.path.isdir("static"):
    static_dir = "static"
elif os.path.isdir("dist"):
    static_dir = "dist"

if static_dir:
    assets_dir = os.path.join(static_dir, "assets")

    if os.path.isdir(assets_dir):
        app.mount(
            "/assets",
            StaticFiles(directory=assets_dir),
            name="assets",
        )

    app.mount(
        "/static",
        StaticFiles(directory=static_dir),
        name="static",
    )

    @app.get("/{full_path:path}")
    async def serve_spa(request: Request, full_path: str):
        # Do not intercept API or health routes.
        if (
            full_path.startswith("api/")
            or full_path == "api"
            or full_path.startswith("health")
            or full_path.startswith("up")
            or full_path.startswith("pool-status")
            or full_path.startswith("database-health")
        ):
            raise HTTPException(
                status_code=404,
                detail="Not found",
            )

        # Serve a requested file if it exists.
        static_file_path = os.path.join(static_dir, full_path)

        if os.path.isfile(static_file_path):
            return FileResponse(static_file_path)

        # Serve the SPA entry point for frontend routes.
        index_path = os.path.join(static_dir, "index.html")

        if os.path.isfile(index_path):
            return FileResponse(index_path)

        raise HTTPException(
            status_code=404,
            detail="Not found",
        )


# -------------------------------------------------------------------
# Local development entry point
# -------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
    )
