```python
import logging

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker,
)

from ..config import settings

logger = logging.getLogger(__name__)


class DatabasePool:
    def __init__(self):
        self.engine = None
        self.session_factory = None

    async def initialize(self):
        """Initialize the asynchronous database connection pool."""

        if self.engine is not None and self.session_factory is not None:
            return

        try:
            database_url = (
                f"postgresql+asyncpg://"
                f"{settings.supabase_db_user}:"
                f"{settings.supabase_db_password}@"
                f"{settings.supabase_db_host}:"
                f"{settings.supabase_db_port}/"
                f"{settings.supabase_db_name}"
            )

            self.engine = create_async_engine(
                database_url,
                pool_size=5,
                max_overflow=5,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
            )

            self.session_factory = async_sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            logger.info("Database connection pool initialized")

        except Exception:
            logger.exception("Database pool initialization failed")

            if self.engine is not None:
                await self.engine.dispose()

            self.engine = None
            self.session_factory = None
            raise

    def get_session(self) -> AsyncSession:
        """Create a database session."""

        if self.session_factory is None:
            raise RuntimeError("Database pool is not initialized")

        return self.session_factory()

    async def close(self):
        """Close the database connection pool."""

        if self.engine is not None:
            await self.engine.dispose()

        self.engine = None
        self.session_factory = None


# Shared database pool instance
db_pool = DatabasePool()


async def get_db_session():
    """FastAPI dependency that provides a database session."""

    if db_pool.session_factory is None:
        await db_pool.initialize()

    async with db_pool.get_session() as session:
        yield session
