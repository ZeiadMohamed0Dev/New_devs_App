
import logging

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ..config import settings

logger = logging.getLogger(__name__)


class DatabasePool:
    def __init__(self):
        self.engine = None
        self.session_factory = None

    async def initialize(self):
        """Initialize the shared asynchronous database connection pool."""

        if self.engine is not None and self.session_factory is not None:
            return

        database_url = settings.database_url

        # Convert PostgreSQL URLs to the async SQLAlchemy driver format.
        if database_url.startswith("postgres://"):
            database_url = database_url.replace(
                "postgres://", "postgresql+asyncpg://", 1
            )
        elif database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )

        try:
            engine = create_async_engine(
                database_url,
                pool_size=settings.database_pool_size,
                max_overflow=settings.database_max_overflow,
                pool_timeout=settings.database_pool_timeout,
                pool_recycle=settings.database_pool_recycle,
                pool_pre_ping=True,
                echo=False,
            )

            session_factory = async_sessionmaker(
                bind=engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            self.engine = engine
            self.session_factory = session_factory

            logger.info("SQLAlchemy database pool initialized")

        except Exception:
            logger.exception("Failed to initialize SQLAlchemy database pool")

            if self.engine is not None:
                await self.engine.dispose()

            self.engine = None
            self.session_factory = None
            raise

    def get_session(self) -> AsyncSession:
        """Create a new asynchronous database session."""

        if self.session_factory is None:
            raise RuntimeError("Database pool is not initialized")

        return self.session_factory()

    async def close(self):
        """Dispose of the shared connection pool."""

        if self.engine is not None:
            await self.engine.dispose()

        self.engine = None
        self.session_factory = None

        logger.info("SQLAlchemy database pool closed")


# One shared instance for the application.
db_pool = DatabasePool()


async def get_db_session():
    """FastAPI dependency that yields a database session."""

    if db_pool.session_factory is None:
        await db_pool.initialize()

    async with db_pool.get_session() as session:
        yield session
