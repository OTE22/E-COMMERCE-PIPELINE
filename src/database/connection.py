"""
Database Connection Management

Production-grade async database connection pool with SQLAlchemy 2.0.
Implements connection pooling, health checks, and graceful shutdown.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import structlog
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool, QueuePool
from sqlalchemy import text

from src.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()

# Global engine and session factory
_engine: Optional[AsyncEngine] = None
_async_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


async def init_database() -> AsyncEngine:
    """
    Initialize the database connection pool.
    
    Creates an async engine with connection pooling optimized for production workloads.
    
    Returns:
        AsyncEngine: The initialized database engine
    """
    global _engine, _async_session_factory
    
    if _engine is not None:
        logger.warning("Database already initialized")
        return _engine
    
    # Engine configuration
    engine_config = {
        "echo": settings.database.echo,
        "future": True,
        "pool_pre_ping": True,  # Verify connections before use
    }
    
    # Use NullPool for async engines (QueuePool doesn't work with asyncio)
    # AsyncPG handles its own connection pooling internally
    engine_config.update({
        "poolclass": NullPool,
    })
    
    _engine = create_async_engine(
        settings.database.async_url,
        **engine_config,
    )
    
    _async_session_factory = async_sessionmaker(
        bind=_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )
    
    # Verify connection
    try:
        async with _engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info(
            "Database connection established",
            host=settings.database.host,
            database=settings.database.db,
        )
    except Exception as e:
        logger.error("Failed to connect to database", error=str(e))
        raise
    
    return _engine


async def close_database() -> None:
    """
    Close the database connection pool.
    
    Gracefully closes all connections in the pool.
    """
    global _engine, _async_session_factory
    
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _async_session_factory = None
        logger.info("Database connection pool closed")


def get_engine() -> AsyncEngine:
    """
    Get the database engine.
    
    Returns:
        AsyncEngine: The active database engine
        
    Raises:
        RuntimeError: If database is not initialized
    """
    if _engine is None:
        raise RuntimeError("Database not initialized. Call init_database() first.")
    return _engine


@asynccontextmanager
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Get a database session from the pool.
    
    Context manager that provides a database session and handles
    commit/rollback/close automatically.
    
    Yields:
        AsyncSession: Database session
        
    Example:
        async with get_db() as db:
            result = await db.execute(query)
    """
    if _async_session_factory is None:
        logger.error("Database not initialized when get_db() called")
        raise RuntimeError("Database not initialized. Call init_database() first.")
    
    logger.debug("Creating new database session")
    session = _async_session_factory()
    try:
        yield session
        await session.commit()
        logger.debug("Database session committed successfully")
    except Exception as e:
        logger.error("Database session error, rolling back", error=str(e), error_type=type(e).__name__)
        await session.rollback()
        raise
    finally:
        await session.close()
        logger.debug("Database session closed")


async def get_db_dependency() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency for database sessions.
    
    Use this in FastAPI route handlers:
    
    Example:
        @app.get("/items")
        async def get_items(db: AsyncSession = Depends(get_db_dependency)):
            ...
    """
    async with get_db() as session:
        yield session


# Alias for FastAPI dependency injection
AsyncSessionLocal = get_db_dependency


async def check_database_health() -> dict:
    """
    Check database health status.
    
    Returns:
        dict: Health status with latency information
    """
    import time
    
    try:
        start = time.perf_counter()
        async with get_db() as db:
            await db.execute(text("SELECT 1"))
        latency_ms = (time.perf_counter() - start) * 1000
        
        return {
            "status": "healthy",
            "latency_ms": round(latency_ms, 2),
            "pool_size": settings.database.pool_size,
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
        }
