"""
Test Suite Configuration
"""
import asyncio
import pytest
from typing import AsyncGenerator, Generator

import polars as pl
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from src.config import Settings
from src.database.models import Base


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an event loop for async tests"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    """Create test settings"""
    return Settings(
        app_env="testing",
        debug=True,
    )


@pytest.fixture(scope="session")
async def test_engine():
    """Create test database engine"""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
    )
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine
    
    await engine.dispose()


@pytest.fixture
async def test_db(test_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create test database session"""
    session_factory = async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    
    async with session_factory() as session:
        yield session
        await session.rollback()


@pytest.fixture
def sample_orders_df() -> pl.DataFrame:
    """Create sample orders DataFrame for testing"""
    return pl.DataFrame({
        "order_id": ["ord-1", "ord-2", "ord-3"],
        "order_number": ["ORD-001", "ORD-002", "ORD-003"],
        "customer_id": ["cust-1", "cust-1", "cust-2"],
        "order_timestamp": [
            "2025-01-01 10:00:00",
            "2025-01-15 14:30:00",
            "2025-01-20 09:15:00",
        ],
        "status": ["delivered", "shipped", "pending"],
        "total_amount": [150.00, 200.50, 75.25],
        "item_count": [3, 2, 1],
        "discount_amount": [10.00, 0.00, 5.00],
        "tax_amount": [12.00, 16.04, 5.62],
        "shipping_amount": [0.00, 9.99, 5.99],
    })


@pytest.fixture
def sample_customers_df() -> pl.DataFrame:
    """Create sample customers DataFrame for testing"""
    return pl.DataFrame({
        "customer_id": ["cust-1", "cust-2", "cust-3"],
        "customer_key": ["CUST-001", "CUST-002", "CUST-003"],
        "email": ["john@example.com", "jane@example.com", "bob@example.com"],
        "first_name": ["John", "Jane", "Bob"],
        "last_name": ["Doe", "Smith", "Wilson"],
        "country": ["US", "UK", "CA"],
        "segment": ["returning", "vip", "new"],
        "lifetime_value": [500.00, 2500.00, 75.25],
        "total_orders": [5, 25, 1],
    })


@pytest.fixture
def sample_products_df() -> pl.DataFrame:
    """Create sample products DataFrame for testing"""
    return pl.DataFrame({
        "product_id": ["prod-1", "prod-2", "prod-3"],
        "product_key": ["PROD-001", "PROD-002", "PROD-003"],
        "sku": ["SKU-001", "SKU-002", "SKU-003"],
        "name": ["Wireless Mouse", "USB Keyboard", "Monitor Stand"],
        "category": ["electronics", "electronics", "home_garden"],
        "unit_price": [29.99, 49.99, 39.99],
        "cost_price": [15.00, 25.00, 18.00],
        "stock_quantity": [100, 50, 25],
        "is_in_stock": [True, True, True],
        "avg_rating": [4.5, 4.2, 4.8],
        "is_active": [True, True, True],
    })
