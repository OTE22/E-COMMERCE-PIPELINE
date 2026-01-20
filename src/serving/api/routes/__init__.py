"""
API Routes Module
"""
from .health import router as health_router
from .orders import router as orders_router
from .products import router as products_router
from .customers import router as customers_router
from .analytics import router as analytics_router

__all__ = [
    "health_router",
    "orders_router",
    "products_router",
    "customers_router",
    "analytics_router",
]
