"""
FastAPI Production Application

Main entry point for the E-Commerce Analytics API.
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import structlog

from src.config import get_settings
from src.database.connection import init_database, close_database
from src.serving.cache import init_redis, close_redis
from src.serving.api.middleware import (
    RequestLoggingMiddleware,
    RateLimitMiddleware,
    SecurityHeadersMiddleware,
)
from src.serving.api.routes import (
    health_router,
    orders_router,
    products_router,
    customers_router,
    analytics_router,
)

settings = get_settings()
logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Configure logging first
    from src.config.logging import configure_logging
    configure_logging("DEBUG")  # Set to DEBUG for visibility
    
    logger.info("Starting E-Commerce Analytics API")
    
    # Initialize services
    try:
        await init_database()
        logger.info("Database initialized")
    except Exception as e:
        logger.warning(f"Database init failed: {e}")
    
    try:
        await init_redis()
        logger.info("Redis initialized")
    except Exception as e:
        logger.warning(f"Redis init failed: {e}")
    
    yield
    
    # Cleanup
    logger.info("Shutting down...")
    await close_database()
    await close_redis()


# Create FastAPI application
app = FastAPI(
    title="E-Commerce Analytics API",
    description="Production-grade analytics and ML-ready data API for e-commerce",
    version=settings.version,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Compression
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Custom middleware
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RateLimitMiddleware, max_requests=100, window_seconds=60)

# API routes
app.include_router(health_router, prefix="/api/v1", tags=["Health"])
app.include_router(orders_router, prefix="/api/v1/orders", tags=["Orders"])
app.include_router(products_router, prefix="/api/v1/products", tags=["Products"])
app.include_router(customers_router, prefix="/api/v1/customers", tags=["Customers"])
app.include_router(analytics_router, prefix="/api/v1/analytics", tags=["Analytics"])

# GraphQL (optional)
try:
    from src.serving.api.graphql import graphql_app
    app.include_router(graphql_app, prefix="/graphql", tags=["GraphQL"])
except ImportError:
    pass

# Serve frontend static files
frontend_path = Path(__file__).parent.parent / "frontend"
if frontend_path.exists():
    # Mount CSS and JS directories
    css_path = frontend_path / "css"
    js_path = frontend_path / "js"
    
    if css_path.exists():
        app.mount("/css", StaticFiles(directory=css_path), name="css")
    if js_path.exists():
        app.mount("/js", StaticFiles(directory=js_path), name="js")
    
    # Also mount entire frontend as static
    app.mount("/static", StaticFiles(directory=frontend_path), name="static")
    
    @app.get("/")
    async def serve_frontend():
        """Serve the frontend dashboard."""
        return FileResponse(frontend_path / "index.html")

    # SPA Catch-all using starlette path convertor
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        """Serve index.html for SPA routing."""
        # Don't catch API calls
        if full_path.startswith("api") or full_path.startswith("static"):
            from fastapi import HTTPException
            raise HTTPException(status_code=404)
        return FileResponse(frontend_path / "index.html")


@app.get("/api/v1/info")
async def api_info():
    """API information endpoint."""
    return {
        "name": "E-Commerce Analytics API",
        "version": settings.version,
        "environment": settings.app_env,
        "documentation": "/docs",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
