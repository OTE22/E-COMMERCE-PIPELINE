"""
Products API Endpoints

REST API for product catalog and analytics.
"""

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.connection import get_db_dependency
from src.database.models import DimProduct, ProductCategory
from src.serving.cache import products_cache

router = APIRouter()


class ProductSummary(BaseModel):
    """Product summary response"""
    product_id: UUID
    sku: str
    name: str
    category: str
    unit_price: float
    stock_quantity: int
    is_in_stock: bool
    avg_rating: Optional[float]
    
    class Config:
        from_attributes = True


class ProductDetail(ProductSummary):
    """Detailed product response"""
    description: Optional[str]
    brand: Optional[str]
    subcategory: Optional[str]
    cost_price: Optional[float]
    margin_percent: Optional[float]
    review_count: int
    is_active: bool


class ProductListResponse(BaseModel):
    """Paginated product list"""
    items: List[ProductSummary]
    total: int
    page: int
    page_size: int


class ProductMetrics(BaseModel):
    """Catalog metrics"""
    total_products: int
    active_products: int
    out_of_stock: int
    avg_price: float
    categories_count: int


@router.get("", response_model=ProductListResponse)
async def list_products(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    category: Optional[str] = None,
    brand: Optional[str] = None,
    in_stock_only: bool = False,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db_dependency),
) -> ProductListResponse:
    """
    List products with filtering and search.
    """
    query = select(DimProduct)
    count_query = select(func.count(DimProduct.product_id))
    
    conditions = [DimProduct.is_active == True]
    
    if category:
        conditions.append(DimProduct.category == category)
    if brand:
        conditions.append(DimProduct.brand == brand)
    if in_stock_only:
        conditions.append(DimProduct.is_in_stock == True)
    if min_price:
        conditions.append(DimProduct.unit_price >= min_price)
    if max_price:
        conditions.append(DimProduct.unit_price <= max_price)
    if search:
        conditions.append(DimProduct.name.ilike(f"%{search}%"))
    
    query = query.where(and_(*conditions))
    count_query = count_query.where(and_(*conditions))
    
    # Get total
    total = (await db.execute(count_query)).scalar() or 0
    
    # Paginate
    offset = (page - 1) * page_size
    query = query.order_by(DimProduct.name).offset(offset).limit(page_size)
    
    result = await db.execute(query)
    products = result.scalars().all()
    
    return ProductListResponse(
        items=[ProductSummary.model_validate(p) for p in products],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/metrics", response_model=ProductMetrics)
async def get_product_metrics(
    db: AsyncSession = Depends(get_db_dependency),
) -> ProductMetrics:
    """Get catalog metrics."""
    # Try cache
    cached = await products_cache.get("metrics")
    if cached:
        return ProductMetrics(**cached)
    
    # Calculate metrics
    total = (await db.execute(select(func.count(DimProduct.product_id)))).scalar() or 0
    active = (await db.execute(
        select(func.count(DimProduct.product_id)).where(DimProduct.is_active == True)
    )).scalar() or 0
    out_of_stock = (await db.execute(
        select(func.count(DimProduct.product_id)).where(DimProduct.is_in_stock == False)
    )).scalar() or 0
    avg_price = (await db.execute(
        select(func.avg(DimProduct.unit_price))
    )).scalar() or 0
    categories = (await db.execute(
        select(func.count(func.distinct(DimProduct.category)))
    )).scalar() or 0
    
    metrics = ProductMetrics(
        total_products=total,
        active_products=active,
        out_of_stock=out_of_stock,
        avg_price=float(avg_price),
        categories_count=categories,
    )
    
    await products_cache.set("metrics", metrics.model_dump())
    return metrics


@router.get("/categories")
async def list_categories(
    db: AsyncSession = Depends(get_db_dependency),
):
    """List all product categories with counts."""
    result = await db.execute(
        select(
            DimProduct.category,
            func.count(DimProduct.product_id).label("count"),
        ).group_by(DimProduct.category)
    )
    
    return [
        {"category": row.category, "count": row.count}
        for row in result.all()
    ]


@router.get("/low-stock")
async def get_low_stock_products(
    threshold: int = Query(10, ge=0),
    db: AsyncSession = Depends(get_db_dependency),
):
    """Get products with stock below threshold."""
    result = await db.execute(
        select(DimProduct)
        .where(
            and_(
                DimProduct.stock_quantity <= threshold,
                DimProduct.is_active == True,
            )
        )
        .order_by(DimProduct.stock_quantity)
        .limit(100)
    )
    
    return [ProductSummary.model_validate(p) for p in result.scalars().all()]


@router.get("/{product_id}", response_model=ProductDetail)
async def get_product(
    product_id: UUID,
    db: AsyncSession = Depends(get_db_dependency),
) -> ProductDetail:
    """Get product details."""
    cached = await products_cache.get(str(product_id))
    if cached:
        return ProductDetail(**cached)
    
    result = await db.execute(
        select(DimProduct).where(DimProduct.product_id == product_id)
    )
    product = result.scalar_one_or_none()
    
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    detail = ProductDetail.model_validate(product)
    await products_cache.set(str(product_id), detail.model_dump())
    
    return detail
