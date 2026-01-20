"""
Orders API Endpoints

REST API for order analytics and data retrieval.
"""

from datetime import datetime, date
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.connection import get_db_dependency
from src.database.models import FactOrder, FactOrderItem, DimCustomer, OrderStatus
from src.serving.cache import orders_cache

router = APIRouter()


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class OrderSummary(BaseModel):
    """Order summary response"""
    order_id: UUID
    order_number: str
    customer_id: UUID
    status: str
    total_amount: float
    item_count: int
    order_timestamp: datetime
    
    class Config:
        from_attributes = True


class OrderDetail(OrderSummary):
    """Detailed order response"""
    subtotal: float
    discount_amount: float
    tax_amount: float
    shipping_amount: float
    payment_method: Optional[str]
    shipping_method: Optional[str]
    is_first_order: bool


class OrderListResponse(BaseModel):
    """Paginated order list"""
    items: List[OrderSummary]
    total: int
    page: int
    page_size: int
    total_pages: int


class OrderMetrics(BaseModel):
    """Order metrics summary"""
    total_orders: int
    total_revenue: float
    avg_order_value: float
    total_items_sold: int
    unique_customers: int


class OrderFilters(BaseModel):
    """Order filter parameters"""
    status: Optional[OrderStatus] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None
    customer_id: Optional[UUID] = None


# =============================================================================
# ENDPOINTS
# =============================================================================

@router.get("", response_model=OrderListResponse)
async def list_orders(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    status: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    customer_id: Optional[UUID] = None,
    db: AsyncSession = Depends(get_db_dependency),
) -> OrderListResponse:
    """
    List orders with pagination and filtering.
    
    Supports filtering by:
    - Status
    - Date range
    - Customer
    """
    # Build query
    query = select(FactOrder)
    count_query = select(func.count(FactOrder.order_id))
    
    # Apply filters
    conditions = []
    if status:
        conditions.append(FactOrder.status == status)
    if start_date:
        conditions.append(FactOrder.order_timestamp >= datetime.combine(start_date, datetime.min.time()))
    if end_date:
        conditions.append(FactOrder.order_timestamp <= datetime.combine(end_date, datetime.max.time()))
    if customer_id:
        conditions.append(FactOrder.customer_id == customer_id)
    
    if conditions:
        query = query.where(and_(*conditions))
        count_query = count_query.where(and_(*conditions))
    
    # Get total count
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0
    
    # Apply pagination
    offset = (page - 1) * page_size
    query = query.order_by(FactOrder.order_timestamp.desc()).offset(offset).limit(page_size)
    
    # Execute query
    result = await db.execute(query)
    orders = result.scalars().all()
    
    return OrderListResponse(
        items=[OrderSummary.model_validate(o) for o in orders],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size,
    )


@router.get("/metrics", response_model=OrderMetrics)
async def get_order_metrics(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: AsyncSession = Depends(get_db_dependency),
) -> OrderMetrics:
    """
    Get aggregated order metrics.
    
    Returns total orders, revenue, AOV, items sold, and unique customers.
    """
    # Try cache first
    cache_key = f"metrics:{start_date}:{end_date}"
    cached = await orders_cache.get(cache_key)
    if cached:
        return OrderMetrics(**cached)
    
    # Build query
    query = select(
        func.count(FactOrder.order_id).label("total_orders"),
        func.sum(FactOrder.total_amount).label("total_revenue"),
        func.avg(FactOrder.total_amount).label("avg_order_value"),
        func.sum(FactOrder.item_count).label("total_items_sold"),
        func.count(func.distinct(FactOrder.customer_id)).label("unique_customers"),
    )
    
    # Apply date filters
    conditions = []
    if start_date:
        conditions.append(FactOrder.order_timestamp >= datetime.combine(start_date, datetime.min.time()))
    if end_date:
        conditions.append(FactOrder.order_timestamp <= datetime.combine(end_date, datetime.max.time()))
    
    if conditions:
        query = query.where(and_(*conditions))
    
    # Execute
    result = await db.execute(query)
    row = result.one()
    
    metrics = OrderMetrics(
        total_orders=row.total_orders or 0,
        total_revenue=float(row.total_revenue or 0),
        avg_order_value=float(row.avg_order_value or 0),
        total_items_sold=row.total_items_sold or 0,
        unique_customers=row.unique_customers or 0,
    )
    
    # Cache result
    await orders_cache.set(cache_key, metrics.model_dump())
    
    return metrics


@router.get("/{order_id}", response_model=OrderDetail)
async def get_order(
    order_id: UUID,
    db: AsyncSession = Depends(get_db_dependency),
) -> OrderDetail:
    """
    Get order details by ID.
    """
    # Try cache
    cached = await orders_cache.get(str(order_id))
    if cached:
        return OrderDetail(**cached)
    
    # Query database
    result = await db.execute(
        select(FactOrder).where(FactOrder.order_id == order_id)
    )
    order = result.scalar_one_or_none()
    
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    detail = OrderDetail.model_validate(order)
    
    # Cache
    await orders_cache.set(str(order_id), detail.model_dump())
    
    return detail


@router.get("/{order_id}/items")
async def get_order_items(
    order_id: UUID,
    db: AsyncSession = Depends(get_db_dependency),
):
    """Get line items for an order."""
    result = await db.execute(
        select(FactOrderItem).where(FactOrderItem.order_id == order_id)
    )
    items = result.scalars().all()
    
    return {"order_id": order_id, "items": items}


@router.get("/customer/{customer_id}")
async def get_customer_orders(
    customer_id: UUID,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=50),
    db: AsyncSession = Depends(get_db_dependency),
) -> OrderListResponse:
    """Get all orders for a specific customer."""
    return await list_orders(
        page=page,
        page_size=page_size,
        customer_id=customer_id,
        db=db,
    )
