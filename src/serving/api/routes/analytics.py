"""
Analytics API Endpoints

REST API for business analytics and dashboards.
"""

from datetime import date, datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select, func, and_, text
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from src.database.connection import get_db_dependency
from src.database.models import (
    FactOrder, 
    FactPageView, 
    DimCustomer, 
    DimProduct,
    AggDailySales,
)
from src.serving.cache import analytics_cache

router = APIRouter()
logger = structlog.get_logger(__name__)

logger.info("Analytics router initialized")


class SalesOverview(BaseModel):
    """Sales overview metrics"""
    total_revenue: float
    total_orders: int
    avg_order_value: float
    total_customers: int
    conversion_rate: Optional[float]
    revenue_growth: Optional[float]
    orders_growth: Optional[float]


class DailySalesData(BaseModel):
    """Daily sales data point"""
    date: date
    revenue: float
    orders: int
    customers: int


class SalesTrend(BaseModel):
    """Sales trend response"""
    data: List[DailySalesData]
    period_start: date
    period_end: date
    total_revenue: float
    total_orders: int


class CategorySales(BaseModel):
    """Sales by category"""
    category: str
    revenue: float
    orders: int
    percentage: float


class HourlyDistribution(BaseModel):
    """Hourly order distribution"""
    hour: int
    orders: int
    revenue: float


@router.get("/sales/overview", response_model=SalesOverview)
async def get_sales_overview(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    compare_previous: bool = True,
    db: AsyncSession = Depends(get_db_dependency),
) -> SalesOverview:
    """
    Get sales overview with optional comparison to previous period.
    """
    logger.info("get_sales_overview called", start_date=str(start_date), end_date=str(end_date))
    
    # Default to last 30 days
    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    logger.debug("Date range calculated", start_date=str(start_date), end_date=str(end_date))
    
    cache_key = f"overview:{start_date}:{end_date}"
    cached = await analytics_cache.get(cache_key)
    if cached:
        logger.debug("Returning cached overview")
        return SalesOverview(**cached)
    
    # Current period metrics
    logger.debug("Querying database for sales overview")
    result = await db.execute(
        select(
            func.sum(FactOrder.total_amount).label("revenue"),
            func.count(FactOrder.order_id).label("orders"),
            func.avg(FactOrder.total_amount).label("aov"),
            func.count(func.distinct(FactOrder.customer_id)).label("customers"),
        ).where(
            and_(
                FactOrder.order_timestamp >= datetime.combine(start_date, datetime.min.time()),
                FactOrder.order_timestamp <= datetime.combine(end_date, datetime.max.time()),
            )
        )
    )
    current = result.one()
    
    logger.info(
        "Sales overview query completed",
        revenue=float(current.revenue or 0),
        orders=current.orders or 0,
        customers=current.customers or 0,
    )
    
    overview = SalesOverview(
        total_revenue=float(current.revenue or 0),
        total_orders=current.orders or 0,
        avg_order_value=float(current.aov or 0),
        total_customers=current.customers or 0,
        conversion_rate=None,
        revenue_growth=None,
        orders_growth=None,
    )
    
    # Calculate growth if comparing
    if compare_previous:
        period_days = (end_date - start_date).days
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=period_days)
        
        prev_result = await db.execute(
            select(
                func.sum(FactOrder.total_amount).label("revenue"),
                func.count(FactOrder.order_id).label("orders"),
            ).where(
                and_(
                    FactOrder.order_timestamp >= datetime.combine(prev_start, datetime.min.time()),
                    FactOrder.order_timestamp <= datetime.combine(prev_end, datetime.max.time()),
                )
            )
        )
        prev = prev_result.one()
        
        if prev.revenue and prev.revenue > 0:
            overview.revenue_growth = ((current.revenue - prev.revenue) / prev.revenue) * 100
        if prev.orders and prev.orders > 0:
            overview.orders_growth = ((current.orders - prev.orders) / prev.orders) * 100
        
        logger.debug("Growth calculated", revenue_growth=overview.revenue_growth, orders_growth=overview.orders_growth)
    
    await analytics_cache.set(cache_key, overview.model_dump())
    logger.info("Sales overview returned successfully")
    return overview


@router.get("/sales/trend", response_model=SalesTrend)
async def get_sales_trend(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    granularity: str = Query("day", enum=["day", "week", "month"]),
    db: AsyncSession = Depends(get_db_dependency),
) -> SalesTrend:
    """
    Get sales trend over time.
    """
    logger.info("get_sales_trend called", start_date=str(start_date), end_date=str(end_date), granularity=granularity)
    
    try:
        if not end_date:
            end_date = date.today()
        if not start_date:
            start_date = end_date - timedelta(days=30)
        
        logger.debug("Querying sales trend data")
        # Query daily aggregates
        result = await db.execute(
            select(
                func.date(FactOrder.order_timestamp).label("date"),
                func.sum(FactOrder.total_amount).label("revenue"),
                func.count(FactOrder.order_id).label("orders"),
                func.count(func.distinct(FactOrder.customer_id)).label("customers"),
            )
            .where(
                and_(
                    FactOrder.order_timestamp >= datetime.combine(start_date, datetime.min.time()),
                    FactOrder.order_timestamp <= datetime.combine(end_date, datetime.max.time()),
                )
            )
            .group_by(func.date(FactOrder.order_timestamp))
            .order_by(func.date(FactOrder.order_timestamp))
        )
        
        data = [
            DailySalesData(
                date=row.date,
                revenue=float(row.revenue or 0),
                orders=row.orders,
                customers=row.customers,
            )
            for row in result.all()
        ]
        
        logger.info("Sales trend query completed", data_points=len(data))
        
        return SalesTrend(
            data=data,
            period_start=start_date,
            period_end=end_date,
            total_revenue=sum(d.revenue for d in data),
            total_orders=sum(d.orders for d in data),
        )
    except Exception as e:
        logger.error("Error in get_sales_trend", error=str(e), error_type=type(e).__name__)
        raise


@router.get("/sales/by-category", response_model=List[CategorySales])
async def get_sales_by_category(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: AsyncSession = Depends(get_db_dependency),
) -> List[CategorySales]:
    """
    Get sales breakdown by product category.
    """
    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    # This would join with order items and products
    # Simplified version using orders
    result = await db.execute(
        text("""
            SELECT 
                p.category,
                SUM(oi.line_total) as revenue,
                COUNT(DISTINCT o.order_id) as orders
            FROM fact_orders o
            JOIN fact_order_items oi ON o.order_id = oi.order_id
            JOIN dim_products p ON oi.product_id = p.product_id
            WHERE o.order_timestamp BETWEEN :start_date AND :end_date
            GROUP BY p.category
            ORDER BY revenue DESC
        """),
        {
            "start_date": datetime.combine(start_date, datetime.min.time()),
            "end_date": datetime.combine(end_date, datetime.max.time()),
        }
    )
    
    rows = result.all()
    total_revenue = sum(r.revenue for r in rows) if rows else 1
    
    return [
        CategorySales(
            category=row.category,
            revenue=float(row.revenue or 0),
            orders=row.orders,
            percentage=(row.revenue / total_revenue) * 100 if total_revenue > 0 else 0,
        )
        for row in rows
    ]


@router.get("/sales/hourly", response_model=List[HourlyDistribution])
async def get_hourly_distribution(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: AsyncSession = Depends(get_db_dependency),
) -> List[HourlyDistribution]:
    """
    Get order distribution by hour of day.
    """
    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    result = await db.execute(
        select(
            func.extract("hour", FactOrder.order_timestamp).label("hour"),
            func.count(FactOrder.order_id).label("orders"),
            func.sum(FactOrder.total_amount).label("revenue"),
        )
        .where(
            and_(
                FactOrder.order_timestamp >= datetime.combine(start_date, datetime.min.time()),
                FactOrder.order_timestamp <= datetime.combine(end_date, datetime.max.time()),
            )
        )
        .group_by(func.extract("hour", FactOrder.order_timestamp))
        .order_by(func.extract("hour", FactOrder.order_timestamp))
    )
    
    return [
        HourlyDistribution(
            hour=int(row.hour),
            orders=row.orders,
            revenue=float(row.revenue or 0),
        )
        for row in result.all()
    ]


@router.get("/customers/cohorts")
async def get_customer_cohorts(
    db: AsyncSession = Depends(get_db_dependency),
):
    """Get customer cohort analysis."""
    result = await db.execute(
        text("""
            SELECT 
                TO_CHAR(first_order_date, 'YYYY-MM') as cohort,
                COUNT(*) as customers,
                AVG(lifetime_value) as avg_ltv,
                AVG(total_orders) as avg_orders
            FROM dim_customers
            WHERE is_current = true AND first_order_date IS NOT NULL
            GROUP BY TO_CHAR(first_order_date, 'YYYY-MM')
            ORDER BY cohort DESC
            LIMIT 12
        """)
    )
    
    return [
        {
            "cohort": row.cohort,
            "customers": row.customers,
            "avg_ltv": float(row.avg_ltv or 0),
            "avg_orders": float(row.avg_orders or 0),
        }
        for row in result.all()
    ]


@router.get("/products/top-selling")
async def get_top_selling_products(
    limit: int = Query(10, ge=1, le=100),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: AsyncSession = Depends(get_db_dependency),
):
    """Get top selling products."""
    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    result = await db.execute(
        text("""
            SELECT 
                p.product_id,
                p.name,
                p.category,
                SUM(oi.quantity) as units_sold,
                SUM(oi.line_total) as revenue
            FROM fact_order_items oi
            JOIN dim_products p ON oi.product_id = p.product_id
            JOIN fact_orders o ON oi.order_id = o.order_id
            WHERE o.order_timestamp BETWEEN :start_date AND :end_date
            GROUP BY p.product_id, p.name, p.category
            ORDER BY revenue DESC
            LIMIT :limit
        """),
        {
            "start_date": datetime.combine(start_date, datetime.min.time()),
            "end_date": datetime.combine(end_date, datetime.max.time()),
            "limit": limit,
        }
    )
    
    return [
        {
            "product_id": str(row.product_id),
            "name": row.name,
            "category": row.category,
            "units_sold": row.units_sold,
            "revenue": float(row.revenue),
        }
        for row in result.all()
    ]
