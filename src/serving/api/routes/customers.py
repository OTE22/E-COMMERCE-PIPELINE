"""
Customers API Endpoints

REST API for customer data and analytics.
"""

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from src.database.connection import get_db_dependency
from src.database.models import DimCustomer, CustomerSegment
from src.serving.cache import customers_cache

router = APIRouter()
logger = structlog.get_logger(__name__)

logger.info("Customers router initialized")


class CustomerSummary(BaseModel):
    """Customer summary response"""
    customer_id: UUID
    customer_key: str
    segment: str
    lifetime_value: float
    total_orders: int
    country: Optional[str]
    
    class Config:
        from_attributes = True


class CustomerDetail(CustomerSummary):
    """Detailed customer response"""
    avg_order_value: float
    rfm_recency_score: Optional[int]
    rfm_frequency_score: Optional[int]
    rfm_monetary_score: Optional[int]
    rfm_combined_score: Optional[int]
    churn_probability: Optional[float]
    first_order_date: Optional[str]
    last_order_date: Optional[str]


class CustomerListResponse(BaseModel):
    """Paginated customer list"""
    items: List[CustomerSummary]
    total: int
    page: int
    page_size: int


class CustomerMetrics(BaseModel):
    """Customer base metrics"""
    total_customers: int
    new_customers: int
    vip_customers: int
    at_risk_customers: int
    avg_lifetime_value: float


@router.get("", response_model=CustomerListResponse)
async def list_customers(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    segment: Optional[str] = None,
    country: Optional[str] = None,
    min_ltv: Optional[float] = None,
    db: AsyncSession = Depends(get_db_dependency),
) -> CustomerListResponse:
    """List customers with filtering."""
    logger.info(
        "list_customers called",
        page=page,
        page_size=page_size,
        segment=segment,
        country=country,
        min_ltv=min_ltv,
    )
    
    try:
        query = select(DimCustomer).where(DimCustomer.is_current == True)
        count_query = select(func.count(DimCustomer.customer_id)).where(DimCustomer.is_current == True)
        
        conditions = []
        if segment:
            conditions.append(DimCustomer.segment == segment)
        if country:
            conditions.append(DimCustomer.country == country)
        if min_ltv:
            conditions.append(DimCustomer.lifetime_value >= min_ltv)
        
        if conditions:
            query = query.where(and_(*conditions))
            count_query = count_query.where(and_(*conditions))
        
        total = (await db.execute(count_query)).scalar() or 0
        logger.debug("Customers count query completed", total=total)
        
        offset = (page - 1) * page_size
        query = query.order_by(DimCustomer.lifetime_value.desc()).offset(offset).limit(page_size)
        
        result = await db.execute(query)
        customers = result.scalars().all()
        
        logger.info("Customers retrieved successfully", count=len(customers), total=total)
        
        return CustomerListResponse(
            items=[CustomerSummary.model_validate(c) for c in customers],
            total=total,
            page=page,
            page_size=page_size,
        )
    except Exception as e:
        logger.error("Error in list_customers", error=str(e), error_type=type(e).__name__)
        raise


@router.get("/metrics", response_model=CustomerMetrics)
async def get_customer_metrics(
    db: AsyncSession = Depends(get_db_dependency),
) -> CustomerMetrics:
    """Get customer base metrics."""
    cached = await customers_cache.get("metrics")
    if cached:
        return CustomerMetrics(**cached)
    
    total = (await db.execute(
        select(func.count(DimCustomer.customer_id)).where(DimCustomer.is_current == True)
    )).scalar() or 0
    
    new = (await db.execute(
        select(func.count(DimCustomer.customer_id)).where(
            and_(DimCustomer.is_current == True, DimCustomer.segment == "new")
        )
    )).scalar() or 0
    
    vip = (await db.execute(
        select(func.count(DimCustomer.customer_id)).where(
            and_(DimCustomer.is_current == True, DimCustomer.segment == "vip")
        )
    )).scalar() or 0
    
    at_risk = (await db.execute(
        select(func.count(DimCustomer.customer_id)).where(
            and_(DimCustomer.is_current == True, DimCustomer.segment == "at_risk")
        )
    )).scalar() or 0
    
    avg_ltv = (await db.execute(
        select(func.avg(DimCustomer.lifetime_value)).where(DimCustomer.is_current == True)
    )).scalar() or 0
    
    metrics = CustomerMetrics(
        total_customers=total,
        new_customers=new,
        vip_customers=vip,
        at_risk_customers=at_risk,
        avg_lifetime_value=float(avg_ltv),
    )
    
    await customers_cache.set("metrics", metrics.model_dump())
    return metrics


@router.get("/segments")
async def get_segment_distribution(
    db: AsyncSession = Depends(get_db_dependency),
):
    """Get customer segment distribution."""
    result = await db.execute(
        select(
            DimCustomer.segment,
            func.count(DimCustomer.customer_id).label("count"),
            func.avg(DimCustomer.lifetime_value).label("avg_ltv"),
        )
        .where(DimCustomer.is_current == True)
        .group_by(DimCustomer.segment)
    )
    
    return [
        {
            "segment": row.segment,
            "count": row.count,
            "avg_ltv": float(row.avg_ltv or 0),
        }
        for row in result.all()
    ]


@router.get("/top-spenders")
async def get_top_spenders(
    limit: int = Query(10, ge=1, le=100),
    db: AsyncSession = Depends(get_db_dependency),
):
    """Get top customers by lifetime value."""
    result = await db.execute(
        select(DimCustomer)
        .where(DimCustomer.is_current == True)
        .order_by(DimCustomer.lifetime_value.desc())
        .limit(limit)
    )
    
    return [CustomerSummary.model_validate(c) for c in result.scalars().all()]


@router.get("/at-risk")
async def get_at_risk_customers(
    db: AsyncSession = Depends(get_db_dependency),
):
    """Get customers at risk of churning."""
    result = await db.execute(
        select(DimCustomer)
        .where(
            and_(
                DimCustomer.is_current == True,
                DimCustomer.segment == "at_risk",
            )
        )
        .order_by(DimCustomer.churn_probability.desc())
        .limit(100)
    )
    
    return [CustomerDetail.model_validate(c) for c in result.scalars().all()]


@router.get("/{customer_id}", response_model=CustomerDetail)
async def get_customer(
    customer_id: UUID,
    db: AsyncSession = Depends(get_db_dependency),
) -> CustomerDetail:
    """Get customer details."""
    cached = await customers_cache.get(str(customer_id))
    if cached:
        return CustomerDetail(**cached)
    
    result = await db.execute(
        select(DimCustomer).where(
            and_(
                DimCustomer.customer_id == customer_id,
                DimCustomer.is_current == True,
            )
        )
    )
    customer = result.scalar_one_or_none()
    
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    
    detail = CustomerDetail.model_validate(customer)
    await customers_cache.set(str(customer_id), detail.model_dump())
    
    return detail
