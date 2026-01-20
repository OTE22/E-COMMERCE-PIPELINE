"""
GraphQL API

Strawberry GraphQL implementation for flexible data queries.
"""

from datetime import datetime, date
from decimal import Decimal
from typing import List, Optional
from uuid import UUID

import strawberry
from strawberry.fastapi import GraphQLRouter
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.connection import get_db
from src.database.models import FactOrder, DimCustomer, DimProduct


# =============================================================================
# TYPES
# =============================================================================

@strawberry.type
class Order:
    order_id: strawberry.ID
    order_number: str
    customer_id: strawberry.ID
    status: str
    total_amount: float
    item_count: int
    order_timestamp: datetime
    
    @strawberry.field
    async def customer(self) -> Optional["Customer"]:
        """Resolve customer for this order"""
        async with get_db() as db:
            result = await db.execute(
                select(DimCustomer).where(
                    DimCustomer.customer_id == UUID(self.customer_id)
                )
            )
            customer = result.scalar_one_or_none()
            if customer:
                return Customer(
                    customer_id=strawberry.ID(str(customer.customer_id)),
                    customer_key=customer.customer_key,
                    segment=customer.segment.value if customer.segment else "new",
                    lifetime_value=float(customer.lifetime_value or 0),
                    total_orders=customer.total_orders or 0,
                    country=customer.country,
                )
        return None


@strawberry.type
class Customer:
    customer_id: strawberry.ID
    customer_key: str
    segment: str
    lifetime_value: float
    total_orders: int
    country: Optional[str]
    
    @strawberry.field
    async def orders(
        self,
        limit: int = 10,
        offset: int = 0,
    ) -> List[Order]:
        """Resolve orders for this customer"""
        async with get_db() as db:
            result = await db.execute(
                select(FactOrder)
                .where(FactOrder.customer_id == UUID(self.customer_id))
                .order_by(FactOrder.order_timestamp.desc())
                .offset(offset)
                .limit(limit)
            )
            orders = result.scalars().all()
            return [
                Order(
                    order_id=strawberry.ID(str(o.order_id)),
                    order_number=o.order_number,
                    customer_id=strawberry.ID(str(o.customer_id)),
                    status=o.status.value if o.status else "pending",
                    total_amount=float(o.total_amount or 0),
                    item_count=o.item_count or 0,
                    order_timestamp=o.order_timestamp,
                )
                for o in orders
            ]


@strawberry.type
class Product:
    product_id: strawberry.ID
    sku: str
    name: str
    category: str
    unit_price: float
    stock_quantity: int
    is_in_stock: bool
    avg_rating: Optional[float]


@strawberry.type
class SalesMetrics:
    total_revenue: float
    total_orders: int
    avg_order_value: float
    unique_customers: int
    period_start: date
    period_end: date


@strawberry.type
class CategoryRevenue:
    category: str
    revenue: float
    orders: int
    percentage: float


# =============================================================================
# INPUTS
# =============================================================================

@strawberry.input
class DateRangeInput:
    start_date: Optional[date] = None
    end_date: Optional[date] = None


@strawberry.input
class OrderFilterInput:
    status: Optional[str] = None
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None
    customer_id: Optional[strawberry.ID] = None


@strawberry.input
class PaginationInput:
    limit: int = 50
    offset: int = 0


# =============================================================================
# QUERIES
# =============================================================================

@strawberry.type
class Query:
    
    @strawberry.field
    async def orders(
        self,
        pagination: Optional[PaginationInput] = None,
        filter: Optional[OrderFilterInput] = None,
        date_range: Optional[DateRangeInput] = None,
    ) -> List[Order]:
        """Query orders with filtering and pagination"""
        if pagination is None:
            pagination = PaginationInput()
        
        async with get_db() as db:
            query = select(FactOrder)
            conditions = []
            
            if filter:
                if filter.status:
                    conditions.append(FactOrder.status == filter.status)
                if filter.min_amount:
                    conditions.append(FactOrder.total_amount >= filter.min_amount)
                if filter.max_amount:
                    conditions.append(FactOrder.total_amount <= filter.max_amount)
                if filter.customer_id:
                    conditions.append(FactOrder.customer_id == UUID(filter.customer_id))
            
            if date_range:
                if date_range.start_date:
                    conditions.append(FactOrder.order_timestamp >= datetime.combine(
                        date_range.start_date, datetime.min.time()
                    ))
                if date_range.end_date:
                    conditions.append(FactOrder.order_timestamp <= datetime.combine(
                        date_range.end_date, datetime.max.time()
                    ))
            
            if conditions:
                query = query.where(and_(*conditions))
            
            query = query.order_by(FactOrder.order_timestamp.desc())
            query = query.offset(pagination.offset).limit(pagination.limit)
            
            result = await db.execute(query)
            orders = result.scalars().all()
            
            return [
                Order(
                    order_id=strawberry.ID(str(o.order_id)),
                    order_number=o.order_number,
                    customer_id=strawberry.ID(str(o.customer_id)),
                    status=o.status.value if o.status else "pending",
                    total_amount=float(o.total_amount or 0),
                    item_count=o.item_count or 0,
                    order_timestamp=o.order_timestamp,
                )
                for o in orders
            ]
    
    @strawberry.field
    async def order(self, order_id: strawberry.ID) -> Optional[Order]:
        """Get single order by ID"""
        async with get_db() as db:
            result = await db.execute(
                select(FactOrder).where(FactOrder.order_id == UUID(order_id))
            )
            o = result.scalar_one_or_none()
            if o:
                return Order(
                    order_id=strawberry.ID(str(o.order_id)),
                    order_number=o.order_number,
                    customer_id=strawberry.ID(str(o.customer_id)),
                    status=o.status.value if o.status else "pending",
                    total_amount=float(o.total_amount or 0),
                    item_count=o.item_count or 0,
                    order_timestamp=o.order_timestamp,
                )
        return None
    
    @strawberry.field
    async def customers(
        self,
        pagination: Optional[PaginationInput] = None,
        segment: Optional[str] = None,
    ) -> List[Customer]:
        """Query customers"""
        if pagination is None:
            pagination = PaginationInput()
        
        async with get_db() as db:
            query = select(DimCustomer).where(DimCustomer.is_current == True)
            
            if segment:
                query = query.where(DimCustomer.segment == segment)
            
            query = query.order_by(DimCustomer.lifetime_value.desc())
            query = query.offset(pagination.offset).limit(pagination.limit)
            
            result = await db.execute(query)
            customers = result.scalars().all()
            
            return [
                Customer(
                    customer_id=strawberry.ID(str(c.customer_id)),
                    customer_key=c.customer_key,
                    segment=c.segment.value if c.segment else "new",
                    lifetime_value=float(c.lifetime_value or 0),
                    total_orders=c.total_orders or 0,
                    country=c.country,
                )
                for c in customers
            ]
    
    @strawberry.field
    async def customer(self, customer_id: strawberry.ID) -> Optional[Customer]:
        """Get single customer by ID"""
        async with get_db() as db:
            result = await db.execute(
                select(DimCustomer).where(
                    and_(
                        DimCustomer.customer_id == UUID(customer_id),
                        DimCustomer.is_current == True,
                    )
                )
            )
            c = result.scalar_one_or_none()
            if c:
                return Customer(
                    customer_id=strawberry.ID(str(c.customer_id)),
                    customer_key=c.customer_key,
                    segment=c.segment.value if c.segment else "new",
                    lifetime_value=float(c.lifetime_value or 0),
                    total_orders=c.total_orders or 0,
                    country=c.country,
                )
        return None
    
    @strawberry.field
    async def products(
        self,
        pagination: Optional[PaginationInput] = None,
        category: Optional[str] = None,
        in_stock_only: bool = False,
    ) -> List[Product]:
        """Query products"""
        if pagination is None:
            pagination = PaginationInput()
        
        async with get_db() as db:
            query = select(DimProduct).where(DimProduct.is_active == True)
            
            if category:
                query = query.where(DimProduct.category == category)
            if in_stock_only:
                query = query.where(DimProduct.is_in_stock == True)
            
            query = query.order_by(DimProduct.name)
            query = query.offset(pagination.offset).limit(pagination.limit)
            
            result = await db.execute(query)
            products = result.scalars().all()
            
            return [
                Product(
                    product_id=strawberry.ID(str(p.product_id)),
                    sku=p.sku,
                    name=p.name,
                    category=p.category.value if p.category else "other",
                    unit_price=float(p.unit_price or 0),
                    stock_quantity=p.stock_quantity or 0,
                    is_in_stock=p.is_in_stock,
                    avg_rating=p.avg_rating,
                )
                for p in products
            ]
    
    @strawberry.field
    async def sales_metrics(
        self,
        date_range: Optional[DateRangeInput] = None,
    ) -> SalesMetrics:
        """Get sales metrics for a period"""
        end = date_range.end_date if date_range and date_range.end_date else date.today()
        start = date_range.start_date if date_range and date_range.start_date else end - timedelta(days=30)
        
        from datetime import timedelta
        
        async with get_db() as db:
            result = await db.execute(
                select(
                    func.sum(FactOrder.total_amount).label("revenue"),
                    func.count(FactOrder.order_id).label("orders"),
                    func.avg(FactOrder.total_amount).label("aov"),
                    func.count(func.distinct(FactOrder.customer_id)).label("customers"),
                ).where(
                    and_(
                        FactOrder.order_timestamp >= datetime.combine(start, datetime.min.time()),
                        FactOrder.order_timestamp <= datetime.combine(end, datetime.max.time()),
                    )
                )
            )
            row = result.one()
            
            return SalesMetrics(
                total_revenue=float(row.revenue or 0),
                total_orders=row.orders or 0,
                avg_order_value=float(row.aov or 0),
                unique_customers=row.customers or 0,
                period_start=start,
                period_end=end,
            )


# =============================================================================
# SCHEMA & ROUTER
# =============================================================================

schema = strawberry.Schema(query=Query)

graphql_app = GraphQLRouter(
    schema,
    path="",
    graphiql=True,
)
