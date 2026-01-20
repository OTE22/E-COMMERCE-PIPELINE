"""
Database Models - Star Schema Design

This module defines the production data models following a star schema design pattern
optimized for analytical workloads. The schema consists of:

Fact Tables:
- FactOrders: Order transactions with metrics
- FactPageViews: Clickstream/page view events
- FactInventorySnapshots: Daily inventory snapshots

Dimension Tables:
- DimCustomers: Customer attributes and segments
- DimProducts: Product catalog and categories
- DimDate: Date dimension with calendar attributes
- DimCampaigns: Marketing campaign attributes
- DimLocations: Geographic dimension
"""

from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import Optional, List
import uuid

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Enum as SQLEnum,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all database models"""
    pass


# =============================================================================
# ENUMERATIONS
# =============================================================================

class OrderStatus(str, Enum):
    """Order status enumeration"""
    PENDING = "pending"
    CONFIRMED = "confirmed"
    PROCESSING = "processing"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"


class PaymentStatus(str, Enum):
    """Payment status enumeration"""
    PENDING = "pending"
    AUTHORIZED = "authorized"
    CAPTURED = "captured"
    FAILED = "failed"
    REFUNDED = "refunded"


class CustomerSegment(str, Enum):
    """Customer segment enumeration"""
    NEW = "new"
    RETURNING = "returning"
    VIP = "vip"
    AT_RISK = "at_risk"
    CHURNED = "churned"


class ProductCategory(str, Enum):
    """Product category enumeration"""
    ELECTRONICS = "electronics"
    CLOTHING = "clothing"
    HOME_GARDEN = "home_garden"
    SPORTS = "sports"
    BEAUTY = "beauty"
    FOOD_BEVERAGE = "food_beverage"
    BOOKS = "books"
    TOYS = "toys"
    OTHER = "other"


# =============================================================================
# DIMENSION TABLES
# =============================================================================

class DimDate(Base):
    """
    Date Dimension Table
    
    Pre-populated calendar dimension for time-based analytics.
    Contains all date attributes for efficient time-series queries.
    """
    __tablename__ = "dim_date"
    
    date_key: Mapped[int] = mapped_column(Integer, primary_key=True)  # YYYYMMDD format
    full_date: Mapped[date] = mapped_column(Date, unique=True, nullable=False)
    day_of_week: Mapped[int] = mapped_column(Integer, nullable=False)  # 0=Monday
    day_of_month: Mapped[int] = mapped_column(Integer, nullable=False)
    day_of_year: Mapped[int] = mapped_column(Integer, nullable=False)
    week_of_year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)
    month_name: Mapped[str] = mapped_column(String(20), nullable=False)
    quarter: Mapped[int] = mapped_column(Integer, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    is_weekend: Mapped[bool] = mapped_column(Boolean, default=False)
    is_holiday: Mapped[bool] = mapped_column(Boolean, default=False)
    holiday_name: Mapped[Optional[str]] = mapped_column(String(100))
    fiscal_year: Mapped[int] = mapped_column(Integer, nullable=False)
    fiscal_quarter: Mapped[int] = mapped_column(Integer, nullable=False)
    
    __table_args__ = (
        Index("ix_dim_date_year_month", "year", "month"),
        Index("ix_dim_date_fiscal", "fiscal_year", "fiscal_quarter"),
    )


class DimCustomer(Base):
    """
    Customer Dimension Table
    
    Stores customer attributes and computed segments.
    Implements SCD Type 2 for tracking customer changes over time.
    """
    __tablename__ = "dim_customers"
    
    customer_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    customer_key: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    
    # Personal info (PII - should be encrypted/masked in production)
    email_hash: Mapped[str] = mapped_column(String(64), nullable=False)  # SHA256 hash
    first_name_masked: Mapped[Optional[str]] = mapped_column(String(100))
    last_name_masked: Mapped[Optional[str]] = mapped_column(String(100))
    
    # Demographics
    age_group: Mapped[Optional[str]] = mapped_column(String(20))
    gender: Mapped[Optional[str]] = mapped_column(String(20))
    
    # Geographic
    country: Mapped[Optional[str]] = mapped_column(String(100))
    state: Mapped[Optional[str]] = mapped_column(String(100))
    city: Mapped[Optional[str]] = mapped_column(String(100))
    postal_code: Mapped[Optional[str]] = mapped_column(String(20))
    
    # Customer metrics
    segment: Mapped[CustomerSegment] = mapped_column(
        SQLEnum(CustomerSegment), default=CustomerSegment.NEW
    )
    lifetime_value: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=0)
    total_orders: Mapped[int] = mapped_column(Integer, default=0)
    avg_order_value: Mapped[Decimal] = mapped_column(Numeric(10, 2), default=0)
    
    # RFM Scores (Recency, Frequency, Monetary)
    rfm_recency_score: Mapped[Optional[int]] = mapped_column(Integer)
    rfm_frequency_score: Mapped[Optional[int]] = mapped_column(Integer)
    rfm_monetary_score: Mapped[Optional[int]] = mapped_column(Integer)
    rfm_combined_score: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Churn prediction
    churn_probability: Mapped[Optional[float]] = mapped_column(Float)
    predicted_next_purchase_date: Mapped[Optional[date]] = mapped_column(Date)
    
    # Dates
    first_order_date: Mapped[Optional[date]] = mapped_column(Date)
    last_order_date: Mapped[Optional[date]] = mapped_column(Date)
    registration_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    # SCD Type 2 fields
    effective_from: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    effective_to: Mapped[Optional[datetime]] = mapped_column(DateTime)
    is_current: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Audit
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )
    
    # Relationships
    orders: Mapped[List["FactOrder"]] = relationship(back_populates="customer")
    
    __table_args__ = (
        Index("ix_dim_customers_segment", "segment"),
        Index("ix_dim_customers_country", "country"),
        Index("ix_dim_customers_rfm", "rfm_combined_score"),
        Index("ix_dim_customers_ltv", "lifetime_value"),
    )


class DimProduct(Base):
    """
    Product Dimension Table
    
    Stores product catalog with categories, pricing, and inventory attributes.
    """
    __tablename__ = "dim_products"
    
    product_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    product_key: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    sku: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    
    # Product details
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    brand: Mapped[Optional[str]] = mapped_column(String(100))
    
    # Categories
    category: Mapped[ProductCategory] = mapped_column(
        SQLEnum(ProductCategory), nullable=False
    )
    subcategory: Mapped[Optional[str]] = mapped_column(String(100))
    category_path: Mapped[Optional[str]] = mapped_column(String(500))  # e.g., "Electronics > Phones"
    
    # Pricing
    unit_price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    cost_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    margin_percent: Mapped[Optional[float]] = mapped_column(Float)
    
    # Inventory
    stock_quantity: Mapped[int] = mapped_column(Integer, default=0)
    reorder_level: Mapped[int] = mapped_column(Integer, default=10)
    is_in_stock: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Attributes
    weight_kg: Mapped[Optional[float]] = mapped_column(Float)
    dimensions: Mapped[Optional[dict]] = mapped_column(JSONB)  # {"length", "width", "height"}
    color: Mapped[Optional[str]] = mapped_column(String(50))
    size: Mapped[Optional[str]] = mapped_column(String(50))
    
    # Ratings
    avg_rating: Mapped[Optional[float]] = mapped_column(Float)
    review_count: Mapped[int] = mapped_column(Integer, default=0)
    
    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    launch_date: Mapped[Optional[date]] = mapped_column(Date)
    discontinue_date: Mapped[Optional[date]] = mapped_column(Date)
    
    # Tags and metadata
    tags: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String(50)))
    attributes: Mapped[Optional[dict]] = mapped_column(JSONB)
    
    # Audit
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )
    
    # Relationships
    order_items: Mapped[List["FactOrderItem"]] = relationship(back_populates="product")
    
    __table_args__ = (
        Index("ix_dim_products_category", "category"),
        Index("ix_dim_products_brand", "brand"),
        Index("ix_dim_products_active", "is_active"),
        Index("ix_dim_products_stock", "is_in_stock"),
    )


class DimCampaign(Base):
    """
    Marketing Campaign Dimension Table
    
    Stores marketing campaign attributes for attribution analysis.
    """
    __tablename__ = "dim_campaigns"
    
    campaign_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    campaign_key: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    
    # Campaign details
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    
    # Channel
    channel: Mapped[str] = mapped_column(String(50), nullable=False)  # email, social, ppc, etc.
    source: Mapped[Optional[str]] = mapped_column(String(100))  # google, facebook, etc.
    medium: Mapped[Optional[str]] = mapped_column(String(100))  # cpc, organic, referral
    
    # Budget and performance
    budget: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))
    spend: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=0)
    impressions: Mapped[int] = mapped_column(Integer, default=0)
    clicks: Mapped[int] = mapped_column(Integer, default=0)
    conversions: Mapped[int] = mapped_column(Integer, default=0)
    
    # Computed metrics
    ctr: Mapped[Optional[float]] = mapped_column(Float)  # Click-through rate
    cpc: Mapped[Optional[float]] = mapped_column(Float)  # Cost per click
    cpa: Mapped[Optional[float]] = mapped_column(Float)  # Cost per acquisition
    roas: Mapped[Optional[float]] = mapped_column(Float)  # Return on ad spend
    
    # Dates
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[Optional[date]] = mapped_column(Date)
    
    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Audit
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )
    
    __table_args__ = (
        Index("ix_dim_campaigns_channel", "channel"),
        Index("ix_dim_campaigns_dates", "start_date", "end_date"),
        Index("ix_dim_campaigns_active", "is_active"),
    )


class DimLocation(Base):
    """
    Location Dimension Table
    
    Geographic dimension for shipment and customer analytics.
    """
    __tablename__ = "dim_locations"
    
    location_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    location_key: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    
    # Geographic hierarchy
    country_code: Mapped[str] = mapped_column(String(3), nullable=False)
    country_name: Mapped[str] = mapped_column(String(100), nullable=False)
    state_code: Mapped[Optional[str]] = mapped_column(String(10))
    state_name: Mapped[Optional[str]] = mapped_column(String(100))
    city: Mapped[Optional[str]] = mapped_column(String(100))
    postal_code: Mapped[Optional[str]] = mapped_column(String(20))
    
    # Coordinates
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    
    # Region
    region: Mapped[Optional[str]] = mapped_column(String(50))  # e.g., North America, EMEA
    timezone: Mapped[Optional[str]] = mapped_column(String(50))
    
    __table_args__ = (
        Index("ix_dim_locations_country", "country_code"),
        Index("ix_dim_locations_region", "region"),
    )


# =============================================================================
# FACT TABLES
# =============================================================================

class FactOrder(Base):
    """
    Order Fact Table
    
    Central fact table for order transactions with grain at order level.
    Contains keys to all relevant dimensions and additive measures.
    """
    __tablename__ = "fact_orders"
    
    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    order_number: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    
    # Dimension foreign keys
    customer_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_customers.customer_id"), nullable=False
    )
    order_date_key: Mapped[int] = mapped_column(
        Integer, ForeignKey("dim_date.date_key"), nullable=False
    )
    ship_date_key: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("dim_date.date_key")
    )
    shipping_location_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_locations.location_id")
    )
    campaign_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_campaigns.campaign_id")
    )
    
    # Order status
    status: Mapped[OrderStatus] = mapped_column(
        SQLEnum(OrderStatus), default=OrderStatus.PENDING
    )
    payment_status: Mapped[PaymentStatus] = mapped_column(
        SQLEnum(PaymentStatus), default=PaymentStatus.PENDING
    )
    
    # Timestamps
    order_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    payment_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime)
    ship_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime)
    delivery_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    # Measures - Additive metrics
    item_count: Mapped[int] = mapped_column(Integer, nullable=False)
    subtotal: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    discount_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), default=0)
    tax_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), default=0)
    shipping_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), default=0)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    
    # Cost and margin
    total_cost: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))
    gross_margin: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))
    
    # Payment
    payment_method: Mapped[Optional[str]] = mapped_column(String(50))
    currency_code: Mapped[str] = mapped_column(String(3), default="USD")
    
    # Shipping
    shipping_method: Mapped[Optional[str]] = mapped_column(String(50))
    tracking_number: Mapped[Optional[str]] = mapped_column(String(100))
    
    # Attribution
    utm_source: Mapped[Optional[str]] = mapped_column(String(100))
    utm_medium: Mapped[Optional[str]] = mapped_column(String(100))
    utm_campaign: Mapped[Optional[str]] = mapped_column(String(100))
    device_type: Mapped[Optional[str]] = mapped_column(String(50))
    
    # Flags
    is_first_order: Mapped[bool] = mapped_column(Boolean, default=False)
    is_gift: Mapped[bool] = mapped_column(Boolean, default=False)
    has_promo_code: Mapped[bool] = mapped_column(Boolean, default=False)
    promo_code: Mapped[Optional[str]] = mapped_column(String(50))
    
    # Audit
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )
    
    # Relationships
    customer: Mapped["DimCustomer"] = relationship(back_populates="orders")
    items: Mapped[List["FactOrderItem"]] = relationship(back_populates="order")
    
    __table_args__ = (
        Index("ix_fact_orders_customer", "customer_id"),
        Index("ix_fact_orders_date", "order_date_key"),
        Index("ix_fact_orders_status", "status"),
        Index("ix_fact_orders_timestamp", "order_timestamp"),
    )


class FactOrderItem(Base):
    """
    Order Item Fact Table
    
    Line-item detail for orders with grain at order-item level.
    """
    __tablename__ = "fact_order_items"
    
    order_item_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    
    # Foreign keys
    order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("fact_orders.order_id"), nullable=False
    )
    product_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_products.product_id"), nullable=False
    )
    
    # Measures
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    discount_percent: Mapped[float] = mapped_column(Float, default=0)
    discount_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2), default=0)
    line_total: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    
    # Cost
    unit_cost: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    line_cost: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))
    line_margin: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))
    
    # Audit
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    
    # Relationships
    order: Mapped["FactOrder"] = relationship(back_populates="items")
    product: Mapped["DimProduct"] = relationship(back_populates="order_items")
    
    __table_args__ = (
        Index("ix_fact_order_items_order", "order_id"),
        Index("ix_fact_order_items_product", "product_id"),
    )


class FactPageView(Base):
    """
    Page View Fact Table
    
    Clickstream analytics fact table with grain at page view level.
    Used for funnel analysis, session analytics, and attribution.
    """
    __tablename__ = "fact_page_views"
    
    page_view_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    
    # Session and visitor
    session_id: Mapped[str] = mapped_column(String(100), nullable=False)
    visitor_id: Mapped[str] = mapped_column(String(100), nullable=False)
    customer_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_customers.customer_id")
    )
    
    # Dimension keys
    date_key: Mapped[int] = mapped_column(
        Integer, ForeignKey("dim_date.date_key"), nullable=False
    )
    
    # Timestamp
    event_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    
    # Page details
    page_url: Mapped[str] = mapped_column(String(2000), nullable=False)
    page_path: Mapped[str] = mapped_column(String(500), nullable=False)
    page_title: Mapped[Optional[str]] = mapped_column(String(500))
    page_type: Mapped[Optional[str]] = mapped_column(String(50))  # home, product, cart, checkout
    
    # Referrer
    referrer_url: Mapped[Optional[str]] = mapped_column(String(2000))
    referrer_domain: Mapped[Optional[str]] = mapped_column(String(200))
    
    # UTM parameters
    utm_source: Mapped[Optional[str]] = mapped_column(String(100))
    utm_medium: Mapped[Optional[str]] = mapped_column(String(100))
    utm_campaign: Mapped[Optional[str]] = mapped_column(String(100))
    utm_content: Mapped[Optional[str]] = mapped_column(String(100))
    utm_term: Mapped[Optional[str]] = mapped_column(String(100))
    
    # Device and browser
    device_type: Mapped[Optional[str]] = mapped_column(String(20))  # desktop, mobile, tablet
    browser: Mapped[Optional[str]] = mapped_column(String(50))
    os: Mapped[Optional[str]] = mapped_column(String(50))
    screen_resolution: Mapped[Optional[str]] = mapped_column(String(20))
    
    # Engagement metrics
    time_on_page_seconds: Mapped[Optional[int]] = mapped_column(Integer)
    scroll_depth_percent: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Product interaction (if on product page)
    product_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_products.product_id")
    )
    
    # Events
    event_type: Mapped[str] = mapped_column(String(50), default="page_view")
    event_category: Mapped[Optional[str]] = mapped_column(String(50))
    event_action: Mapped[Optional[str]] = mapped_column(String(50))
    event_label: Mapped[Optional[str]] = mapped_column(String(200))
    event_value: Mapped[Optional[float]] = mapped_column(Float)
    
    __table_args__ = (
        Index("ix_fact_page_views_session", "session_id"),
        Index("ix_fact_page_views_visitor", "visitor_id"),
        Index("ix_fact_page_views_customer", "customer_id"),
        Index("ix_fact_page_views_date", "date_key"),
        Index("ix_fact_page_views_timestamp", "event_timestamp"),
        Index("ix_fact_page_views_page_type", "page_type"),
    )


class FactInventorySnapshot(Base):
    """
    Inventory Snapshot Fact Table
    
    Daily snapshots of inventory levels for trend analysis.
    Grain: one row per product per day.
    """
    __tablename__ = "fact_inventory_snapshots"
    
    snapshot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    
    # Keys
    product_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_products.product_id"), nullable=False
    )
    date_key: Mapped[int] = mapped_column(
        Integer, ForeignKey("dim_date.date_key"), nullable=False
    )
    
    # Inventory measures
    quantity_on_hand: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity_reserved: Mapped[int] = mapped_column(Integer, default=0)
    quantity_available: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity_on_order: Mapped[int] = mapped_column(Integer, default=0)
    
    # Stock status
    is_in_stock: Mapped[bool] = mapped_column(Boolean, default=True)
    days_of_supply: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Movement
    units_sold: Mapped[int] = mapped_column(Integer, default=0)
    units_received: Mapped[int] = mapped_column(Integer, default=0)
    units_returned: Mapped[int] = mapped_column(Integer, default=0)
    
    # Value
    inventory_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 2))
    
    # Audit
    snapshot_timestamp: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    
    __table_args__ = (
        UniqueConstraint("product_id", "date_key", name="uq_inventory_product_date"),
        Index("ix_fact_inventory_product", "product_id"),
        Index("ix_fact_inventory_date", "date_key"),
    )


# =============================================================================
# ANALYTICS AGGREGATES
# =============================================================================

class AggDailySales(Base):
    """
    Daily Sales Aggregate Table
    
    Pre-computed daily sales metrics for fast dashboard queries.
    Updated by ETL pipeline.
    """
    __tablename__ = "agg_daily_sales"
    
    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    date_key: Mapped[int] = mapped_column(
        Integer, ForeignKey("dim_date.date_key"), nullable=False
    )
    
    # Measures
    total_orders: Mapped[int] = mapped_column(Integer, default=0)
    total_revenue: Mapped[Decimal] = mapped_column(Numeric(14, 2), default=0)
    total_items_sold: Mapped[int] = mapped_column(Integer, default=0)
    total_discount: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=0)
    total_shipping: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=0)
    
    # Averages
    avg_order_value: Mapped[Decimal] = mapped_column(Numeric(10, 2), default=0)
    avg_items_per_order: Mapped[float] = mapped_column(Float, default=0)
    
    # Customer metrics
    unique_customers: Mapped[int] = mapped_column(Integer, default=0)
    new_customers: Mapped[int] = mapped_column(Integer, default=0)
    returning_customers: Mapped[int] = mapped_column(Integer, default=0)
    
    # Traffic metrics
    total_sessions: Mapped[int] = mapped_column(Integer, default=0)
    total_page_views: Mapped[int] = mapped_column(Integer, default=0)
    conversion_rate: Mapped[float] = mapped_column(Float, default=0)
    
    # Audit
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    
    __table_args__ = (
        UniqueConstraint("date_key", name="uq_agg_daily_sales_date"),
        Index("ix_agg_daily_sales_date", "date_key"),
    )


class AggProductPerformance(Base):
    """
    Product Performance Aggregate Table
    
    Pre-computed product metrics for product analytics.
    """
    __tablename__ = "agg_product_performance"
    
    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    product_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dim_products.product_id"), nullable=False
    )
    date_key: Mapped[int] = mapped_column(
        Integer, ForeignKey("dim_date.date_key"), nullable=False
    )
    
    # Sales measures
    units_sold: Mapped[int] = mapped_column(Integer, default=0)
    revenue: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=0)
    orders_count: Mapped[int] = mapped_column(Integer, default=0)
    
    # Engagement
    page_views: Mapped[int] = mapped_column(Integer, default=0)
    add_to_cart_count: Mapped[int] = mapped_column(Integer, default=0)
    conversion_rate: Mapped[float] = mapped_column(Float, default=0)
    
    # Audit
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    
    __table_args__ = (
        UniqueConstraint("product_id", "date_key", name="uq_agg_product_perf_date"),
        Index("ix_agg_product_perf_product", "product_id"),
        Index("ix_agg_product_perf_date", "date_key"),
    )
