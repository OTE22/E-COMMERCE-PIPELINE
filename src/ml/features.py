"""
ML Feature Engineering

Production feature engineering for:
- Customer churn prediction
- Customer lifetime value (CLV) prediction
- Product recommendation
- Demand forecasting
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import polars as pl
from scipy import stats
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class CustomerFeatures:
    """Customer-level features for ML models"""
    customer_id: str
    
    # RFM Features
    recency_days: float
    frequency: int
    monetary_total: float
    monetary_avg: float
    
    # Behavioral Features
    days_since_first_order: int
    order_count: int
    avg_days_between_orders: float
    order_frequency_trend: float  # Positive = increasing frequency
    
    # Monetary Features
    total_spend: float
    avg_order_value: float
    max_order_value: float
    min_order_value: float
    std_order_value: float
    
    # Product Features
    unique_products_purchased: int
    unique_categories_purchased: int
    favorite_category: Optional[str]
    
    # Time Features
    preferred_day_of_week: int
    preferred_hour: int
    is_weekend_shopper: bool
    
    # Engagement Features
    has_returned_items: bool
    return_rate: float
    uses_promotions: bool
    promo_order_rate: float


@dataclass
class ProductFeatures:
    """Product-level features for ML models"""
    product_id: str
    
    # Sales Features
    total_units_sold: int
    total_revenue: float
    unique_customers: int
    avg_quantity_per_order: float
    
    # Pricing Features
    current_price: float
    avg_selling_price: float
    price_variance: float
    
    # Performance Features
    conversion_rate: float
    cart_abandonment_rate: float
    return_rate: float
    
    # Time Features
    days_since_launch: int
    sales_velocity_7d: float
    sales_velocity_30d: float
    
    # Inventory Features
    current_stock: int
    days_of_supply: float
    stockout_frequency: int


class FeatureEngineer:
    """
    Production feature engineering pipeline.
    
    Generates ML-ready features from raw e-commerce data.
    """
    
    def __init__(self, reference_date: Optional[datetime] = None):
        self.reference_date = reference_date or datetime.utcnow()
    
    def compute_customer_features(
        self,
        orders_df: pl.DataFrame,
        order_items_df: Optional[pl.DataFrame] = None,
    ) -> pl.DataFrame:
        """
        Compute customer-level features from orders.
        
        Args:
            orders_df: Orders fact table
            order_items_df: Optional order items for product features
            
        Returns:
            DataFrame with customer features
        """
        logger.info("Computing customer features")
        
        # Basic aggregations
        features = orders_df.group_by("customer_id").agg([
            # Recency
            (pl.lit(self.reference_date) - pl.col("order_timestamp").max())
            .dt.total_days()
            .alias("recency_days"),
            
            # Frequency
            pl.count().alias("order_count"),
            
            # Monetary
            pl.col("total_amount").sum().alias("total_spend"),
            pl.col("total_amount").mean().alias("avg_order_value"),
            pl.col("total_amount").max().alias("max_order_value"),
            pl.col("total_amount").min().alias("min_order_value"),
            pl.col("total_amount").std().alias("std_order_value"),
            
            # Time
            pl.col("order_timestamp").min().alias("first_order_date"),
            pl.col("order_timestamp").max().alias("last_order_date"),
            
            # Items
            pl.col("item_count").sum().alias("total_items"),
            pl.col("item_count").mean().alias("avg_items_per_order"),
            
            # Promotions
            pl.col("has_promo_code").sum().alias("promo_order_count"),
            
            # First order flag
            pl.col("is_first_order").any().alias("is_new_customer"),
        ])
        
        # Calculate derived features
        features = features.with_columns([
            # Days since first order
            (pl.lit(self.reference_date) - pl.col("first_order_date"))
            .dt.total_days()
            .alias("tenure_days"),
            
            # Customer lifespan
            (pl.col("last_order_date") - pl.col("first_order_date"))
            .dt.total_days()
            .alias("lifespan_days"),
            
            # Promo usage rate
            (pl.col("promo_order_count") / pl.col("order_count"))
            .alias("promo_usage_rate"),
        ])
        
        # Purchase frequency
        features = features.with_columns(
            pl.when(pl.col("lifespan_days") > 0)
            .then(pl.col("order_count") / (pl.col("lifespan_days") / 30))
            .otherwise(pl.col("order_count"))
            .alias("orders_per_month")
        )
        
        # Average days between orders
        features = features.with_columns(
            pl.when(pl.col("order_count") > 1)
            .then(pl.col("lifespan_days") / (pl.col("order_count") - 1))
            .otherwise(pl.lit(None))
            .alias("avg_days_between_orders")
        )
        
        logger.info(f"Computed features for {len(features)} customers")
        return features
    
    def compute_product_features(
        self,
        order_items_df: pl.DataFrame,
        products_df: pl.DataFrame,
        page_views_df: Optional[pl.DataFrame] = None,
    ) -> pl.DataFrame:
        """
        Compute product-level features.
        
        Args:
            order_items_df: Order items fact table
            products_df: Products dimension table
            page_views_df: Optional page views for engagement features
            
        Returns:
            DataFrame with product features
        """
        logger.info("Computing product features")
        
        # Sales aggregations
        sales_features = order_items_df.group_by("product_id").agg([
            pl.col("quantity").sum().alias("total_units_sold"),
            pl.col("line_total").sum().alias("total_revenue"),
            pl.col("order_id").n_unique().alias("orders_count"),
            pl.col("quantity").mean().alias("avg_quantity_per_order"),
            pl.col("unit_price").mean().alias("avg_selling_price"),
            pl.col("unit_price").std().alias("price_variance"),
        ])
        
        # Join with product info
        features = products_df.join(
            sales_features,
            on="product_id",
            how="left",
        )
        
        # Fill nulls for products with no sales
        features = features.with_columns([
            pl.col("total_units_sold").fill_null(0),
            pl.col("total_revenue").fill_null(0),
            pl.col("orders_count").fill_null(0),
        ])
        
        # Calculate derived features
        if "launch_date" in features.columns:
            features = features.with_columns(
                (pl.lit(self.reference_date.date()) - pl.col("launch_date"))
                .dt.total_days()
                .alias("days_since_launch")
            )
        
        # Inventory features
        if "stock_quantity" in features.columns and "reorder_level" in features.columns:
            features = features.with_columns(
                pl.when(pl.col("total_units_sold") > 0)
                .then(
                    pl.col("stock_quantity") / 
                    (pl.col("total_units_sold") / 30)  # 30-day sales rate
                )
                .otherwise(pl.lit(999))
                .alias("days_of_supply")
            )
        
        logger.info(f"Computed features for {len(features)} products")
        return features
    
    def compute_churn_features(
        self,
        customer_features: pl.DataFrame,
        lookback_days: int = 90,
    ) -> pl.DataFrame:
        """
        Compute features specifically for churn prediction.
        
        Args:
            customer_features: Base customer features
            lookback_days: Days to consider for activity
            
        Returns:
            DataFrame with churn prediction features
        """
        features = customer_features.with_columns([
            # Churn risk indicators
            (pl.col("recency_days") > lookback_days).alias("is_inactive"),
            
            # Recency score (higher = more at risk)
            (pl.col("recency_days") / lookback_days).clip(0, 2).alias("recency_risk_score"),
            
            # Frequency score (lower = more at risk)
            (1 - (pl.col("orders_per_month") / 
                  pl.col("orders_per_month").quantile(0.9)).clip(0, 1))
            .alias("frequency_risk_score"),
            
            # Monetary score (lower = more at risk)
            (1 - (pl.col("total_spend") / 
                  pl.col("total_spend").quantile(0.9)).clip(0, 1))
            .alias("monetary_risk_score"),
        ])
        
        # Combined churn risk score
        features = features.with_columns(
            ((pl.col("recency_risk_score") * 0.5) + 
             (pl.col("frequency_risk_score") * 0.3) + 
             (pl.col("monetary_risk_score") * 0.2))
            .alias("churn_risk_score")
        )
        
        # Binary churn label (for training)
        features = features.with_columns(
            (pl.col("churn_risk_score") > 0.7).alias("is_churned")
        )
        
        return features
    
    def compute_clv_features(
        self,
        customer_features: pl.DataFrame,
        projection_months: int = 12,
    ) -> pl.DataFrame:
        """
        Compute features for CLV prediction.
        
        Args:
            customer_features: Base customer features
            projection_months: Months to project
            
        Returns:
            DataFrame with CLV features
        """
        features = customer_features.with_columns([
            # Historical CLV
            pl.col("total_spend").alias("historical_clv"),
            
            # Projected CLV (simple model)
            (pl.col("avg_order_value") * 
             pl.col("orders_per_month") * 
             projection_months)
            .alias("projected_clv"),
            
            # Revenue per day
            pl.when(pl.col("tenure_days") > 0)
            .then(pl.col("total_spend") / pl.col("tenure_days"))
            .otherwise(0)
            .alias("revenue_per_day"),
        ])
        
        # CLV quartile
        features = features.with_columns(
            pl.when(pl.col("historical_clv") >= pl.col("historical_clv").quantile(0.75))
            .then(pl.lit(4))
            .when(pl.col("historical_clv") >= pl.col("historical_clv").quantile(0.5))
            .then(pl.lit(3))
            .when(pl.col("historical_clv") >= pl.col("historical_clv").quantile(0.25))
            .then(pl.lit(2))
            .otherwise(pl.lit(1))
            .alias("clv_quartile")
        )
        
        return features
    
    def compute_recommendation_features(
        self,
        order_items_df: pl.DataFrame,
        products_df: pl.DataFrame,
    ) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """
        Compute features for product recommendations.
        
        Returns:
            Tuple of (user-item interactions, product similarity features)
        """
        # User-item interaction matrix
        interactions = order_items_df.group_by(["customer_id", "product_id"]).agg([
            pl.col("quantity").sum().alias("purchase_count"),
            pl.col("line_total").sum().alias("total_spent"),
        ])
        
        # Product co-purchase matrix (products bought together)
        # This is a simplified version - in production use more sophisticated methods
        
        # First, get all products per order
        order_products = order_items_df.group_by("order_id").agg([
            pl.col("product_id").alias("products"),
        ])
        
        logger.info("Computed recommendation features")
        return interactions, order_products


def create_ml_dataset(
    orders_df: pl.DataFrame,
    target: str = "churn",
    **kwargs,
) -> pl.DataFrame:
    """
    Create ML-ready dataset for a specific target.
    
    Args:
        orders_df: Orders fact table
        target: "churn", "clv", or "next_purchase"
        **kwargs: Additional parameters
        
    Returns:
        ML-ready feature DataFrame
    """
    engineer = FeatureEngineer()
    
    # Compute base features
    features = engineer.compute_customer_features(orders_df)
    
    if target == "churn":
        features = engineer.compute_churn_features(features, **kwargs)
    elif target == "clv":
        features = engineer.compute_clv_features(features, **kwargs)
    
    # Drop non-feature columns
    drop_cols = ["first_order_date", "last_order_date"]
    features = features.drop([c for c in drop_cols if c in features.columns])
    
    # Fill remaining nulls
    features = features.fill_null(0)
    
    return features
