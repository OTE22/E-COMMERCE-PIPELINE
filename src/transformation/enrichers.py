"""
Data Enrichment Module

Enriches e-commerce data with derived attributes, lookups, and ML features.
Includes:
- Customer RFM scoring
- Customer lifetime value calculation
- Product affinity features
- Geographic enrichment
- Session attribution
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import hashlib

import polars as pl
import numpy as np
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class RFMScores:
    """RFM scoring results"""
    recency_score: int  # 1-5, 5 = most recent
    frequency_score: int  # 1-5, 5 = most frequent
    monetary_score: int  # 1-5, 5 = highest value
    combined_score: int  # Sum of R+F+M
    segment: str  # Customer segment label


class DataEnricher:
    """
    Production data enricher for customer and order data.
    
    Adds derived attributes, calculates metrics, and applies
    machine learning feature engineering.
    """
    
    def __init__(self, reference_date: Optional[datetime] = None):
        self.reference_date = reference_date or datetime.utcnow()
    
    def calculate_rfm_scores(
        self,
        df: pl.DataFrame,
        customer_id_col: str = "customer_id",
        order_date_col: str = "order_date",
        amount_col: str = "total_amount",
    ) -> pl.DataFrame:
        """
        Calculate RFM (Recency, Frequency, Monetary) scores for customers.
        
        Args:
            df: Orders DataFrame
            customer_id_col: Customer ID column
            order_date_col: Order date column
            amount_col: Order amount column
            
        Returns:
            DataFrame with customer RFM scores
        """
        # Aggregate orders by customer
        rfm = df.group_by(customer_id_col).agg([
            # Recency: days since last order
            (pl.lit(self.reference_date) - pl.col(order_date_col).max())
            .dt.total_days()
            .alias("recency_days"),
            # Frequency: number of orders
            pl.count().alias("frequency"),
            # Monetary: total spend
            pl.col(amount_col).sum().alias("monetary"),
        ])
        
        # Calculate quintile scores (1-5)
        rfm = rfm.with_columns([
            # Recency score (lower days = higher score)
            pl.when(pl.col("recency_days") <= pl.col("recency_days").quantile(0.2))
            .then(5)
            .when(pl.col("recency_days") <= pl.col("recency_days").quantile(0.4))
            .then(4)
            .when(pl.col("recency_days") <= pl.col("recency_days").quantile(0.6))
            .then(3)
            .when(pl.col("recency_days") <= pl.col("recency_days").quantile(0.8))
            .then(2)
            .otherwise(1)
            .alias("rfm_recency_score"),
            
            # Frequency score (higher = better)
            pl.when(pl.col("frequency") >= pl.col("frequency").quantile(0.8))
            .then(5)
            .when(pl.col("frequency") >= pl.col("frequency").quantile(0.6))
            .then(4)
            .when(pl.col("frequency") >= pl.col("frequency").quantile(0.4))
            .then(3)
            .when(pl.col("frequency") >= pl.col("frequency").quantile(0.2))
            .then(2)
            .otherwise(1)
            .alias("rfm_frequency_score"),
            
            # Monetary score (higher = better)
            pl.when(pl.col("monetary") >= pl.col("monetary").quantile(0.8))
            .then(5)
            .when(pl.col("monetary") >= pl.col("monetary").quantile(0.6))
            .then(4)
            .when(pl.col("monetary") >= pl.col("monetary").quantile(0.4))
            .then(3)
            .when(pl.col("monetary") >= pl.col("monetary").quantile(0.2))
            .then(2)
            .otherwise(1)
            .alias("rfm_monetary_score"),
        ])
        
        # Calculate combined score
        rfm = rfm.with_columns(
            (pl.col("rfm_recency_score") + 
             pl.col("rfm_frequency_score") + 
             pl.col("rfm_monetary_score"))
            .alias("rfm_combined_score")
        )
        
        # Assign customer segment
        rfm = rfm.with_columns(
            pl.when(pl.col("rfm_combined_score") >= 12)
            .then(pl.lit("vip"))
            .when(pl.col("rfm_combined_score") >= 9)
            .then(pl.lit("loyal"))
            .when(pl.col("rfm_combined_score") >= 6)
            .then(pl.lit("potential"))
            .when(pl.col("rfm_recency_score") <= 2)
            .then(pl.lit("at_risk"))
            .otherwise(pl.lit("new"))
            .alias("customer_segment")
        )
        
        return rfm
    
    def calculate_clv(
        self,
        df: pl.DataFrame,
        customer_id_col: str = "customer_id",
        order_date_col: str = "order_date",
        amount_col: str = "total_amount",
        time_horizon_days: int = 365,
    ) -> pl.DataFrame:
        """
        Calculate Customer Lifetime Value (CLV) using simple historical method.
        
        CLV = Average Order Value × Purchase Frequency × Expected Lifespan
        
        Args:
            df: Orders DataFrame
            customer_id_col: Customer ID column
            order_date_col: Order date column
            amount_col: Order amount column
            time_horizon_days: Time horizon for frequency calculation
            
        Returns:
            DataFrame with CLV metrics
        """
        # Calculate customer-level metrics
        customer_metrics = df.group_by(customer_id_col).agg([
            # Total orders
            pl.count().alias("total_orders"),
            # Total revenue
            pl.col(amount_col).sum().alias("total_revenue"),
            # Average order value
            pl.col(amount_col).mean().alias("avg_order_value"),
            # First order date
            pl.col(order_date_col).min().alias("first_order_date"),
            # Last order date
            pl.col(order_date_col).max().alias("last_order_date"),
        ])
        
        # Calculate customer lifespan and frequency
        customer_metrics = customer_metrics.with_columns([
            # Customer lifespan in days
            (pl.col("last_order_date") - pl.col("first_order_date"))
            .dt.total_days()
            .alias("customer_lifespan_days"),
            
            # Days since first order
            (pl.lit(self.reference_date) - pl.col("first_order_date"))
            .dt.total_days()
            .alias("days_since_first_order"),
        ])
        
        # Calculate purchase frequency (orders per year)
        customer_metrics = customer_metrics.with_columns(
            pl.when(pl.col("customer_lifespan_days") > 0)
            .then(pl.col("total_orders") / (pl.col("customer_lifespan_days") / 365))
            .otherwise(pl.col("total_orders"))
            .alias("purchase_frequency_yearly")
        )
        
        # Calculate simple CLV (1-year projection)
        customer_metrics = customer_metrics.with_columns(
            (pl.col("avg_order_value") * pl.col("purchase_frequency_yearly"))
            .alias("projected_annual_value")
        )
        
        # Historical CLV
        customer_metrics = customer_metrics.with_columns(
            pl.col("total_revenue").alias("lifetime_value")
        )
        
        return customer_metrics
    
    def enrich_orders_with_time_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Add time-based features to orders.
        
        Features added:
        - Day of week, hour of day
        - Is weekend/holiday
        - Time since last order
        - Order sequence number
        """
        order_date_col = "order_timestamp" if "order_timestamp" in df.columns else "order_date"
        
        if order_date_col not in df.columns:
            return df
        
        # Extract time features
        df = df.with_columns([
            pl.col(order_date_col).dt.weekday().alias("order_day_of_week"),
            pl.col(order_date_col).dt.hour().alias("order_hour"),
            pl.col(order_date_col).dt.month().alias("order_month"),
            pl.col(order_date_col).dt.quarter().alias("order_quarter"),
            (pl.col(order_date_col).dt.weekday() >= 5).alias("is_weekend_order"),
        ])
        
        # Add order sequence per customer
        if "customer_id" in df.columns:
            df = df.with_columns(
                pl.col(order_date_col)
                .rank("dense")
                .over("customer_id")
                .alias("customer_order_sequence")
            )
            
            # Flag first order
            df = df.with_columns(
                (pl.col("customer_order_sequence") == 1).alias("is_first_order")
            )
        
        return df
    
    def enrich_products_with_metrics(
        self,
        products_df: pl.DataFrame,
        orders_df: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Enrich products with sales performance metrics.
        
        Adds:
        - Total units sold
        - Total revenue
        - Order count
        - Average selling price
        - Sales velocity (units/day)
        """
        # Aggregate order items by product
        if "product_id" not in orders_df.columns:
            return products_df
        
        product_sales = orders_df.group_by("product_id").agg([
            pl.col("quantity").sum().alias("total_units_sold"),
            pl.col("line_total").sum().alias("total_product_revenue"),
            pl.count().alias("order_count"),
            (pl.col("line_total") / pl.col("quantity")).mean().alias("avg_selling_price"),
        ])
        
        # Calculate sales velocity
        if "order_timestamp" in orders_df.columns:
            date_range = (
                orders_df["order_timestamp"].max() - 
                orders_df["order_timestamp"].min()
            )
            days = max(date_range.total_seconds() / 86400, 1)
            
            product_sales = product_sales.with_columns(
                (pl.col("total_units_sold") / days).alias("sales_velocity_daily")
            )
        
        # Join to products
        products_df = products_df.join(
            product_sales,
            on="product_id",
            how="left",
        )
        
        # Fill nulls for products with no sales
        products_df = products_df.with_columns([
            pl.col("total_units_sold").fill_null(0),
            pl.col("total_product_revenue").fill_null(0),
            pl.col("order_count").fill_null(0),
        ])
        
        return products_df
    
    def add_customer_cohorts(
        self,
        df: pl.DataFrame,
        date_col: str = "first_order_date",
        cohort_type: str = "month",
    ) -> pl.DataFrame:
        """
        Add cohort labels based on acquisition date.
        
        Args:
            df: Customer or orders DataFrame
            date_col: Date column for cohort assignment
            cohort_type: "week", "month", or "quarter"
        """
        if date_col not in df.columns:
            return df
        
        if cohort_type == "week":
            df = df.with_columns(
                pl.col(date_col).dt.strftime("%Y-W%W").alias("acquisition_cohort")
            )
        elif cohort_type == "month":
            df = df.with_columns(
                pl.col(date_col).dt.strftime("%Y-%m").alias("acquisition_cohort")
            )
        elif cohort_type == "quarter":
            df = df.with_columns(
                (pl.col(date_col).dt.year().cast(pl.Utf8) + "-Q" + 
                 pl.col(date_col).dt.quarter().cast(pl.Utf8))
                .alias("acquisition_cohort")
            )
        
        return df
    
    def hash_pii(
        self,
        df: pl.DataFrame,
        columns: List[str],
        salt: str = "ecommerce-salt",
    ) -> pl.DataFrame:
        """
        Hash PII columns for privacy compliance.
        
        Uses SHA-256 with salt for irreversible hashing.
        """
        for col in columns:
            if col in df.columns:
                # Create hashed column
                df = df.with_columns(
                    pl.col(col)
                    .map_elements(
                        lambda x: hashlib.sha256(
                            f"{salt}{x}".encode()
                        ).hexdigest() if x else None,
                        return_dtype=pl.Utf8
                    )
                    .alias(f"{col}_hash")
                )
                
                # Mask original column
                df = df.with_columns(
                    pl.col(col)
                    .map_elements(
                        lambda x: x[:2] + "***" + x[-2:] if x and len(x) > 4 else "***",
                        return_dtype=pl.Utf8
                    )
                    .alias(f"{col}_masked")
                )
        
        return df


def enrich_customer_data(
    customers_df: pl.DataFrame,
    orders_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Convenience function to enrich customer data with metrics.
    
    Args:
        customers_df: Customer dimension DataFrame
        orders_df: Orders fact DataFrame
        
    Returns:
        Enriched customer DataFrame
    """
    enricher = DataEnricher()
    
    # Calculate RFM scores
    rfm_scores = enricher.calculate_rfm_scores(
        orders_df,
        customer_id_col="customer_id",
        order_date_col="order_timestamp",
        amount_col="total_amount",
    )
    
    # Calculate CLV
    clv_metrics = enricher.calculate_clv(
        orders_df,
        customer_id_col="customer_id",
        order_date_col="order_timestamp",
        amount_col="total_amount",
    )
    
    # Join metrics to customers
    customers_df = customers_df.join(
        rfm_scores.select([
            "customer_id",
            "rfm_recency_score",
            "rfm_frequency_score",
            "rfm_monetary_score",
            "rfm_combined_score",
            "customer_segment",
        ]),
        on="customer_id",
        how="left",
    )
    
    customers_df = customers_df.join(
        clv_metrics.select([
            "customer_id",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "lifetime_value",
            "purchase_frequency_yearly",
        ]),
        on="customer_id",
        how="left",
    )
    
    # Add cohorts
    customers_df = enricher.add_customer_cohorts(
        customers_df,
        date_col="first_order_date",
        cohort_type="month",
    )
    
    return customers_df


def enrich_order_data(orders_df: pl.DataFrame) -> pl.DataFrame:
    """
    Convenience function to enrich order data.
    
    Args:
        orders_df: Orders fact DataFrame
        
    Returns:
        Enriched orders DataFrame
    """
    enricher = DataEnricher()
    
    # Add time features
    orders_df = enricher.enrich_orders_with_time_features(orders_df)
    
    return orders_df
