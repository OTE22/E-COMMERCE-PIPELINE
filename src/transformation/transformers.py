"""
ETL Transformer

Main ETL transformation orchestrator that combines cleaning, enrichment,
and loading into a unified pipeline.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import polars as pl
import structlog

from src.config import get_settings
from .cleaners import DataCleaner, clean_dataframe
from .enrichers import DataEnricher, enrich_customer_data, enrich_order_data

logger = structlog.get_logger(__name__)
settings = get_settings()


class TransformationType(str, Enum):
    """Types of transformations"""
    ORDERS = "orders"
    CUSTOMERS = "customers"
    PRODUCTS = "products"
    CLICKSTREAM = "clickstream"
    INVENTORY = "inventory"


@dataclass
class TransformResult:
    """Result of transformation pipeline"""
    transformation_type: TransformationType
    input_rows: int
    output_rows: int
    rows_dropped: int
    started_at: datetime
    completed_at: datetime
    duration_seconds: float
    output_path: Optional[str] = None
    errors: List[str] = None


class ETLTransformer:
    """
    Main ETL transformation pipeline orchestrator.
    
    Coordinates cleaning, enrichment, and output generation
    for all data types in the e-commerce platform.
    
    Example:
        transformer = ETLTransformer()
        result = await transformer.transform_orders(raw_orders_df)
    """
    
    def __init__(
        self,
        output_path: Optional[str] = None,
        enable_validation: bool = True,
    ):
        self.output_path = Path(output_path or settings.data_lake.curated_path)
        self.enable_validation = enable_validation
        self.cleaner = DataCleaner()
        self.enricher = DataEnricher()
        
        # Ensure output directory exists
        self.output_path.mkdir(parents=True, exist_ok=True)
    
    def _write_output(
        self,
        df: pl.DataFrame,
        name: str,
        partition_by: Optional[List[str]] = None,
    ) -> str:
        """Write transformed data to curated zone"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_file = self.output_path / f"{name}_{timestamp}.parquet"
        
        df.write_parquet(output_file)
        logger.info(f"Written {len(df)} rows to {output_file}")
        
        return str(output_file)
    
    async def transform_orders(
        self,
        df: pl.DataFrame,
        include_items: bool = True,
    ) -> TransformResult:
        """
        Transform raw orders data.
        
        Pipeline:
        1. Clean order data
        2. Enrich with time features
        3. Calculate derived metrics
        4. Output to curated zone
        """
        started_at = datetime.utcnow()
        input_rows = len(df)
        errors = []
        
        logger.info(f"Starting orders transformation with {input_rows} rows")
        
        try:
            # Step 1: Clean
            df = self.cleaner.clean_orders(df)
            logger.info(f"After cleaning: {len(df)} rows")
            
            # Step 2: Enrich with time features
            df = self.enricher.enrich_orders_with_time_features(df)
            
            # Step 3: Calculate order-level metrics
            if "subtotal" in df.columns and "discount_amount" in df.columns:
                df = df.with_columns(
                    (pl.col("subtotal") - pl.col("discount_amount"))
                    .alias("net_amount")
                )
            
            # Step 4: Add date key for star schema
            if "order_timestamp" in df.columns:
                df = df.with_columns(
                    pl.col("order_timestamp").dt.strftime("%Y%m%d").cast(pl.Int32)
                    .alias("order_date_key")
                )
            
            # Step 5: Write output
            output_file = self._write_output(df, "orders_curated")
            
        except Exception as e:
            logger.error(f"Orders transformation failed: {e}")
            errors.append(str(e))
            output_file = None
        
        completed_at = datetime.utcnow()
        
        return TransformResult(
            transformation_type=TransformationType.ORDERS,
            input_rows=input_rows,
            output_rows=len(df),
            rows_dropped=input_rows - len(df),
            started_at=started_at,
            completed_at=completed_at,
            duration_seconds=(completed_at - started_at).total_seconds(),
            output_path=output_file,
            errors=errors,
        )
    
    async def transform_customers(
        self,
        customers_df: pl.DataFrame,
        orders_df: Optional[pl.DataFrame] = None,
    ) -> TransformResult:
        """
        Transform raw customer data.
        
        Pipeline:
        1. Clean customer data
        2. Hash PII for compliance
        3. Enrich with RFM and CLV if orders provided
        4. Output to curated zone
        """
        started_at = datetime.utcnow()
        input_rows = len(customers_df)
        errors = []
        
        logger.info(f"Starting customer transformation with {input_rows} rows")
        
        try:
            # Step 1: Clean
            customers_df = self.cleaner.clean_customers(customers_df)
            logger.info(f"After cleaning: {len(customers_df)} rows")
            
            # Step 2: Hash PII
            pii_columns = ["email", "phone", "first_name", "last_name"]
            pii_cols_present = [c for c in pii_columns if c in customers_df.columns]
            if pii_cols_present:
                customers_df = self.enricher.hash_pii(customers_df, pii_cols_present)
            
            # Step 3: Enrich with metrics if orders available
            if orders_df is not None:
                customers_df = enrich_customer_data(customers_df, orders_df)
            
            # Step 4: Write output
            output_file = self._write_output(customers_df, "customers_curated")
            
        except Exception as e:
            logger.error(f"Customer transformation failed: {e}")
            errors.append(str(e))
            output_file = None
        
        completed_at = datetime.utcnow()
        
        return TransformResult(
            transformation_type=TransformationType.CUSTOMERS,
            input_rows=input_rows,
            output_rows=len(customers_df),
            rows_dropped=input_rows - len(customers_df),
            started_at=started_at,
            completed_at=completed_at,
            duration_seconds=(completed_at - started_at).total_seconds(),
            output_path=output_file,
            errors=errors,
        )
    
    async def transform_products(
        self,
        products_df: pl.DataFrame,
        orders_df: Optional[pl.DataFrame] = None,
    ) -> TransformResult:
        """
        Transform raw product data.
        
        Pipeline:
        1. Clean product data
        2. Enrich with sales metrics
        3. Calculate inventory metrics
        4. Output to curated zone
        """
        started_at = datetime.utcnow()
        input_rows = len(products_df)
        errors = []
        
        logger.info(f"Starting product transformation with {input_rows} rows")
        
        try:
            # Step 1: Clean
            products_df = self.cleaner.clean_products(products_df)
            logger.info(f"After cleaning: {len(products_df)} rows")
            
            # Step 2: Enrich with sales metrics
            if orders_df is not None:
                products_df = self.enricher.enrich_products_with_metrics(
                    products_df, orders_df
                )
            
            # Step 3: Calculate margin if not present
            if "unit_price" in products_df.columns and "cost_price" in products_df.columns:
                products_df = products_df.with_columns(
                    ((pl.col("unit_price") - pl.col("cost_price")) / pl.col("unit_price") * 100)
                    .alias("margin_percent")
                )
            
            # Step 4: Write output
            output_file = self._write_output(products_df, "products_curated")
            
        except Exception as e:
            logger.error(f"Product transformation failed: {e}")
            errors.append(str(e))
            output_file = None
        
        completed_at = datetime.utcnow()
        
        return TransformResult(
            transformation_type=TransformationType.PRODUCTS,
            input_rows=input_rows,
            output_rows=len(products_df),
            rows_dropped=input_rows - len(products_df),
            started_at=started_at,
            completed_at=completed_at,
            duration_seconds=(completed_at - started_at).total_seconds(),
            output_path=output_file,
            errors=errors,
        )
    
    async def transform_clickstream(
        self,
        df: pl.DataFrame,
    ) -> TransformResult:
        """
        Transform clickstream/page view data.
        
        Pipeline:
        1. Clean and deduplicate
        2. Parse UTM parameters
        3. Calculate session metrics
        4. Output to curated zone
        """
        started_at = datetime.utcnow()
        input_rows = len(df)
        errors = []
        
        logger.info(f"Starting clickstream transformation with {input_rows} rows")
        
        try:
            # Step 1: Clean
            df = self.cleaner._trim_strings(df)
            df = self.cleaner._remove_duplicates(df, subset=["page_view_id"])
            
            # Step 2: Add date key
            if "event_timestamp" in df.columns:
                df = df.with_columns(
                    pl.col("event_timestamp").dt.strftime("%Y%m%d").cast(pl.Int32)
                    .alias("date_key")
                )
            
            # Step 3: Calculate session metrics (simple version)
            if "session_id" in df.columns:
                session_metrics = df.group_by("session_id").agg([
                    pl.count().alias("pages_per_session"),
                    pl.col("event_timestamp").min().alias("session_start"),
                    pl.col("event_timestamp").max().alias("session_end"),
                ])
                
                session_metrics = session_metrics.with_columns(
                    (pl.col("session_end") - pl.col("session_start"))
                    .dt.total_seconds()
                    .alias("session_duration_seconds")
                )
                
                df = df.join(
                    session_metrics.select(["session_id", "pages_per_session", "session_duration_seconds"]),
                    on="session_id",
                    how="left",
                )
            
            # Step 4: Write output
            output_file = self._write_output(df, "clickstream_curated")
            
        except Exception as e:
            logger.error(f"Clickstream transformation failed: {e}")
            errors.append(str(e))
            output_file = None
        
        completed_at = datetime.utcnow()
        
        return TransformResult(
            transformation_type=TransformationType.CLICKSTREAM,
            input_rows=input_rows,
            output_rows=len(df),
            rows_dropped=input_rows - len(df),
            started_at=started_at,
            completed_at=completed_at,
            duration_seconds=(completed_at - started_at).total_seconds(),
            output_path=output_file,
            errors=errors,
        )
    
    async def run_full_etl(
        self,
        orders_df: pl.DataFrame,
        customers_df: pl.DataFrame,
        products_df: pl.DataFrame,
        clickstream_df: Optional[pl.DataFrame] = None,
    ) -> Dict[str, TransformResult]:
        """
        Run full ETL pipeline for all data types.
        
        Args:
            orders_df: Raw orders data
            customers_df: Raw customers data
            products_df: Raw products data
            clickstream_df: Optional clickstream data
            
        Returns:
            Dictionary of transformation results by type
        """
        logger.info("Starting full ETL pipeline")
        results = {}
        
        # Transform in dependency order
        results["products"] = await self.transform_products(products_df)
        results["customers"] = await self.transform_customers(customers_df, orders_df)
        results["orders"] = await self.transform_orders(orders_df)
        
        if clickstream_df is not None:
            results["clickstream"] = await self.transform_clickstream(clickstream_df)
        
        # Summary
        total_input = sum(r.input_rows for r in results.values())
        total_output = sum(r.output_rows for r in results.values())
        total_duration = sum(r.duration_seconds for r in results.values())
        
        logger.info(
            f"Full ETL complete: {total_input} input → {total_output} output, "
            f"duration: {total_duration:.2f}s"
        )
        
        return results
