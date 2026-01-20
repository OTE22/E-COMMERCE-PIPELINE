"""
Data Cleaning Module

Production data cleaning transformations for e-commerce data.
Handles:
- Missing value imputation
- Data type standardization
- Deduplication
- Outlier detection and handling
- Format normalization
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Union
import re

import polars as pl
import pandas as pd
import numpy as np
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class CleaningStats:
    """Statistics from cleaning operations"""
    total_rows: int
    rows_after_cleaning: int
    nulls_filled: int
    duplicates_removed: int
    outliers_handled: int
    format_corrections: int


class DataCleaner:
    """
    Production data cleaner with comprehensive cleaning transformations.
    
    Supports both Polars and Pandas DataFrames.
    
    Example:
        cleaner = DataCleaner()
        df_clean, stats = cleaner.clean(df, config)
    """
    
    def __init__(self):
        self._cleaning_rules: Dict[str, Callable] = {}
        self._register_default_rules()
    
    def _register_default_rules(self) -> None:
        """Register default cleaning rules"""
        self._cleaning_rules = {
            "trim_strings": self._trim_strings,
            "normalize_case": self._normalize_case,
            "remove_duplicates": self._remove_duplicates,
            "fill_nulls": self._fill_nulls,
            "standardize_dates": self._standardize_dates,
            "normalize_currency": self._normalize_currency,
            "clean_email": self._clean_email,
            "clean_phone": self._clean_phone,
        }
    
    def register_rule(self, name: str, func: Callable) -> None:
        """Register a custom cleaning rule"""
        self._cleaning_rules[name] = func
    
    def _trim_strings(self, df: pl.DataFrame, columns: Optional[List[str]] = None) -> pl.DataFrame:
        """Trim whitespace from string columns"""
        string_cols = columns or [
            col for col, dtype in zip(df.columns, df.dtypes) 
            if dtype == pl.Utf8
        ]
        
        for col in string_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).str.strip_chars().alias(col)
                )
        
        return df
    
    def _normalize_case(
        self, 
        df: pl.DataFrame, 
        columns: List[str], 
        case: str = "lower"
    ) -> pl.DataFrame:
        """Normalize string case"""
        for col in columns:
            if col in df.columns:
                if case == "lower":
                    df = df.with_columns(pl.col(col).str.to_lowercase().alias(col))
                elif case == "upper":
                    df = df.with_columns(pl.col(col).str.to_uppercase().alias(col))
                elif case == "title":
                    df = df.with_columns(pl.col(col).str.to_titlecase().alias(col))
        
        return df
    
    def _remove_duplicates(
        self, 
        df: pl.DataFrame, 
        subset: Optional[List[str]] = None,
        keep: str = "first"
    ) -> pl.DataFrame:
        """Remove duplicate rows"""
        if subset:
            return df.unique(subset=subset, keep=keep)
        return df.unique()
    
    def _fill_nulls(
        self, 
        df: pl.DataFrame, 
        fill_values: Dict[str, Any]
    ) -> pl.DataFrame:
        """Fill null values with specified defaults"""
        for col, value in fill_values.items():
            if col in df.columns:
                df = df.with_columns(pl.col(col).fill_null(value).alias(col))
        
        return df
    
    def _standardize_dates(
        self, 
        df: pl.DataFrame, 
        date_columns: List[str],
        format: str = "%Y-%m-%d %H:%M:%S"
    ) -> pl.DataFrame:
        """Standardize date formats"""
        for col in date_columns:
            if col in df.columns:
                try:
                    df = df.with_columns(
                        pl.col(col).str.strptime(pl.Datetime, format).alias(col)
                    )
                except Exception:
                    # Try multiple formats
                    formats = [
                        "%Y-%m-%d",
                        "%m/%d/%Y",
                        "%d-%m-%Y",
                        "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d %H:%M:%S.%f",
                    ]
                    for fmt in formats:
                        try:
                            df = df.with_columns(
                                pl.col(col).str.strptime(pl.Datetime, fmt).alias(col)
                            )
                            break
                        except Exception:
                            continue
        
        return df
    
    def _normalize_currency(
        self, 
        df: pl.DataFrame, 
        amount_columns: List[str]
    ) -> pl.DataFrame:
        """Normalize currency values (remove symbols, convert to decimal)"""
        for col in amount_columns:
            if col in df.columns:
                # Remove currency symbols and convert
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.Utf8)
                    .str.replace_all(r"[$€£¥,]", "")
                    .str.strip_chars()
                    .cast(pl.Float64)
                    .alias(col)
                )
        
        return df
    
    def _clean_email(self, df: pl.DataFrame, email_column: str = "email") -> pl.DataFrame:
        """Clean and validate email addresses"""
        if email_column not in df.columns:
            return df
        
        # Lowercase and trim
        df = df.with_columns(
            pl.col(email_column)
            .str.to_lowercase()
            .str.strip_chars()
            .alias(email_column)
        )
        
        # Basic email validation pattern
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        
        # Mark invalid emails as null
        df = df.with_columns(
            pl.when(pl.col(email_column).str.contains(email_pattern))
            .then(pl.col(email_column))
            .otherwise(None)
            .alias(email_column)
        )
        
        return df
    
    def _clean_phone(
        self, 
        df: pl.DataFrame, 
        phone_column: str = "phone"
    ) -> pl.DataFrame:
        """Clean phone numbers - keep only digits"""
        if phone_column not in df.columns:
            return df
        
        df = df.with_columns(
            pl.col(phone_column)
            .str.replace_all(r"[^\d]", "")
            .alias(phone_column)
        )
        
        return df
    
    def clean_orders(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply order-specific cleaning transformations"""
        # Trim strings
        df = self._trim_strings(df)
        
        # Standardize dates
        date_cols = ["order_date", "ship_date", "created_at", "updated_at"]
        df = self._standardize_dates(df, [c for c in date_cols if c in df.columns])
        
        # Normalize currency
        amount_cols = ["subtotal", "total_amount", "discount_amount", "tax_amount", "shipping_amount"]
        df = self._normalize_currency(df, [c for c in amount_cols if c in df.columns])
        
        # Fill null values
        df = self._fill_nulls(df, {
            "discount_amount": 0.0,
            "tax_amount": 0.0,
            "shipping_amount": 0.0,
            "item_count": 1,
        })
        
        # Remove duplicates by order_number
        if "order_number" in df.columns:
            df = self._remove_duplicates(df, subset=["order_number"])
        
        # Validate amounts are positive
        for col in amount_cols:
            if col in df.columns:
                df = df.filter(pl.col(col) >= 0)
        
        return df
    
    def clean_customers(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply customer-specific cleaning transformations"""
        # Trim strings
        df = self._trim_strings(df)
        
        # Clean email
        if "email" in df.columns:
            df = self._clean_email(df, "email")
        
        # Clean phone
        if "phone" in df.columns:
            df = self._clean_phone(df, "phone")
        
        # Normalize names
        name_cols = ["first_name", "last_name"]
        df = self._normalize_case(df, [c for c in name_cols if c in df.columns], "title")
        
        # Standardize country codes
        if "country" in df.columns:
            df = self._normalize_case(df, ["country"], "upper")
        
        # Fill nulls
        df = self._fill_nulls(df, {
            "total_orders": 0,
            "lifetime_value": 0.0,
        })
        
        # Remove duplicates
        if "customer_key" in df.columns:
            df = self._remove_duplicates(df, subset=["customer_key"])
        
        return df
    
    def clean_products(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply product-specific cleaning transformations"""
        # Trim strings
        df = self._trim_strings(df)
        
        # Normalize prices
        price_cols = ["unit_price", "cost_price"]
        df = self._normalize_currency(df, [c for c in price_cols if c in df.columns])
        
        # Fill nulls
        df = self._fill_nulls(df, {
            "stock_quantity": 0,
            "review_count": 0,
            "avg_rating": 0.0,
            "is_active": True,
        })
        
        # Validate prices are positive
        for col in price_cols:
            if col in df.columns:
                df = df.filter(pl.col(col) > 0)
        
        # Remove duplicates
        if "sku" in df.columns:
            df = self._remove_duplicates(df, subset=["sku"])
        
        return df
    
    def detect_outliers(
        self, 
        df: pl.DataFrame, 
        column: str, 
        method: str = "iqr",
        threshold: float = 1.5
    ) -> pl.DataFrame:
        """
        Detect outliers using IQR or Z-score method.
        
        Args:
            df: Input DataFrame
            column: Column to check for outliers
            method: "iqr" or "zscore"
            threshold: IQR multiplier or Z-score threshold
            
        Returns:
            DataFrame with _is_outlier column added
        """
        if column not in df.columns:
            return df
        
        if method == "iqr":
            q1 = df[column].quantile(0.25)
            q3 = df[column].quantile(0.75)
            iqr = q3 - q1
            lower = q1 - threshold * iqr
            upper = q3 + threshold * iqr
            
            df = df.with_columns(
                ((pl.col(column) < lower) | (pl.col(column) > upper))
                .alias("_is_outlier")
            )
        
        elif method == "zscore":
            mean = df[column].mean()
            std = df[column].std()
            
            df = df.with_columns(
                (((pl.col(column) - mean) / std).abs() > threshold)
                .alias("_is_outlier")
            )
        
        return df
    
    def handle_outliers(
        self, 
        df: pl.DataFrame, 
        column: str, 
        strategy: str = "clip",
        **kwargs
    ) -> pl.DataFrame:
        """
        Handle outliers using specified strategy.
        
        Args:
            df: Input DataFrame
            column: Column with outliers
            strategy: "clip", "remove", or "replace"
            **kwargs: Additional parameters (e.g., lower, upper, replacement_value)
        """
        df = self.detect_outliers(df, column, **kwargs)
        
        if "_is_outlier" not in df.columns:
            return df
        
        if strategy == "remove":
            df = df.filter(~pl.col("_is_outlier"))
        
        elif strategy == "clip":
            q1 = df[column].quantile(0.25)
            q3 = df[column].quantile(0.75)
            iqr = q3 - q1
            lower = kwargs.get("lower", q1 - 1.5 * iqr)
            upper = kwargs.get("upper", q3 + 1.5 * iqr)
            
            df = df.with_columns(
                pl.col(column).clip(lower, upper).alias(column)
            )
        
        elif strategy == "replace":
            replacement = kwargs.get("replacement_value", df[column].median())
            df = df.with_columns(
                pl.when(pl.col("_is_outlier"))
                .then(replacement)
                .otherwise(pl.col(column))
                .alias(column)
            )
        
        # Remove helper column
        df = df.drop("_is_outlier")
        
        return df


def clean_dataframe(
    df: pl.DataFrame, 
    data_type: str = "generic"
) -> pl.DataFrame:
    """
    Convenience function to clean a DataFrame.
    
    Args:
        df: Input DataFrame
        data_type: "orders", "customers", "products", or "generic"
        
    Returns:
        Cleaned DataFrame
    """
    cleaner = DataCleaner()
    
    if data_type == "orders":
        return cleaner.clean_orders(df)
    elif data_type == "customers":
        return cleaner.clean_customers(df)
    elif data_type == "products":
        return cleaner.clean_products(df)
    else:
        # Generic cleaning
        df = cleaner._trim_strings(df)
        df = cleaner._remove_duplicates(df)
        return df
