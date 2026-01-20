"""
Unit Tests - Data Transformation
"""
import pytest
import polars as pl

from src.transformation.cleaners import DataCleaner, clean_dataframe
from src.transformation.enrichers import DataEnricher


class TestDataCleaner:
    """Tests for DataCleaner"""
    
    def test_trim_strings(self):
        """Test string trimming"""
        cleaner = DataCleaner()
        df = pl.DataFrame({
            "name": ["  John  ", "Jane", "  Bob"],
            "email": [" test@example.com ", "user@test.com", "admin@site.com "],
        })
        
        result = cleaner._trim_strings(df)
        
        assert result["name"].to_list() == ["John", "Jane", "Bob"]
        assert result["email"].to_list() == ["test@example.com", "user@test.com", "admin@site.com"]
    
    def test_remove_duplicates(self):
        """Test duplicate removal"""
        cleaner = DataCleaner()
        df = pl.DataFrame({
            "id": [1, 2, 1, 3],
            "value": ["a", "b", "a", "c"],
        })
        
        result = cleaner._remove_duplicates(df, subset=["id"])
        
        assert len(result) == 3
    
    def test_fill_nulls(self):
        """Test null value filling"""
        cleaner = DataCleaner()
        df = pl.DataFrame({
            "amount": [100.0, None, 200.0],
            "count": [1, 2, None],
        })
        
        result = cleaner._fill_nulls(df, {"amount": 0.0, "count": 0})
        
        assert result["amount"].to_list() == [100.0, 0.0, 200.0]
        assert result["count"].to_list() == [1, 2, 0]
    
    def test_clean_orders(self, sample_orders_df):
        """Test order-specific cleaning"""
        cleaner = DataCleaner()
        result = cleaner.clean_orders(sample_orders_df)
        
        # Should have all columns
        assert "order_id" in result.columns
        assert "total_amount" in result.columns
        
        # No duplicates
        assert result["order_number"].n_unique() == len(result)


class TestDataEnricher:
    """Tests for DataEnricher"""
    
    def test_enrich_orders_with_time_features(self, sample_orders_df):
        """Test time feature enrichment"""
        enricher = DataEnricher()
        
        # Convert to datetime first
        df = sample_orders_df.with_columns(
            pl.col("order_timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
        )
        
        result = enricher.enrich_orders_with_time_features(df)
        
        assert "order_day_of_week" in result.columns
        assert "order_hour" in result.columns
        assert "order_month" in result.columns
        assert "is_weekend_order" in result.columns


class TestCleanDataframe:
    """Tests for clean_dataframe convenience function"""
    
    def test_clean_orders(self, sample_orders_df):
        """Test cleaning orders data type"""
        result = clean_dataframe(sample_orders_df, data_type="orders")
        assert len(result) > 0
    
    def test_clean_customers(self, sample_customers_df):
        """Test cleaning customers data type"""
        result = clean_dataframe(sample_customers_df, data_type="customers")
        assert len(result) > 0
    
    def test_clean_products(self, sample_products_df):
        """Test cleaning products data type"""
        result = clean_dataframe(sample_products_df, data_type="products")
        assert len(result) > 0
    
    def test_clean_generic(self, sample_orders_df):
        """Test generic cleaning"""
        result = clean_dataframe(sample_orders_df, data_type="generic")
        assert len(result) > 0
