"""
Unit Tests - Data Quality
"""
import pytest
import polars as pl
import numpy as np

from src.quality.validators import (
    DataValidator,
    ValidationSeverity,
    ValidationStatus,
    create_orders_validator,
)
from src.quality.anomaly_detector import (
    AnomalyDetector,
    AnomalyType,
    AnomalySeverity,
)


class TestDataValidator:
    """Tests for DataValidator"""
    
    def test_not_null_check_passes(self):
        """Test not null check with valid data"""
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        
        validator = DataValidator()
        validator.add_not_null_check("id")
        
        result = validator.validate(df)
        
        assert result.status == ValidationStatus.PASSED
        assert result.passed_checks == 1
    
    def test_not_null_check_fails(self):
        """Test not null check with null values"""
        df = pl.DataFrame({"id": [1, None, 3], "name": ["a", "b", "c"]})
        
        validator = DataValidator()
        validator.add_not_null_check("id")
        
        result = validator.validate(df)
        
        assert result.status == ValidationStatus.FAILED
        assert result.failed_checks == 1
    
    def test_unique_check_passes(self):
        """Test unique check with unique values"""
        df = pl.DataFrame({"id": [1, 2, 3]})
        
        validator = DataValidator()
        validator.add_unique_check("id")
        
        result = validator.validate(df)
        
        assert result.status == ValidationStatus.PASSED
    
    def test_unique_check_fails(self):
        """Test unique check with duplicates"""
        df = pl.DataFrame({"id": [1, 2, 1]})
        
        validator = DataValidator()
        validator.add_unique_check("id")
        
        result = validator.validate(df)
        
        assert result.status == ValidationStatus.FAILED
    
    def test_range_check(self):
        """Test range check"""
        df = pl.DataFrame({"price": [10.0, 50.0, -5.0, 200.0]})
        
        validator = DataValidator()
        validator.add_range_check("price", min_value=0, max_value=100)
        
        result = validator.validate(df)
        
        assert result.status == ValidationStatus.FAILED
        # Two values outside range: -5 and 200
        check = result.checks[0]
        assert check.failed_rows == 2
    
    def test_enum_check(self):
        """Test enum/allowed values check"""
        df = pl.DataFrame({"status": ["pending", "shipped", "invalid"]})
        
        validator = DataValidator()
        validator.add_enum_check("status", ["pending", "shipped", "delivered"])
        
        result = validator.validate(df)
        
        assert result.status == ValidationStatus.FAILED
    
    def test_pattern_check(self):
        """Test regex pattern check"""
        df = pl.DataFrame({"email": ["test@example.com", "invalid", "user@test.org"]})
        
        validator = DataValidator()
        validator.add_pattern_check("email", r".*@.*\..*")
        
        result = validator.validate(df)
        
        # "invalid" doesn't match pattern
        assert result.status == ValidationStatus.FAILED
    
    def test_custom_check(self):
        """Test custom validation check"""
        df = pl.DataFrame({"total": [100, 200, 300]})
        
        validator = DataValidator()
        validator.add_custom_check(
            name="total_sum",
            check_func=lambda df: df["total"].sum() < 1000,
            message_on_fail="Sum exceeds 1000",
        )
        
        result = validator.validate(df)
        
        # Sum is 600, which is < 1000
        assert result.status == ValidationStatus.PASSED
    
    def test_orders_validator(self, sample_orders_df):
        """Test pre-built orders validator"""
        validator = create_orders_validator()
        result = validator.validate(sample_orders_df)
        
        # Should run all checks
        assert result.total_checks > 0


class TestAnomalyDetector:
    """Tests for AnomalyDetector"""
    
    def test_zscore_detection(self):
        """Test Z-score anomaly detection"""
        # Normal values with one outlier
        values = np.array([10, 11, 10, 12, 11, 10, 50])  # 50 is outlier
        
        detector = AnomalyDetector(z_threshold=2.0)
        detector.add_metric("test_metric", values)
        
        report = detector.detect(methods=["zscore"])
        
        assert report.anomalies_found >= 1
        assert any(a.value == 50 for a in report.anomalies)
    
    def test_iqr_detection(self):
        """Test IQR anomaly detection"""
        values = np.array([1, 2, 2, 3, 3, 3, 4, 4, 5, 100])  # 100 is outlier
        
        detector = AnomalyDetector(iqr_multiplier=1.5)
        detector.add_metric("test_metric", values)
        
        report = detector.detect(methods=["iqr"])
        
        assert report.anomalies_found >= 1
    
    def test_pct_change_detection(self):
        """Test percentage change detection"""
        # Sudden spike
        values = np.array([100, 102, 98, 105, 200, 103])  # 200 is 90% spike
        
        detector = AnomalyDetector(pct_change_threshold=50)
        detector.add_metric("test_metric", values)
        
        report = detector.detect(methods=["pct_change"])
        
        assert report.anomalies_found >= 1
        # Should detect spike to 200
        spike_anomalies = [a for a in report.anomalies if a.anomaly_type == AnomalyType.SPIKE]
        assert len(spike_anomalies) >= 1
    
    def test_missing_data_detection(self):
        """Test missing/null data detection"""
        values = np.array([1, 2, np.nan, np.nan, np.nan, 6, np.nan, 8, 9, 10])
        
        detector = AnomalyDetector()
        detector.add_metric("test_metric", values)
        
        report = detector.detect(methods=["missing"])
        
        # 40% missing, should be detected
        assert report.anomalies_found >= 1
        missing_anomalies = [a for a in report.anomalies if a.anomaly_type == AnomalyType.MISSING]
        assert len(missing_anomalies) >= 1
    
    def test_custom_rule(self):
        """Test custom rule-based detection"""
        values = np.array([100, 200, -50, 300])  # -50 is negative
        
        detector = AnomalyDetector()
        detector.add_metric("revenue", values)
        detector.add_rule(
            name="negative_revenue",
            check_func=lambda x: x < 0,
            message="Negative revenue: {value}",
            severity=AnomalySeverity.CRITICAL,
        )
        
        report = detector.detect(methods=["rules"])
        
        assert report.anomalies_found >= 1
        assert report.has_critical_anomalies
    
    def test_no_anomalies(self):
        """Test with normal data"""
        values = np.array([100, 101, 99, 102, 100, 101])
        
        detector = AnomalyDetector(z_threshold=3.0)
        detector.add_metric("stable_metric", values)
        
        report = detector.detect(methods=["zscore"])
        
        assert report.anomalies_found == 0
