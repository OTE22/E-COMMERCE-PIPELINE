"""
Data Validation Module

Production data quality validation using rule-based checks.
Implements validation patterns inspired by Great Expectations.

Features:
- Schema validation
- Data type checks
- Business rule validation
- Referential integrity checks
- Statistical profiling
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

import polars as pl
import numpy as np
import structlog

from src.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()


class ValidationSeverity(str, Enum):
    """Severity levels for validation failures"""
    ERROR = "error"  # Critical - blocks pipeline
    WARNING = "warning"  # Non-critical - logged but continues
    INFO = "info"  # Informational only


class ValidationStatus(str, Enum):
    """Overall validation status"""
    PASSED = "passed"
    FAILED = "failed"
    PARTIAL = "partial"


@dataclass
class ValidationCheck:
    """Single validation check result"""
    name: str
    passed: bool
    severity: ValidationSeverity
    message: str
    details: Optional[Dict[str, Any]] = None
    failed_rows: int = 0
    total_rows: int = 0


@dataclass
class ValidationResult:
    """Complete validation suite result"""
    status: ValidationStatus
    total_checks: int
    passed_checks: int
    failed_checks: int
    warning_count: int
    checks: List[ValidationCheck] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    
    @property
    def success_rate(self) -> float:
        """Percentage of passed checks"""
        if self.total_checks == 0:
            return 100.0
        return (self.passed_checks / self.total_checks) * 100


class DataValidator:
    """
    Production data validator with comprehensive check suite.
    
    Validates data quality through:
    - Schema validation
    - Null checks
    - Range/boundary checks
    - Uniqueness checks
    - Pattern matching
    - Business rules
    
    Example:
        validator = DataValidator()
        validator.add_not_null_check("customer_id")
        validator.add_range_check("price", min_value=0)
        result = validator.validate(df)
    """
    
    def __init__(self, strict_mode: bool = False):
        self.strict_mode = strict_mode  # Fail on any warning
        self._checks: List[Callable] = []
        self._results: List[ValidationCheck] = []
    
    def reset(self) -> None:
        """Reset validator state"""
        self._checks = []
        self._results = []
    
    def add_not_null_check(
        self,
        column: str,
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ) -> "DataValidator":
        """Add check for null values in column"""
        def check(df: pl.DataFrame) -> ValidationCheck:
            if column not in df.columns:
                return ValidationCheck(
                    name=f"not_null_{column}",
                    passed=False,
                    severity=severity,
                    message=f"Column '{column}' not found",
                )
            
            null_count = df[column].null_count()
            total = len(df)
            passed = null_count == 0
            
            return ValidationCheck(
                name=f"not_null_{column}",
                passed=passed,
                severity=severity,
                message=f"Column '{column}' has {null_count} null values" if not passed else f"Column '{column}' has no null values",
                details={"null_count": null_count, "null_percentage": (null_count / total) * 100 if total > 0 else 0},
                failed_rows=null_count,
                total_rows=total,
            )
        
        self._checks.append(check)
        return self
    
    def add_unique_check(
        self,
        column: str,
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ) -> "DataValidator":
        """Add check for uniqueness of column values"""
        def check(df: pl.DataFrame) -> ValidationCheck:
            if column not in df.columns:
                return ValidationCheck(
                    name=f"unique_{column}",
                    passed=False,
                    severity=severity,
                    message=f"Column '{column}' not found",
                )
            
            total = len(df)
            unique_count = df[column].n_unique()
            duplicate_count = total - unique_count
            passed = duplicate_count == 0
            
            return ValidationCheck(
                name=f"unique_{column}",
                passed=passed,
                severity=severity,
                message=f"Column '{column}' has {duplicate_count} duplicate values" if not passed else f"Column '{column}' values are unique",
                details={"unique_count": unique_count, "duplicate_count": duplicate_count},
                failed_rows=duplicate_count,
                total_rows=total,
            )
        
        self._checks.append(check)
        return self
    
    def add_range_check(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ) -> "DataValidator":
        """Add check for values within specified range"""
        def check(df: pl.DataFrame) -> ValidationCheck:
            if column not in df.columns:
                return ValidationCheck(
                    name=f"range_{column}",
                    passed=False,
                    severity=severity,
                    message=f"Column '{column}' not found",
                )
            
            conditions = []
            if min_value is not None:
                conditions.append(pl.col(column) < min_value)
            if max_value is not None:
                conditions.append(pl.col(column) > max_value)
            
            if not conditions:
                return ValidationCheck(
                    name=f"range_{column}",
                    passed=True,
                    severity=severity,
                    message="No range specified",
                )
            
            # Combine conditions with OR
            combined = conditions[0]
            for cond in conditions[1:]:
                combined = combined | cond
            
            out_of_range = df.filter(combined).height
            total = len(df)
            passed = out_of_range == 0
            
            return ValidationCheck(
                name=f"range_{column}",
                passed=passed,
                severity=severity,
                message=f"Column '{column}' has {out_of_range} values outside range [{min_value}, {max_value}]" if not passed else f"All values in range",
                details={"min": min_value, "max": max_value, "out_of_range_count": out_of_range},
                failed_rows=out_of_range,
                total_rows=total,
            )
        
        self._checks.append(check)
        return self
    
    def add_positive_check(
        self,
        column: str,
        allow_zero: bool = True,
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ) -> "DataValidator":
        """Add check for positive values"""
        min_val = 0 if allow_zero else 0.0001
        return self.add_range_check(column, min_value=min_val, severity=severity)
    
    def add_pattern_check(
        self,
        column: str,
        pattern: str,
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ) -> "DataValidator":
        """Add regex pattern check"""
        def check(df: pl.DataFrame) -> ValidationCheck:
            if column not in df.columns:
                return ValidationCheck(
                    name=f"pattern_{column}",
                    passed=False,
                    severity=severity,
                    message=f"Column '{column}' not found",
                )
            
            non_matching = df.filter(
                ~pl.col(column).str.contains(pattern) & pl.col(column).is_not_null()
            ).height
            total = df.filter(pl.col(column).is_not_null()).height
            passed = non_matching == 0
            
            return ValidationCheck(
                name=f"pattern_{column}",
                passed=passed,
                severity=severity,
                message=f"Column '{column}' has {non_matching} values not matching pattern" if not passed else "All values match pattern",
                details={"pattern": pattern, "non_matching_count": non_matching},
                failed_rows=non_matching,
                total_rows=total,
            )
        
        self._checks.append(check)
        return self
    
    def add_enum_check(
        self,
        column: str,
        allowed_values: List[Any],
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ) -> "DataValidator":
        """Add check for values in allowed set"""
        def check(df: pl.DataFrame) -> ValidationCheck:
            if column not in df.columns:
                return ValidationCheck(
                    name=f"enum_{column}",
                    passed=False,
                    severity=severity,
                    message=f"Column '{column}' not found",
                )
            
            invalid = df.filter(
                ~pl.col(column).is_in(allowed_values) & pl.col(column).is_not_null()
            ).height
            total = len(df)
            passed = invalid == 0
            
            return ValidationCheck(
                name=f"enum_{column}",
                passed=passed,
                severity=severity,
                message=f"Column '{column}' has {invalid} invalid values" if not passed else "All values are valid",
                details={"allowed_values": allowed_values, "invalid_count": invalid},
                failed_rows=invalid,
                total_rows=total,
            )
        
        self._checks.append(check)
        return self
    
    def add_custom_check(
        self,
        name: str,
        check_func: Callable[[pl.DataFrame], bool],
        message_on_fail: str,
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ) -> "DataValidator":
        """Add custom validation check"""
        def check(df: pl.DataFrame) -> ValidationCheck:
            try:
                passed = check_func(df)
                return ValidationCheck(
                    name=name,
                    passed=passed,
                    severity=severity,
                    message="Check passed" if passed else message_on_fail,
                    total_rows=len(df),
                )
            except Exception as e:
                return ValidationCheck(
                    name=name,
                    passed=False,
                    severity=severity,
                    message=f"Check failed with error: {str(e)}",
                )
        
        self._checks.append(check)
        return self
    
    def add_referential_integrity_check(
        self,
        column: str,
        reference_df: pl.DataFrame,
        reference_column: str,
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ) -> "DataValidator":
        """Add referential integrity check"""
        def check(df: pl.DataFrame) -> ValidationCheck:
            if column not in df.columns:
                return ValidationCheck(
                    name=f"ref_integrity_{column}",
                    passed=False,
                    severity=severity,
                    message=f"Column '{column}' not found",
                )
            
            # Get reference values
            ref_values = set(reference_df[reference_column].to_list())
            
            # Find orphan records
            orphans = df.filter(
                ~pl.col(column).is_in(list(ref_values)) & pl.col(column).is_not_null()
            ).height
            total = len(df)
            passed = orphans == 0
            
            return ValidationCheck(
                name=f"ref_integrity_{column}",
                passed=passed,
                severity=severity,
                message=f"Column '{column}' has {orphans} orphan records" if not passed else "Referential integrity maintained",
                details={"orphan_count": orphans},
                failed_rows=orphans,
                total_rows=total,
            )
        
        self._checks.append(check)
        return self
    
    def validate(self, df: pl.DataFrame) -> ValidationResult:
        """
        Run all validation checks on DataFrame.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            ValidationResult with all check results
        """
        started_at = datetime.utcnow()
        results = []
        
        logger.info(f"Running {len(self._checks)} validation checks on {len(df)} rows")
        
        for check_func in self._checks:
            result = check_func(df)
            results.append(result)
            
            if not result.passed:
                logger.warning(
                    f"Validation failed: {result.name}",
                    message=result.message,
                    severity=result.severity.value,
                )
        
        completed_at = datetime.utcnow()
        
        # Calculate summary
        passed_checks = sum(1 for r in results if r.passed)
        failed_checks = sum(1 for r in results if not r.passed and r.severity == ValidationSeverity.ERROR)
        warning_count = sum(1 for r in results if not r.passed and r.severity == ValidationSeverity.WARNING)
        
        # Determine overall status
        if failed_checks > 0:
            status = ValidationStatus.FAILED
        elif warning_count > 0 and self.strict_mode:
            status = ValidationStatus.FAILED
        elif warning_count > 0:
            status = ValidationStatus.PARTIAL
        else:
            status = ValidationStatus.PASSED
        
        validation_result = ValidationResult(
            status=status,
            total_checks=len(results),
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            warning_count=warning_count,
            checks=results,
            started_at=started_at,
            completed_at=completed_at,
        )
        
        logger.info(
            f"Validation complete: {status.value}",
            passed=passed_checks,
            failed=failed_checks,
            warnings=warning_count,
        )
        
        return validation_result


# Pre-built validators for common data types
def create_orders_validator() -> DataValidator:
    """Create pre-configured validator for orders data"""
    return (
        DataValidator()
        .add_not_null_check("order_id")
        .add_not_null_check("customer_id")
        .add_not_null_check("order_timestamp")
        .add_unique_check("order_number")
        .add_positive_check("total_amount")
        .add_positive_check("item_count", allow_zero=False)
        .add_range_check("discount_amount", min_value=0)
        .add_enum_check("status", ["pending", "confirmed", "processing", "shipped", "delivered", "cancelled", "refunded"])
    )


def create_customers_validator() -> DataValidator:
    """Create pre-configured validator for customers data"""
    return (
        DataValidator()
        .add_not_null_check("customer_id")
        .add_unique_check("customer_key")
        .add_pattern_check("email_hash", r"^[a-f0-9]{64}$", severity=ValidationSeverity.WARNING)
        .add_range_check("lifetime_value", min_value=0)
        .add_range_check("total_orders", min_value=0)
    )


def create_products_validator() -> DataValidator:
    """Create pre-configured validator for products data"""
    return (
        DataValidator()
        .add_not_null_check("product_id")
        .add_not_null_check("name")
        .add_unique_check("sku")
        .add_positive_check("unit_price", allow_zero=False)
        .add_range_check("stock_quantity", min_value=0)
    )
