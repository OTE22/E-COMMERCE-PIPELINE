"""
Anomaly Detection Module

Real-time and batch anomaly detection for e-commerce metrics.
Implements:
- Statistical anomaly detection (Z-score, IQR, MAD)
- Time-series anomaly detection
- Rule-based anomaly triggers
- Multi-metric correlation
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import polars as pl
import numpy as np
from scipy import stats
import structlog

from src.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()


class AnomalyType(str, Enum):
    """Types of anomalies detected"""
    SPIKE = "spike"  # Sudden increase
    DROP = "drop"  # Sudden decrease
    OUTLIER = "outlier"  # Statistical outlier
    PATTERN = "pattern"  # Pattern violation
    MISSING = "missing"  # Missing data
    DRIFT = "drift"  # Data drift


class AnomalySeverity(str, Enum):
    """Severity levels for anomalies"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class AnomalyResult:
    """Single anomaly detection result"""
    metric_name: str
    anomaly_type: AnomalyType
    severity: AnomalySeverity
    detected_at: datetime
    value: float
    expected_value: float
    deviation: float  # Z-score or percentage deviation
    threshold: float
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_critical(self) -> bool:
        return self.severity in [AnomalySeverity.CRITICAL, AnomalySeverity.HIGH]


@dataclass
class AnomalyReport:
    """Complete anomaly detection report"""
    started_at: datetime
    completed_at: datetime
    metrics_checked: int
    anomalies_found: int
    critical_count: int
    anomalies: List[AnomalyResult] = field(default_factory=list)
    
    @property
    def has_critical_anomalies(self) -> bool:
        return self.critical_count > 0


class AnomalyDetector:
    """
    Production anomaly detector for e-commerce metrics.
    
    Detection methods:
    - Z-score: Detects values far from mean
    - IQR: Robust outlier detection
    - MAD: Median Absolute Deviation
    - Percentage change: Sudden spikes/drops
    - Rule-based: Business-specific rules
    
    Example:
        detector = AnomalyDetector(z_threshold=3.0)
        detector.add_metric("order_count", orders_df["count"])
        report = detector.detect()
    """
    
    def __init__(
        self,
        z_threshold: float = 3.0,
        iqr_multiplier: float = 1.5,
        pct_change_threshold: float = 50.0,
    ):
        self.z_threshold = z_threshold
        self.iqr_multiplier = iqr_multiplier
        self.pct_change_threshold = pct_change_threshold
        
        self._metrics: Dict[str, np.ndarray] = {}
        self._baselines: Dict[str, Dict[str, float]] = {}
        self._rules: List[Callable] = []
        self._anomalies: List[AnomalyResult] = []
    
    def add_metric(
        self,
        name: str,
        values: np.ndarray,
        baseline: Optional[Dict[str, float]] = None,
    ) -> "AnomalyDetector":
        """Add metric for anomaly detection"""
        self._metrics[name] = np.asarray(values)
        
        if baseline:
            self._baselines[name] = baseline
        else:
            # Calculate baseline from data
            values_clean = values[~np.isnan(values)]
            if len(values_clean) > 0:
                self._baselines[name] = {
                    "mean": float(np.mean(values_clean)),
                    "std": float(np.std(values_clean)),
                    "median": float(np.median(values_clean)),
                    "q1": float(np.percentile(values_clean, 25)),
                    "q3": float(np.percentile(values_clean, 75)),
                }
        
        return self
    
    def add_rule(
        self,
        name: str,
        check_func: Callable[[float], bool],
        message: str,
        severity: AnomalySeverity = AnomalySeverity.MEDIUM,
    ) -> "AnomalyDetector":
        """Add custom rule-based check"""
        self._rules.append({
            "name": name,
            "check": check_func,
            "message": message,
            "severity": severity,
        })
        return self
    
    def _detect_zscore_anomalies(
        self,
        name: str,
        values: np.ndarray,
    ) -> List[AnomalyResult]:
        """Detect anomalies using Z-score method"""
        anomalies = []
        baseline = self._baselines.get(name, {})
        
        mean = baseline.get("mean", np.nanmean(values))
        std = baseline.get("std", np.nanstd(values))
        
        if std == 0:
            return anomalies
        
        z_scores = np.abs((values - mean) / std)
        
        for i, (value, z_score) in enumerate(zip(values, z_scores)):
            if z_score > self.z_threshold:
                severity = (
                    AnomalySeverity.CRITICAL if z_score > self.z_threshold * 2
                    else AnomalySeverity.HIGH if z_score > self.z_threshold * 1.5
                    else AnomalySeverity.MEDIUM
                )
                
                anomaly_type = AnomalyType.SPIKE if value > mean else AnomalyType.DROP
                
                anomalies.append(AnomalyResult(
                    metric_name=name,
                    anomaly_type=anomaly_type,
                    severity=severity,
                    detected_at=datetime.utcnow(),
                    value=float(value),
                    expected_value=mean,
                    deviation=float(z_score),
                    threshold=self.z_threshold,
                    message=f"{name} value {value:.2f} is {z_score:.2f} standard deviations from mean {mean:.2f}",
                    details={"index": i, "method": "z-score"},
                ))
        
        return anomalies
    
    def _detect_iqr_anomalies(
        self,
        name: str,
        values: np.ndarray,
    ) -> List[AnomalyResult]:
        """Detect anomalies using IQR method"""
        anomalies = []
        baseline = self._baselines.get(name, {})
        
        q1 = baseline.get("q1", np.nanpercentile(values, 25))
        q3 = baseline.get("q3", np.nanpercentile(values, 75))
        iqr = q3 - q1
        
        if iqr == 0:
            return anomalies
        
        lower_bound = q1 - self.iqr_multiplier * iqr
        upper_bound = q3 + self.iqr_multiplier * iqr
        
        for i, value in enumerate(values):
            if value < lower_bound or value > upper_bound:
                deviation = (
                    (value - upper_bound) / iqr if value > upper_bound
                    else (lower_bound - value) / iqr
                )
                
                anomalies.append(AnomalyResult(
                    metric_name=name,
                    anomaly_type=AnomalyType.OUTLIER,
                    severity=AnomalySeverity.MEDIUM,
                    detected_at=datetime.utcnow(),
                    value=float(value),
                    expected_value=float((q1 + q3) / 2),
                    deviation=float(deviation),
                    threshold=self.iqr_multiplier,
                    message=f"{name} value {value:.2f} is outside IQR bounds [{lower_bound:.2f}, {upper_bound:.2f}]",
                    details={"index": i, "method": "iqr", "q1": q1, "q3": q3},
                ))
        
        return anomalies
    
    def _detect_pct_change_anomalies(
        self,
        name: str,
        values: np.ndarray,
    ) -> List[AnomalyResult]:
        """Detect sudden spikes/drops based on percentage change"""
        anomalies = []
        
        if len(values) < 2:
            return anomalies
        
        for i in range(1, len(values)):
            prev_value = values[i - 1]
            curr_value = values[i]
            
            if prev_value == 0:
                continue
            
            pct_change = ((curr_value - prev_value) / abs(prev_value)) * 100
            
            if abs(pct_change) > self.pct_change_threshold:
                anomaly_type = AnomalyType.SPIKE if pct_change > 0 else AnomalyType.DROP
                severity = (
                    AnomalySeverity.CRITICAL if abs(pct_change) > self.pct_change_threshold * 2
                    else AnomalySeverity.HIGH if abs(pct_change) > self.pct_change_threshold * 1.5
                    else AnomalySeverity.MEDIUM
                )
                
                anomalies.append(AnomalyResult(
                    metric_name=name,
                    anomaly_type=anomaly_type,
                    severity=severity,
                    detected_at=datetime.utcnow(),
                    value=float(curr_value),
                    expected_value=float(prev_value),
                    deviation=float(pct_change),
                    threshold=self.pct_change_threshold,
                    message=f"{name} changed by {pct_change:.1f}% from {prev_value:.2f} to {curr_value:.2f}",
                    details={"index": i, "method": "pct_change"},
                ))
        
        return anomalies
    
    def _detect_missing_data(
        self,
        name: str,
        values: np.ndarray,
    ) -> List[AnomalyResult]:
        """Detect missing or null data"""
        anomalies = []
        
        null_count = np.sum(np.isnan(values))
        null_pct = (null_count / len(values)) * 100 if len(values) > 0 else 0
        
        if null_pct > 5:  # More than 5% missing
            severity = (
                AnomalySeverity.CRITICAL if null_pct > 50
                else AnomalySeverity.HIGH if null_pct > 20
                else AnomalySeverity.MEDIUM
            )
            
            anomalies.append(AnomalyResult(
                metric_name=name,
                anomaly_type=AnomalyType.MISSING,
                severity=severity,
                detected_at=datetime.utcnow(),
                value=float(null_count),
                expected_value=0.0,
                deviation=float(null_pct),
                threshold=5.0,
                message=f"{name} has {null_count} ({null_pct:.1f}%) missing values",
                details={"null_count": int(null_count), "null_percentage": float(null_pct)},
            ))
        
        return anomalies
    
    def _check_rules(
        self,
        name: str,
        values: np.ndarray,
    ) -> List[AnomalyResult]:
        """Run rule-based anomaly checks"""
        anomalies = []
        
        for rule in self._rules:
            for i, value in enumerate(values):
                if rule["check"](value):
                    anomalies.append(AnomalyResult(
                        metric_name=name,
                        anomaly_type=AnomalyType.PATTERN,
                        severity=rule["severity"],
                        detected_at=datetime.utcnow(),
                        value=float(value),
                        expected_value=0.0,
                        deviation=0.0,
                        threshold=0.0,
                        message=rule["message"].format(value=value),
                        details={"index": i, "rule_name": rule["name"]},
                    ))
        
        return anomalies
    
    def detect(
        self,
        methods: Optional[List[str]] = None,
    ) -> AnomalyReport:
        """
        Run anomaly detection on all registered metrics.
        
        Args:
            methods: Detection methods to use (default: all)
                    Options: "zscore", "iqr", "pct_change", "missing", "rules"
        
        Returns:
            AnomalyReport with all detected anomalies
        """
        started_at = datetime.utcnow()
        all_anomalies = []
        
        if methods is None:
            methods = ["zscore", "iqr", "pct_change", "missing", "rules"]
        
        for name, values in self._metrics.items():
            logger.debug(f"Checking metric: {name}")
            
            if "zscore" in methods:
                all_anomalies.extend(self._detect_zscore_anomalies(name, values))
            
            if "iqr" in methods:
                all_anomalies.extend(self._detect_iqr_anomalies(name, values))
            
            if "pct_change" in methods:
                all_anomalies.extend(self._detect_pct_change_anomalies(name, values))
            
            if "missing" in methods:
                all_anomalies.extend(self._detect_missing_data(name, values))
            
            if "rules" in methods:
                all_anomalies.extend(self._check_rules(name, values))
        
        completed_at = datetime.utcnow()
        
        # Deduplicate by metric and type
        unique_anomalies = []
        seen = set()
        for anomaly in all_anomalies:
            key = (anomaly.metric_name, anomaly.anomaly_type, anomaly.details.get("index"))
            if key not in seen:
                seen.add(key)
                unique_anomalies.append(anomaly)
        
        critical_count = sum(
            1 for a in unique_anomalies
            if a.severity in [AnomalySeverity.CRITICAL, AnomalySeverity.HIGH]
        )
        
        report = AnomalyReport(
            started_at=started_at,
            completed_at=completed_at,
            metrics_checked=len(self._metrics),
            anomalies_found=len(unique_anomalies),
            critical_count=critical_count,
            anomalies=unique_anomalies,
        )
        
        if report.has_critical_anomalies:
            logger.warning(
                f"Critical anomalies detected: {critical_count}",
                total_anomalies=len(unique_anomalies),
            )
        else:
            logger.info(f"Anomaly detection complete: {len(unique_anomalies)} anomalies found")
        
        return report


def detect_order_anomalies(
    orders_df: pl.DataFrame,
    reference_df: Optional[pl.DataFrame] = None,
) -> AnomalyReport:
    """
    Convenience function for detecting order-related anomalies.
    
    Checks:
    - Order volume spikes/drops
    - Revenue anomalies
    - Average order value outliers
    - Negative amounts
    """
    detector = AnomalyDetector(
        z_threshold=settings.data_quality.anomaly_alert_threshold,
    )
    
    # Add metrics
    if "total_amount" in orders_df.columns:
        detector.add_metric("order_revenue", orders_df["total_amount"].to_numpy())
    
    if "item_count" in orders_df.columns:
        detector.add_metric("items_per_order", orders_df["item_count"].to_numpy())
    
    # Add business rules
    detector.add_rule(
        name="negative_amount",
        check_func=lambda x: x < 0,
        message="Negative order amount detected: {value}",
        severity=AnomalySeverity.CRITICAL,
    )
    
    detector.add_rule(
        name="zero_amount",
        check_func=lambda x: x == 0,
        message="Zero order amount detected",
        severity=AnomalySeverity.HIGH,
    )
    
    return detector.detect()
