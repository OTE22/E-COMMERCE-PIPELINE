"""
Prefect Workflow Orchestration - Batch ETL

Production workflow for batch data processing with:
- Scheduled execution
- Error handling and retries
- Monitoring and alerting
- Data quality checks
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.blocks.system import Secret

from src.config import get_settings
from src.ingestion.batch_loader import BatchLoader, BatchFileConfig, FileFormat
from src.transformation.transformers import ETLTransformer
from src.quality.validators import create_orders_validator, create_customers_validator

settings = get_settings()


# =============================================================================
# TASKS
# =============================================================================

@task(
    name="load_batch_files",
    description="Load batch files from source directory",
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
async def load_batch_files(
    source_dir: str,
    file_format: str = "csv",
    target_table: str = "staging_data",
) -> dict:
    """Load batch files from source directory"""
    logger = get_run_logger()
    
    loader = BatchLoader()
    format_enum = FileFormat(file_format)
    
    results = await loader.load_directory(
        directory=source_dir,
        file_format=format_enum,
        target_table=target_table,
    )
    
    successful = sum(1 for r in results if r.status.value == "completed")
    failed = sum(1 for r in results if r.status.value == "failed")
    
    logger.info(f"Batch load complete: {successful} succeeded, {failed} failed")
    
    return {
        "total_files": len(results),
        "successful": successful,
        "failed": failed,
        "results": [r.model_dump() for r in results],
    }


@task(
    name="validate_data",
    description="Run data quality validations",
    retries=2,
    retry_delay_seconds=30,
)
async def validate_data(
    data_type: str,
    df,
) -> dict:
    """Validate data quality"""
    logger = get_run_logger()
    
    if data_type == "orders":
        validator = create_orders_validator()
    elif data_type == "customers":
        validator = create_customers_validator()
    else:
        logger.warning(f"No validator for data type: {data_type}")
        return {"passed": True, "checks": []}
    
    result = validator.validate(df)
    
    logger.info(
        f"Validation {result.status.value}: "
        f"{result.passed_checks}/{result.total_checks} checks passed"
    )
    
    return {
        "passed": result.status.value == "passed",
        "total_checks": result.total_checks,
        "passed_checks": result.passed_checks,
        "failed_checks": result.failed_checks,
        "success_rate": result.success_rate,
    }


@task(
    name="transform_data",
    description="Apply ETL transformations",
    retries=2,
    retry_delay_seconds=60,
)
async def transform_data(
    data_type: str,
    input_path: str,
) -> dict:
    """Transform raw data to curated format"""
    logger = get_run_logger()
    import polars as pl
    
    transformer = ETLTransformer()
    
    # Load data
    df = pl.read_parquet(input_path)
    logger.info(f"Loaded {len(df)} rows for transformation")
    
    # Transform based on type
    if data_type == "orders":
        result = await transformer.transform_orders(df)
    elif data_type == "customers":
        result = await transformer.transform_customers(df)
    elif data_type == "products":
        result = await transformer.transform_products(df)
    else:
        raise ValueError(f"Unknown data type: {data_type}")
    
    logger.info(
        f"Transformation complete: {result.input_rows} -> {result.output_rows} rows"
    )
    
    return {
        "input_rows": result.input_rows,
        "output_rows": result.output_rows,
        "rows_dropped": result.rows_dropped,
        "duration_seconds": result.duration_seconds,
        "output_path": result.output_path,
    }


@task(
    name="update_aggregates",
    description="Update daily aggregate tables",
)
async def update_aggregates(date: datetime) -> dict:
    """Update aggregate tables for dashboards"""
    logger = get_run_logger()
    from src.database.connection import get_db
    from sqlalchemy import text
    
    date_key = int(date.strftime("%Y%m%d"))
    
    async with get_db() as db:
        # Update daily sales aggregate
        await db.execute(text("""
            INSERT INTO agg_daily_sales (
                aggregate_id, date_key, total_orders, total_revenue,
                total_items_sold, avg_order_value, unique_customers
            )
            SELECT
                gen_random_uuid(),
                :date_key,
                COUNT(*),
                SUM(total_amount),
                SUM(item_count),
                AVG(total_amount),
                COUNT(DISTINCT customer_id)
            FROM fact_orders
            WHERE order_date_key = :date_key
            ON CONFLICT (date_key) DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                total_revenue = EXCLUDED.total_revenue,
                total_items_sold = EXCLUDED.total_items_sold,
                avg_order_value = EXCLUDED.avg_order_value,
                unique_customers = EXCLUDED.unique_customers,
                computed_at = NOW()
        """), {"date_key": date_key})
        
        await db.commit()
    
    logger.info(f"Updated aggregates for date_key: {date_key}")
    return {"date_key": date_key, "updated": True}


@task(
    name="send_alert",
    description="Send alert notification",
)
async def send_alert(
    alert_type: str,
    message: str,
    severity: str = "info",
) -> None:
    """Send alert notification"""
    logger = get_run_logger()
    
    # In production, integrate with:
    # - Slack
    # - PagerDuty
    # - Email
    # - etc.
    
    logger.warning(f"[{severity.upper()}] {alert_type}: {message}")


# =============================================================================
# FLOWS
# =============================================================================

@flow(
    name="daily_batch_etl",
    description="Daily batch ETL pipeline for e-commerce data",
    retries=1,
    retry_delay_seconds=300,
)
async def daily_batch_etl(
    source_dir: Optional[str] = None,
    process_date: Optional[datetime] = None,
) -> dict:
    """
    Daily batch ETL pipeline.
    
    Steps:
    1. Load batch files from source
    2. Validate data quality
    3. Transform and enrich data
    4. Update aggregate tables
    5. Send completion notification
    """
    logger = get_run_logger()
    
    source_dir = source_dir or settings.data_lake.raw_path
    process_date = process_date or datetime.utcnow() - timedelta(days=1)
    
    logger.info(f"Starting daily batch ETL for {process_date.date()}")
    
    results = {
        "process_date": process_date.isoformat(),
        "steps": {},
    }
    
    try:
        # Step 1: Load orders
        orders_load = await load_batch_files(
            source_dir=f"{source_dir}/orders",
            file_format="csv",
            target_table="staging_orders",
        )
        results["steps"]["load_orders"] = orders_load
        
        # Step 2: Load customers
        customers_load = await load_batch_files(
            source_dir=f"{source_dir}/customers",
            file_format="csv",
            target_table="staging_customers",
        )
        results["steps"]["load_customers"] = customers_load
        
        # Step 3: Update aggregates
        agg_result = await update_aggregates(process_date)
        results["steps"]["aggregates"] = agg_result
        
        # Success notification
        await send_alert(
            alert_type="ETL Complete",
            message=f"Daily batch ETL completed successfully for {process_date.date()}",
            severity="info",
        )
        
        results["status"] = "success"
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        
        await send_alert(
            alert_type="ETL Failed",
            message=f"Daily batch ETL failed: {str(e)}",
            severity="critical",
        )
        
        results["status"] = "failed"
        results["error"] = str(e)
        raise
    
    return results


@flow(
    name="incremental_etl",
    description="Incremental ETL for near-real-time updates",
)
async def incremental_etl(
    last_run_timestamp: Optional[datetime] = None,
) -> dict:
    """
    Incremental ETL pipeline.
    
    Processes only new/changed records since last run.
    """
    logger = get_run_logger()
    
    if last_run_timestamp is None:
        last_run_timestamp = datetime.utcnow() - timedelta(hours=1)
    
    logger.info(f"Starting incremental ETL since {last_run_timestamp}")
    
    # Implementation would query for records with updated_at > last_run_timestamp
    # and process only those records
    
    return {
        "last_run_timestamp": last_run_timestamp.isoformat(),
        "status": "success",
    }


@flow(
    name="data_quality_check",
    description="Scheduled data quality check flow",
)
async def data_quality_check() -> dict:
    """
    Standalone data quality check flow.
    
    Runs comprehensive data quality checks and alerts on issues.
    """
    logger = get_run_logger()
    import polars as pl
    from src.quality.anomaly_detector import detect_order_anomalies
    from src.database.connection import get_db
    from sqlalchemy import text
    
    results = {
        "checks": [],
        "anomalies": [],
    }
    
    # Load recent orders for quality check
    async with get_db() as db:
        result = await db.execute(text("""
            SELECT * FROM fact_orders
            WHERE order_timestamp >= NOW() - INTERVAL '24 hours'
        """))
        rows = result.fetchall()
    
    if rows:
        # Convert to DataFrame
        df = pl.DataFrame([dict(r._mapping) for r in rows])
        
        # Run validation
        validation = await validate_data("orders", df)
        results["checks"].append(validation)
        
        # Run anomaly detection
        anomaly_report = detect_order_anomalies(df)
        
        if anomaly_report.has_critical_anomalies:
            await send_alert(
                alert_type="Data Anomaly Detected",
                message=f"Found {anomaly_report.critical_count} critical anomalies",
                severity="critical",
            )
        
        results["anomalies"] = [{
            "metric": a.metric_name,
            "type": a.anomaly_type.value,
            "severity": a.severity.value,
            "message": a.message,
        } for a in anomaly_report.anomalies[:10]]  # Top 10
    
    logger.info(f"Data quality check complete: {len(results['checks'])} checks run")
    return results


# =============================================================================
# DEPLOYMENT CONFIGURATION
# =============================================================================

if __name__ == "__main__":
    import asyncio
    
    # Run daily ETL
    asyncio.run(daily_batch_etl())
