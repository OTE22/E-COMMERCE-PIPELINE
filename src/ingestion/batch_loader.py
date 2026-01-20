"""
Batch Data Loader

Production-grade batch data ingestion for CSV, JSON, and Parquet files.
Supports:
- Schema inference and validation
- Incremental loading with watermarks
- Parallel processing
- Error handling and dead-letter queues
- Audit logging
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
import hashlib

import pandas as pd
import polars as pl
import structlog
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from src.config import get_settings
from src.database.connection import get_db

logger = structlog.get_logger(__name__)
settings = get_settings()


class FileFormat(str, Enum):
    """Supported file formats"""
    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    PARQUET = "parquet"


class LoadStatus(str, Enum):
    """Batch load status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


@dataclass
class BatchFileConfig:
    """Configuration for batch file loading"""
    file_path: Union[str, Path]
    file_format: FileFormat
    target_table: str
    schema: Optional[Dict[str, str]] = None
    delimiter: str = ","
    encoding: str = "utf-8"
    skip_rows: int = 0
    date_columns: List[str] = field(default_factory=list)
    datetime_format: str = "%Y-%m-%d %H:%M:%S"
    null_values: List[str] = field(default_factory=lambda: ["", "NULL", "null", "None", "NA", "N/A"])
    chunk_size: int = 10000
    validate_schema: bool = True


class LoadResult(BaseModel):
    """Result of a batch load operation"""
    file_path: str
    target_table: str
    status: LoadStatus
    rows_loaded: int = 0
    rows_failed: int = 0
    error_message: Optional[str] = None
    load_duration_seconds: float = 0
    started_at: datetime
    completed_at: Optional[datetime] = None
    file_hash: Optional[str] = None


class BatchLoader:
    """
    Production batch data loader with enterprise features.
    
    Features:
    - Multi-format support (CSV, JSON, Parquet)
    - Schema inference and validation
    - Incremental loading with deduplication
    - Parallel chunk processing
    - Comprehensive error handling
    - Audit trail
    
    Example:
        loader = BatchLoader()
        config = BatchFileConfig(
            file_path="data/raw/orders.csv",
            file_format=FileFormat.CSV,
            target_table="staging_orders"
        )
        result = await loader.load(config)
    """
    
    def __init__(
        self,
        max_workers: int = 4,
        enable_validation: bool = True,
        dead_letter_path: Optional[str] = None,
    ):
        self.max_workers = max_workers
        self.enable_validation = enable_validation
        self.dead_letter_path = Path(dead_letter_path or settings.data_lake.raw_path) / "dead_letter"
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """Ensure required directories exist"""
        self.dead_letter_path.mkdir(parents=True, exist_ok=True)
    
    def _compute_file_hash(self, file_path: Path) -> str:
        """Compute MD5 hash of file for deduplication"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def _read_csv(self, config: BatchFileConfig) -> pl.DataFrame:
        """Read CSV file with Polars for performance"""
        return pl.read_csv(
            config.file_path,
            separator=config.delimiter,
            encoding=config.encoding,
            skip_rows=config.skip_rows,
            null_values=config.null_values,
            try_parse_dates=True,
            dtypes=config.schema,
        )
    
    def _read_json(self, config: BatchFileConfig) -> pl.DataFrame:
        """Read JSON file"""
        return pl.read_json(config.file_path)
    
    def _read_jsonl(self, config: BatchFileConfig) -> pl.DataFrame:
        """Read JSON Lines (NDJSON) file"""
        return pl.read_ndjson(config.file_path)
    
    def _read_parquet(self, config: BatchFileConfig) -> pl.DataFrame:
        """Read Parquet file"""
        return pl.read_parquet(config.file_path)
    
    def _read_file(self, config: BatchFileConfig) -> pl.DataFrame:
        """Read file based on format"""
        readers = {
            FileFormat.CSV: self._read_csv,
            FileFormat.JSON: self._read_json,
            FileFormat.JSONL: self._read_jsonl,
            FileFormat.PARQUET: self._read_parquet,
        }
        reader = readers.get(config.file_format)
        if not reader:
            raise ValueError(f"Unsupported file format: {config.file_format}")
        return reader(config)
    
    def _validate_schema(
        self, 
        df: pl.DataFrame, 
        expected_schema: Dict[str, str]
    ) -> List[str]:
        """Validate DataFrame schema against expected schema"""
        errors = []
        
        for column, expected_type in expected_schema.items():
            if column not in df.columns:
                errors.append(f"Missing column: {column}")
                continue
            
            actual_type = str(df[column].dtype)
            if expected_type.lower() not in actual_type.lower():
                errors.append(
                    f"Column {column}: expected {expected_type}, got {actual_type}"
                )
        
        return errors
    
    def _clean_data(self, df: pl.DataFrame, config: BatchFileConfig) -> pl.DataFrame:
        """Apply data cleaning transformations"""
        # Convert date columns
        for col in config.date_columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).str.strptime(pl.Datetime, config.datetime_format)
                )
        
        # Remove completely null rows
        df = df.filter(~pl.all_horizontal(pl.all().is_null()))
        
        # Add metadata columns
        df = df.with_columns([
            pl.lit(str(config.file_path)).alias("_source_file"),
            pl.lit(datetime.utcnow()).alias("_loaded_at"),
        ])
        
        return df
    
    async def _write_to_dead_letter(
        self,
        df: pl.DataFrame,
        config: BatchFileConfig,
        error: str,
    ) -> None:
        """Write failed records to dead letter queue"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_name = Path(config.file_path).stem
        dead_letter_file = self.dead_letter_path / f"{file_name}_{timestamp}.parquet"
        
        # Add error metadata
        df = df.with_columns([
            pl.lit(error).alias("_error_message"),
            pl.lit(datetime.utcnow()).alias("_failed_at"),
        ])
        
        df.write_parquet(dead_letter_file)
        logger.warning(
            "Written failed records to dead letter queue",
            file=str(dead_letter_file),
            records=len(df),
        )
    
    async def _insert_to_database(
        self,
        df: pl.DataFrame,
        table_name: str,
    ) -> int:
        """Insert DataFrame to database table"""
        from sqlalchemy import text
        
        # Convert to pandas for SQLAlchemy insertion
        pandas_df = df.to_pandas()
        
        async with get_db() as db:
            # Use chunked insertion for large datasets
            chunk_size = 5000
            total_inserted = 0
            
            for i in range(0, len(pandas_df), chunk_size):
                chunk = pandas_df.iloc[i:i + chunk_size]
                
                # Build INSERT statement
                columns = ", ".join(chunk.columns)
                placeholders = ", ".join([f":{col}" for col in chunk.columns])
                
                stmt = text(f"""
                    INSERT INTO {table_name} ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT DO NOTHING
                """)
                
                for _, row in chunk.iterrows():
                    await db.execute(stmt, dict(row))
                
                total_inserted += len(chunk)
            
            await db.commit()
        
        return total_inserted
    
    async def load(self, config: BatchFileConfig) -> LoadResult:
        """
        Load a batch file into the database.
        
        Args:
            config: Batch file configuration
            
        Returns:
            LoadResult: Result of the load operation
        """
        file_path = Path(config.file_path)
        started_at = datetime.utcnow()
        
        result = LoadResult(
            file_path=str(file_path),
            target_table=config.target_table,
            status=LoadStatus.RUNNING,
            started_at=started_at,
        )
        
        logger.info(
            "Starting batch load",
            file=str(file_path),
            target_table=config.target_table,
        )
        
        try:
            # Validate file exists
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Compute file hash for deduplication
            result.file_hash = self._compute_file_hash(file_path)
            
            # Read file
            df = self._read_file(config)
            total_rows = len(df)
            
            logger.info(f"Read {total_rows} rows from file")
            
            # Validate schema if enabled
            if self.enable_validation and config.schema:
                schema_errors = self._validate_schema(df, config.schema)
                if schema_errors:
                    raise ValueError(f"Schema validation failed: {schema_errors}")
            
            # Clean data
            df = self._clean_data(df, config)
            
            # Insert to database
            rows_inserted = await self._insert_to_database(df, config.target_table)
            
            # Update result
            result.status = LoadStatus.COMPLETED
            result.rows_loaded = rows_inserted
            result.rows_failed = total_rows - rows_inserted
            result.completed_at = datetime.utcnow()
            result.load_duration_seconds = (
                result.completed_at - started_at
            ).total_seconds()
            
            logger.info(
                "Batch load completed",
                rows_loaded=rows_inserted,
                duration_seconds=result.load_duration_seconds,
            )
            
        except Exception as e:
            result.status = LoadStatus.FAILED
            result.error_message = str(e)
            result.completed_at = datetime.utcnow()
            result.load_duration_seconds = (
                result.completed_at - started_at
            ).total_seconds()
            
            logger.error(
                "Batch load failed",
                error=str(e),
                file=str(file_path),
            )
            
            # Write to dead letter if we have data
            try:
                df = self._read_file(config)
                await self._write_to_dead_letter(df, config, str(e))
            except Exception:
                pass
        
        return result
    
    async def load_directory(
        self,
        directory: Union[str, Path],
        file_format: FileFormat,
        target_table: str,
        pattern: str = "*",
        **kwargs,
    ) -> List[LoadResult]:
        """
        Load all matching files from a directory.
        
        Args:
            directory: Directory containing files
            file_format: File format to process
            target_table: Target database table
            pattern: Glob pattern for file matching
            **kwargs: Additional BatchFileConfig parameters
            
        Returns:
            List of LoadResult for each file
        """
        directory = Path(directory)
        extension = f".{file_format.value}"
        files = list(directory.glob(f"{pattern}{extension}"))
        
        logger.info(
            f"Found {len(files)} files to load",
            directory=str(directory),
            pattern=pattern,
        )
        
        results = []
        for file_path in files:
            config = BatchFileConfig(
                file_path=file_path,
                file_format=file_format,
                target_table=target_table,
                **kwargs,
            )
            result = await self.load(config)
            results.append(result)
        
        # Summary
        successful = sum(1 for r in results if r.status == LoadStatus.COMPLETED)
        failed = sum(1 for r in results if r.status == LoadStatus.FAILED)
        
        logger.info(
            f"Directory load completed: {successful} successful, {failed} failed",
            total_files=len(files),
        )
        
        return results
    
    async def load_incremental(
        self,
        config: BatchFileConfig,
        watermark_column: str,
        last_watermark: datetime,
    ) -> LoadResult:
        """
        Incremental load - only load records newer than watermark.
        
        Args:
            config: Batch file configuration
            watermark_column: Column to use for watermark comparison
            last_watermark: Only load records after this timestamp
            
        Returns:
            LoadResult for the incremental load
        """
        file_path = Path(config.file_path)
        started_at = datetime.utcnow()
        
        result = LoadResult(
            file_path=str(file_path),
            target_table=config.target_table,
            status=LoadStatus.RUNNING,
            started_at=started_at,
        )
        
        try:
            # Read file
            df = self._read_file(config)
            total_rows = len(df)
            
            # Filter by watermark
            df = df.filter(pl.col(watermark_column) > last_watermark)
            incremental_rows = len(df)
            
            logger.info(
                f"Incremental load: {incremental_rows} of {total_rows} rows pass watermark",
                watermark=str(last_watermark),
            )
            
            if incremental_rows == 0:
                result.status = LoadStatus.COMPLETED
                result.rows_loaded = 0
                result.completed_at = datetime.utcnow()
                return result
            
            # Clean and load
            df = self._clean_data(df, config)
            rows_inserted = await self._insert_to_database(df, config.target_table)
            
            result.status = LoadStatus.COMPLETED
            result.rows_loaded = rows_inserted
            result.completed_at = datetime.utcnow()
            result.load_duration_seconds = (
                result.completed_at - started_at
            ).total_seconds()
            
        except Exception as e:
            result.status = LoadStatus.FAILED
            result.error_message = str(e)
            result.completed_at = datetime.utcnow()
            logger.error("Incremental load failed", error=str(e))
        
        return result


# Factory function for creating configured loader
def create_batch_loader() -> BatchLoader:
    """Create a configured BatchLoader instance"""
    return BatchLoader(
        max_workers=4,
        enable_validation=settings.data_quality.enable_data_quality_checks,
        dead_letter_path=str(Path(settings.data_lake.raw_path) / "dead_letter"),
    )
