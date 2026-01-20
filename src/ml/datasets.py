"""
ML Dataset Builder

Builds production ML datasets for training and inference.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import polars as pl
import numpy as np
from sklearn.model_selection import train_test_split
import structlog

from src.config import get_settings
from .features import FeatureEngineer

logger = structlog.get_logger(__name__)
settings = get_settings()


@dataclass
class DatasetConfig:
    """Configuration for ML dataset building"""
    target: str  # churn, clv, recommendation
    train_start_date: datetime
    train_end_date: datetime
    test_start_date: Optional[datetime] = None
    test_end_date: Optional[datetime] = None
    validation_split: float = 0.2
    random_state: int = 42
    feature_columns: Optional[List[str]] = None
    exclude_columns: Optional[List[str]] = None


@dataclass
class DatasetSplit:
    """Train/validation/test split"""
    X_train: np.ndarray
    y_train: np.ndarray
    X_val: np.ndarray
    y_val: np.ndarray
    X_test: Optional[np.ndarray] = None
    y_test: Optional[np.ndarray] = None
    feature_names: List[str] = None
    metadata: Dict = None


class MLDatasetBuilder:
    """
    Production ML dataset builder.
    
    Creates training and inference datasets for:
    - Customer churn prediction
    - Customer lifetime value prediction
    - Product recommendations
    
    Example:
        builder = MLDatasetBuilder()
        config = DatasetConfig(
            target="churn",
            train_start_date=datetime(2025, 1, 1),
            train_end_date=datetime(2025, 12, 31),
        )
        split = await builder.build_classification_dataset(config, orders_df)
    """
    
    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir or settings.data_lake.curated_path) / "ml_datasets"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.engineer = FeatureEngineer()
    
    def _prepare_features(
        self,
        df: pl.DataFrame,
        config: DatasetConfig,
    ) -> Tuple[np.ndarray, List[str]]:
        """Prepare feature matrix from DataFrame"""
        # Determine feature columns
        if config.feature_columns:
            feature_cols = config.feature_columns
        else:
            # Auto-detect numeric columns
            exclude = set(config.exclude_columns or [])
            exclude.update(["customer_id", "product_id", "order_id"])
            
            feature_cols = [
                col for col, dtype in zip(df.columns, df.dtypes)
                if col not in exclude and dtype in [pl.Float64, pl.Int64, pl.Int32, pl.Float32]
            ]
        
        # Extract features
        X = df.select(feature_cols).to_numpy()
        
        return X, feature_cols
    
    async def build_churn_dataset(
        self,
        orders_df: pl.DataFrame,
        config: DatasetConfig,
    ) -> DatasetSplit:
        """
        Build dataset for churn prediction.
        
        Target: Binary (churned / not churned)
        """
        logger.info("Building churn prediction dataset")
        
        # Filter to training period
        train_orders = orders_df.filter(
            (pl.col("order_timestamp") >= config.train_start_date) &
            (pl.col("order_timestamp") <= config.train_end_date)
        )
        
        # Compute features
        features = self.engineer.compute_customer_features(train_orders)
        features = self.engineer.compute_churn_features(features)
        
        # Prepare target
        target_col = "is_churned"
        
        # Prepare feature matrix
        X, feature_names = self._prepare_features(
            features.drop([target_col, "customer_id"]),
            config,
        )
        y = features[target_col].to_numpy().astype(int)
        
        # Train/validation split
        X_train, X_val, y_train, y_val = train_test_split(
            X, y,
            test_size=config.validation_split,
            random_state=config.random_state,
            stratify=y,
        )
        
        # Build test set if dates provided
        X_test, y_test = None, None
        if config.test_start_date and config.test_end_date:
            test_orders = orders_df.filter(
                (pl.col("order_timestamp") >= config.test_start_date) &
                (pl.col("order_timestamp") <= config.test_end_date)
            )
            test_features = self.engineer.compute_customer_features(test_orders)
            test_features = self.engineer.compute_churn_features(test_features)
            
            X_test, _ = self._prepare_features(
                test_features.drop([target_col, "customer_id"]),
                config,
            )
            y_test = test_features[target_col].to_numpy().astype(int)
        
        split = DatasetSplit(
            X_train=X_train,
            y_train=y_train,
            X_val=X_val,
            y_val=y_val,
            X_test=X_test,
            y_test=y_test,
            feature_names=feature_names,
            metadata={
                "target": "churn",
                "train_samples": len(X_train),
                "val_samples": len(X_val),
                "test_samples": len(X_test) if X_test is not None else 0,
                "churn_rate_train": float(y_train.mean()),
                "created_at": datetime.utcnow().isoformat(),
            },
        )
        
        logger.info(
            f"Churn dataset built: {len(X_train)} train, {len(X_val)} val samples, "
            f"churn rate: {y_train.mean():.2%}"
        )
        
        return split
    
    async def build_clv_dataset(
        self,
        orders_df: pl.DataFrame,
        config: DatasetConfig,
    ) -> DatasetSplit:
        """
        Build dataset for CLV prediction.
        
        Target: Continuous (predicted CLV)
        """
        logger.info("Building CLV prediction dataset")
        
        # Filter to training period
        train_orders = orders_df.filter(
            (pl.col("order_timestamp") >= config.train_start_date) &
            (pl.col("order_timestamp") <= config.train_end_date)
        )
        
        # Compute features
        features = self.engineer.compute_customer_features(train_orders)
        features = self.engineer.compute_clv_features(features)
        
        # Target: projected CLV
        target_col = "projected_clv"
        
        # Prepare feature matrix (exclude target-related columns)
        exclude_cols = [target_col, "customer_id", "historical_clv", "clv_quartile"]
        X, feature_names = self._prepare_features(
            features.drop([c for c in exclude_cols if c in features.columns]),
            config,
        )
        y = features[target_col].fill_null(0).to_numpy()
        
        # Train/validation split
        X_train, X_val, y_train, y_val = train_test_split(
            X, y,
            test_size=config.validation_split,
            random_state=config.random_state,
        )
        
        split = DatasetSplit(
            X_train=X_train,
            y_train=y_train,
            X_val=X_val,
            y_val=y_val,
            feature_names=feature_names,
            metadata={
                "target": "clv",
                "train_samples": len(X_train),
                "val_samples": len(X_val),
                "target_mean": float(y.mean()),
                "target_std": float(y.std()),
                "created_at": datetime.utcnow().isoformat(),
            },
        )
        
        logger.info(
            f"CLV dataset built: {len(X_train)} train, {len(X_val)} val samples, "
            f"mean CLV: ${y.mean():.2f}"
        )
        
        return split
    
    async def build_recommendation_dataset(
        self,
        order_items_df: pl.DataFrame,
        products_df: pl.DataFrame,
        config: DatasetConfig,
    ) -> Dict:
        """
        Build dataset for product recommendations.
        
        Creates:
        - User-item interaction matrix
        - Product feature matrix
        - Co-purchase graph
        """
        logger.info("Building recommendation dataset")
        
        # Filter to training period
        if "order_timestamp" in order_items_df.columns:
            train_items = order_items_df.filter(
                (pl.col("order_timestamp") >= config.train_start_date) &
                (pl.col("order_timestamp") <= config.train_end_date)
            )
        else:
            train_items = order_items_df
        
        # User-item interactions
        interactions = train_items.group_by(["customer_id", "product_id"]).agg([
            pl.col("quantity").sum().alias("purchase_count"),
            pl.col("line_total").sum().alias("total_spent"),
        ])
        
        # Create interaction matrix (sparse representation)
        customer_ids = interactions["customer_id"].unique().to_list()
        product_ids = interactions["product_id"].unique().to_list()
        
        customer_idx = {c: i for i, c in enumerate(customer_ids)}
        product_idx = {p: i for i, p in enumerate(product_ids)}
        
        # Interaction data for sparse matrix
        rows = []
        cols = []
        values = []
        
        for row in interactions.iter_rows(named=True):
            rows.append(customer_idx[row["customer_id"]])
            cols.append(product_idx[row["product_id"]])
            values.append(row["purchase_count"])
        
        # Product features
        product_features = products_df.select([
            "product_id",
            "unit_price",
            "stock_quantity",
            "avg_rating",
        ]).to_pandas()
        
        dataset = {
            "interactions": {
                "rows": rows,
                "cols": cols,
                "values": values,
                "n_users": len(customer_ids),
                "n_items": len(product_ids),
            },
            "customer_ids": customer_ids,
            "product_ids": product_ids,
            "customer_idx": customer_idx,
            "product_idx": product_idx,
            "product_features": product_features,
            "metadata": {
                "n_interactions": len(interactions),
                "n_users": len(customer_ids),
                "n_items": len(product_ids),
                "sparsity": 1 - (len(interactions) / (len(customer_ids) * len(product_ids))),
                "created_at": datetime.utcnow().isoformat(),
            },
        }
        
        logger.info(
            f"Recommendation dataset built: {len(customer_ids)} users, "
            f"{len(product_ids)} products, {len(interactions)} interactions"
        )
        
        return dataset
    
    def save_dataset(
        self,
        split: DatasetSplit,
        name: str,
    ) -> Path:
        """Save dataset to disk"""
        output_path = self.output_dir / name
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save arrays
        np.save(output_path / "X_train.npy", split.X_train)
        np.save(output_path / "y_train.npy", split.y_train)
        np.save(output_path / "X_val.npy", split.X_val)
        np.save(output_path / "y_val.npy", split.y_val)
        
        if split.X_test is not None:
            np.save(output_path / "X_test.npy", split.X_test)
            np.save(output_path / "y_test.npy", split.y_test)
        
        # Save metadata
        import json
        with open(output_path / "metadata.json", "w") as f:
            metadata = split.metadata.copy()
            metadata["feature_names"] = split.feature_names
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Dataset saved to {output_path}")
        return output_path
    
    def load_dataset(self, name: str) -> DatasetSplit:
        """Load dataset from disk"""
        path = self.output_dir / name
        
        import json
        with open(path / "metadata.json", "r") as f:
            metadata = json.load(f)
        
        feature_names = metadata.pop("feature_names", None)
        
        X_test, y_test = None, None
        if (path / "X_test.npy").exists():
            X_test = np.load(path / "X_test.npy")
            y_test = np.load(path / "y_test.npy")
        
        return DatasetSplit(
            X_train=np.load(path / "X_train.npy"),
            y_train=np.load(path / "y_train.npy"),
            X_val=np.load(path / "X_val.npy"),
            y_val=np.load(path / "y_val.npy"),
            X_test=X_test,
            y_test=y_test,
            feature_names=feature_names,
            metadata=metadata,
        )
