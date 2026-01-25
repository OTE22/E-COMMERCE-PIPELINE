import asyncio
import hashlib
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import polars as pl
import structlog
from sqlalchemy import text

from src.database.connection import get_db, init_database
from src.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()

DATA_DIR = Path("/app/data/generated")

from sqlalchemy.dialects.postgresql import insert
from src.database.models import (
    DimDate, DimCustomer, DimProduct, FactOrder, FactOrderItem, FactPageView
)

async def execute_batch_insert(model: Any, records: List[Dict[str, Any]]):
    """Helper to insert batch of records using Core Insert"""
    if not records:
        return
        
    async with get_db() as db:
        chunk_size = 1000
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            
            stmt = insert(model).values(chunk)
            stmt = stmt.on_conflict_do_nothing()
            
            await db.execute(stmt)
        await db.commit()
    logger.info(f"Inserted {len(records)} records into {model.__tablename__}")

async def seed_dim_date(start_year: int = 2023, end_year: int = 2026):
    """Generate and load date dimension"""
    logger.info("Seeding DimDate...")
    # ... (date generation logic is same) ...
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    delta = end_date - start_date
    
    dates = []
    for i in range(delta.days + 1):
        d = start_date + timedelta(days=i)
        date_key = int(d.strftime("%Y%m%d"))
        
        dates.append({
            "date_key": date_key,
            "full_date": d.date(),
            "day_of_week": d.weekday(),
            "day_of_month": d.day,
            "day_of_year": d.timetuple().tm_yday,
            "week_of_year": d.isocalendar()[1],
            "month": d.month,
            "month_name": d.strftime("%B"),
            "quarter": (d.month - 1) // 3 + 1,
            "year": d.year,
            "is_weekend": d.weekday() >= 5,
            "is_holiday": False,
            "fiscal_year": d.year,
            "fiscal_quarter": (d.month - 1) // 3 + 1
        })
    
    await execute_batch_insert(DimDate, dates)

async def seed_customers():
    """Load customers from CSV"""
    logger.info("Seeding DimCustomers...")
    df = pl.read_csv(DATA_DIR / "customers.csv")
    
    records = []
    for row in df.to_dicts():
        # Convert segment to lowercase to match database enum
        segment = row["segment"].lower()
        records.append({
            "customer_id": row["customer_id"],
            "customer_key": row["customer_id"], # Use ID as key
            "email_hash": hashlib.sha256(row["email"].encode()).hexdigest(),
            "first_name_masked": row["first_name"][0] + "***",
            "last_name_masked": row["last_name"][0] + "***",
            "country": row["country"],
            "segment": segment,
            "lifetime_value": row["lifetime_value"],
            "total_orders": row["total_orders"],
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        })
        
    await execute_batch_insert(DimCustomer, records)

async def seed_products():
    """Load products from CSV"""
    logger.info("Seeding DimProducts...")
    df = pl.read_csv(DATA_DIR / "products.csv")
    
    records = []
    for row in df.to_dicts():
        records.append({
            "product_id": row["product_id"],
            "product_key": row["sku"],
            "sku": row["sku"],
            "name": row["name"],
            "category": row["category"].lower(),
            "unit_price": row["unit_price"],
            "cost_price": row["cost_price"],
            "stock_quantity": row["stock_quantity"],
            "avg_rating": row["avg_rating"],
            "is_active": True,
            "is_in_stock": row["stock_quantity"] > 0,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        })
        
    await execute_batch_insert(DimProduct, records)

async def seed_orders():
    """Load orders from CSV"""
    logger.info("Seeding FactOrders...")
    df = pl.read_csv(DATA_DIR / "orders.csv")
    
    # Convert timestamp to datetime and extract date_key
    df = df.with_columns(
        pl.col("order_timestamp").str.strptime(pl.Datetime).alias("ts")
    )
    
    records = []
    for row in df.to_dicts():
        ts = row["ts"]
        date_key = int(ts.strftime("%Y%m%d"))
        
        # Ensure enums are lowercase just in case
        status = row["status"].lower()
        payment_method = row["payment_method"] # Assuming string, needs to match Enum if mapped?
        # FactOrder.payment_method is String(50), not Enum in updated model? Let's check.
        # FactOrder.status IS Enum OrderStatus.
        
        records.append({
            "order_id": row["order_id"],
            "order_number": row["order_number"],
            "customer_id": row["customer_id"],
            "order_date_key": date_key,
            "status": status,
            "order_timestamp": ts,
            "item_count": row["item_count"],
            "subtotal": row["subtotal"],
            "tax_amount": row["tax_amount"],
            "shipping_amount": row["shipping_amount"],
            "total_amount": row["total_amount"],
            "payment_method": payment_method,
            "device_type": row["device_type"],
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        })
        
    await execute_batch_insert(FactOrder, records)

async def seed_order_items():
    """Load order items from CSV"""
    logger.info("Seeding FactOrderItems...")
    df = pl.read_csv(DATA_DIR / "order_items.csv")
    
    records = []
    for row in df.to_dicts():
        line_total = row["quantity"] * row["unit_price"]
        records.append({
            "order_item_id": row["order_item_id"],
            "order_id": row["order_id"],
            "product_id": row["product_id"],
            "quantity": row["quantity"],
            "unit_price": row["unit_price"],
            "line_total": line_total,
            "created_at": datetime.utcnow()
        })
        
    await execute_batch_insert(FactOrderItem, records)

async def seed_clickstream():
    """Load clickstream from CSV"""
    logger.info("Seeding FactPageViews...")
    df = pl.read_csv(DATA_DIR / "clickstream.csv")
    
    df = df.with_columns(
        pl.col("event_timestamp").str.strptime(pl.Datetime).alias("ts")
    )
    
    records = []
    for row in df.to_dicts():
        ts = row["ts"]
        date_key = int(ts.strftime("%Y%m%d"))
        
        # Handle nullable customer_id
        cust_id = row["customer_id"]
        if cust_id == "None" or cust_id is None:
            cust_id = None
            
        records.append({
            "page_view_id": row["event_id"],
            "session_id": row["session_id"],
            "visitor_id": row["session_id"], # Fallback
            "customer_id": cust_id,
            "date_key": date_key,
            "event_timestamp": ts,
            "page_url": f"http://store.com/{row['page_type']}", 
            "page_path": f"/{row['page_type']}",
            "page_type": row["page_type"],
            "device_type": row["device_type"],
            "time_on_page_seconds": row["time_on_page"]
        })
        
    await execute_batch_insert(FactPageView, records)

async def main():
    logger.info("Starting database seeding...")
    await init_database()
    
    try:
        await seed_dim_date()
        await seed_customers()
        await seed_products()
        await seed_orders()
        await seed_order_items()
        await seed_clickstream()
        logger.info("Database seeding completed successfully!")
    except Exception as e:
        logger.error(f"Seeding failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
