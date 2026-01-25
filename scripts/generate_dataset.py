"""
Fast E-Commerce Dataset Generator  
Generates 100,000 orders using vectorized operations (fast!)
"""

import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl
from faker import Faker

fake = Faker()
random.seed(42)
np.random.seed(42)
Faker.seed(42)

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "generated"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ==========================================
# CUSTOMERS (10,000)
# ==========================================
def generate_customers(n=10000):
    print(f"📊 Generating {n:,} customers...")
    
    segments = np.random.choice(
        ["new", "returning", "vip", "at_risk", "churned"],
        size=n, p=[0.30, 0.40, 0.15, 0.10, 0.05]
    )
    
    df = pl.DataFrame({
        "customer_id": [str(uuid.uuid4()) for _ in range(n)],
        "email": [fake.email() for _ in range(n)],
        "first_name": [fake.first_name() for _ in range(n)],
        "last_name": [fake.last_name() for _ in range(n)],
        "country": np.random.choice(["US", "UK", "CA", "DE", "FR", "AU"], n),
        "segment": segments,
        "lifetime_value": np.round(np.random.uniform(50, 5000, n), 2),
        "total_orders": np.random.randint(1, 50, n),
    })
    
    df.write_csv(OUTPUT_DIR / "customers.csv")
    print(f"   ✅ customers.csv: {n:,} rows")
    return df

# ==========================================
# PRODUCTS (5,000)
# ==========================================
def generate_products(n=5000):
    print(f"📊 Generating {n:,} products...")
    
    categories = ["electronics", "clothing", "home_garden", "sports", "beauty", "books"]
    
    df = pl.DataFrame({
        "product_id": [str(uuid.uuid4()) for _ in range(n)],
        "sku": [f"SKU-{i:08d}" for i in range(n)],
        "name": [f"{fake.word().title()} Product {i}" for i in range(n)],
        "category": np.random.choice(categories, n),
        "unit_price": np.round(np.random.uniform(10, 500, n), 2),
        "cost_price": np.round(np.random.uniform(5, 250, n), 2),
        "stock_quantity": np.random.randint(0, 1000, n),
        "avg_rating": np.round(np.random.uniform(3.0, 5.0, n), 1),
    })
    
    df.write_csv(OUTPUT_DIR / "products.csv")
    print(f"   ✅ products.csv: {n:,} rows")
    return df

# ==========================================
# ORDERS (100,000) - VECTORIZED!
# ==========================================
def generate_orders(n=100000, customer_ids=None, product_ids=None):
    print(f"📊 Generating {n:,} orders (vectorized)...")
    
    statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
    payments = ["credit_card", "debit_card", "paypal", "apple_pay"]
    
    # Generate timestamps
    base_date = datetime.now() - timedelta(days=365)
    random_days = np.random.randint(0, 365, n)
    random_hours = np.random.randint(0, 24, n)
    timestamps = [base_date + timedelta(days=int(d), hours=int(h)) for d, h in zip(random_days, random_hours)]
    
    df = pl.DataFrame({
        "order_id": [str(uuid.uuid4()) for _ in range(n)],
        "order_number": [f"ORD-{i:010d}" for i in range(n)],
        "customer_id": np.random.choice(customer_ids, n),
        "order_timestamp": timestamps,
        "status": np.random.choice(statuses, n, p=[0.05, 0.05, 0.10, 0.75, 0.05]),
        "item_count": np.random.randint(1, 8, n),
        "subtotal": np.round(np.random.uniform(20, 500, n), 2),
        "tax_amount": np.round(np.random.uniform(2, 50, n), 2),
        "shipping_amount": np.random.choice([0, 5.99, 9.99, 14.99], n),
        "total_amount": np.round(np.random.uniform(25, 600, n), 2),
        "payment_method": np.random.choice(payments, n),
        "device_type": np.random.choice(["desktop", "mobile", "tablet"], n),
    })
    
    df.write_csv(OUTPUT_DIR / "orders.csv")
    print(f"   ✅ orders.csv: {n:,} rows")
    return df

# ==========================================
# ORDER ITEMS (~250,000)
# ==========================================
def generate_order_items(orders_df, product_ids):
    print(f"📊 Generating order items...")
    
    order_ids = orders_df["order_id"].to_list()
    item_counts = orders_df["item_count"].to_list()
    
    items = []
    for order_id, item_count in zip(order_ids, item_counts):
        for _ in range(item_count):
            items.append({
                "order_item_id": str(uuid.uuid4()),
                "order_id": order_id,
                "product_id": random.choice(product_ids),
                "quantity": random.randint(1, 3),
                "unit_price": round(random.uniform(10, 200), 2),
            })
    
    df = pl.DataFrame(items)
    df.write_csv(OUTPUT_DIR / "order_items.csv")
    print(f"   ✅ order_items.csv: {len(items):,} rows")
    return df

# ==========================================
# CLICKSTREAM (50,000)
# ==========================================
def generate_clickstream(n=50000, customer_ids=None, product_ids=None):
    print(f"📊 Generating {n:,} clickstream events...")
    
    page_types = ["home", "category", "product", "cart", "checkout", "account"]
    
    base_date = datetime.now() - timedelta(days=30)
    random_days = np.random.randint(0, 30, n)
    random_hours = np.random.randint(0, 24, n)
    timestamps = [base_date + timedelta(days=int(d), hours=int(h)) for d, h in zip(random_days, random_hours)]
    
    # Use only valid customer IDs (no None)
    df = pl.DataFrame({
        "event_id": [str(uuid.uuid4()) for _ in range(n)],
        "session_id": [str(uuid.uuid4()) for _ in range(n)],
        "customer_id": np.random.choice(customer_ids, n),
        "event_timestamp": timestamps,
        "page_type": np.random.choice(page_types, n, p=[0.20, 0.25, 0.30, 0.10, 0.08, 0.07]),
        "device_type": np.random.choice(["desktop", "mobile", "tablet"], n),
        "time_on_page": np.random.randint(5, 300, n),
    })
    
    df.write_csv(OUTPUT_DIR / "clickstream.csv")
    print(f"   ✅ clickstream.csv: {n:,} rows")
    return df

# ==========================================
# MAIN
# ==========================================
def main():
    print("=" * 60)
    print("🛒 Fast E-Commerce Dataset Generator")
    print("=" * 60 + "\n")
    
    # Generate all datasets
    customers_df = generate_customers(10000)
    products_df = generate_products(5000)
    
    customer_ids = customers_df["customer_id"].to_list()
    product_ids = products_df["product_id"].to_list()
    
    orders_df = generate_orders(100000, customer_ids, product_ids)
    generate_order_items(orders_df, product_ids)
    generate_clickstream(50000, customer_ids, product_ids)
    
    # Summary
    print("\n" + "=" * 60)
    print("✅ Dataset Generation Complete!")
    print("=" * 60)
    print(f"\n📁 Output: {OUTPUT_DIR}\n")
    
    total = 0
    for f in OUTPUT_DIR.glob("*.csv"):
        size = f.stat().st_size / 1024 / 1024
        with open(f, 'r') as file:
            rows = sum(1 for _ in file) - 1
        total += rows
        print(f"   📄 {f.name}: {rows:,} rows ({size:.2f} MB)")
    
    print(f"\n📊 Total: {total:,} rows")


if __name__ == "__main__":
    main()
