"""
Synthetic Data Generator

Generates realistic e-commerce data for testing and development.
Includes:
- Customers with realistic demographics
- Products across categories
- Orders with realistic patterns
- Clickstream events
"""

import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List, Optional, Tuple
import json

import polars as pl
from faker import Faker
import numpy as np

from src.config import get_settings

fake = Faker()
settings = get_settings()

# Seed for reproducibility
random.seed(42)
np.random.seed(42)
Faker.seed(42)


# =============================================================================
# CONFIGURATION
# =============================================================================

CATEGORIES = [
    ("electronics", ["Phones", "Laptops", "Tablets", "Headphones", "Cameras"]),
    ("clothing", ["Shirts", "Pants", "Dresses", "Shoes", "Jackets"]),
    ("home_garden", ["Furniture", "Kitchen", "Bedding", "Garden", "Decor"]),
    ("sports", ["Fitness", "Outdoor", "Team Sports", "Water Sports", "Cycling"]),
    ("beauty", ["Skincare", "Makeup", "Haircare", "Fragrance", "Tools"]),
    ("books", ["Fiction", "Non-Fiction", "Educational", "Children", "Comics"]),
]

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
SHIPPING_METHODS = ["standard", "express", "overnight", "pickup"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]

ORDER_STATUSES = [
    ("pending", 0.05),
    ("confirmed", 0.05),
    ("processing", 0.05),
    ("shipped", 0.10),
    ("delivered", 0.70),
    ("cancelled", 0.03),
    ("refunded", 0.02),
]


# =============================================================================
# GENERATORS
# =============================================================================

class CustomerGenerator:
    """Generate realistic customer data"""
    
    def __init__(self):
        self.segments = {
            "new": 0.30,
            "returning": 0.40,
            "vip": 0.15,
            "at_risk": 0.10,
            "churned": 0.05,
        }
    
    def generate(self, n: int = 1000) -> pl.DataFrame:
        """Generate n customers"""
        customers = []
        
        for _ in range(n):
            customer_id = str(uuid.uuid4())
            segment = random.choices(
                list(self.segments.keys()),
                weights=list(self.segments.values()),
            )[0]
            
            # Generate based on segment
            if segment == "vip":
                ltv = random.uniform(5000, 50000)
                orders = random.randint(20, 100)
            elif segment == "returning":
                ltv = random.uniform(500, 5000)
                orders = random.randint(5, 20)
            elif segment == "at_risk":
                ltv = random.uniform(100, 1000)
                orders = random.randint(2, 10)
            else:
                ltv = random.uniform(0, 500)
                orders = random.randint(1, 5)
            
            customers.append({
                "customer_id": customer_id,
                "customer_key": f"CUST-{fake.unique.random_number(digits=8)}",
                "email": fake.email(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "phone": fake.phone_number(),
                "country": fake.country_code(),
                "state": fake.state_abbr() if random.random() > 0.3 else None,
                "city": fake.city(),
                "postal_code": fake.postcode(),
                "age_group": random.choice(["18-24", "25-34", "35-44", "45-54", "55+"]),
                "gender": random.choice(["M", "F", "Other", None]),
                "segment": segment,
                "lifetime_value": round(ltv, 2),
                "total_orders": orders,
                "avg_order_value": round(ltv / orders if orders > 0 else 0, 2),
                "registration_date": fake.date_time_between(
                    start_date="-3y", end_date="now"
                ),
                "is_current": True,
            })
        
        return pl.DataFrame(customers)


class ProductGenerator:
    """Generate realistic product catalog"""
    
    def __init__(self):
        self.brands = [
            "TechPro", "StyleMax", "HomeEase", "SportFit", "BeautyGlow",
            "BookWorld", "GenericCo", "PremiumPlus", "ValueChoice", "EcoFriendly"
        ]
    
    def generate(self, n: int = 500) -> pl.DataFrame:
        """Generate n products"""
        products = []
        
        for _ in range(n):
            category, subcategories = random.choice(CATEGORIES)
            subcategory = random.choice(subcategories)
            
            # Price based on category
            base_price = {
                "electronics": random.uniform(50, 2000),
                "clothing": random.uniform(20, 500),
                "home_garden": random.uniform(30, 1000),
                "sports": random.uniform(25, 800),
                "beauty": random.uniform(10, 200),
                "books": random.uniform(10, 50),
            }[category]
            
            unit_price = round(base_price, 2)
            cost_price = round(unit_price * random.uniform(0.3, 0.7), 2)
            
            products.append({
                "product_id": str(uuid.uuid4()),
                "product_key": f"PROD-{fake.unique.random_number(digits=8)}",
                "sku": f"SKU-{fake.unique.random_number(digits=10)}",
                "name": f"{fake.word().title()} {subcategory}",
                "description": fake.sentence(nb_words=15),
                "brand": random.choice(self.brands),
                "category": category,
                "subcategory": subcategory,
                "unit_price": unit_price,
                "cost_price": cost_price,
                "margin_percent": round((unit_price - cost_price) / unit_price * 100, 2),
                "stock_quantity": random.randint(0, 1000),
                "reorder_level": random.randint(10, 50),
                "is_in_stock": random.random() > 0.1,
                "avg_rating": round(random.uniform(3.0, 5.0), 1),
                "review_count": random.randint(0, 500),
                "is_active": random.random() > 0.05,
                "launch_date": fake.date_between(start_date="-2y", end_date="today"),
            })
        
        return pl.DataFrame(products)


class OrderGenerator:
    """Generate realistic order data"""
    
    def __init__(
        self,
        customers_df: pl.DataFrame,
        products_df: pl.DataFrame,
    ):
        self.customer_ids = customers_df["customer_id"].to_list()
        self.product_data = products_df.select([
            "product_id", "unit_price", "cost_price"
        ]).to_dicts()
    
    def generate(
        self,
        n: int = 10000,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """Generate n orders with items"""
        start_date = start_date or datetime.now() - timedelta(days=365)
        end_date = end_date or datetime.now()
        
        orders = []
        order_items = []
        
        for _ in range(n):
            order_id = str(uuid.uuid4())
            customer_id = random.choice(self.customer_ids)
            
            # Order timestamp with realistic patterns
            order_timestamp = fake.date_time_between(
                start_date=start_date,
                end_date=end_date,
            )
            
            # Number of items (most orders have 1-3 items)
            num_items = np.random.choice(
                [1, 2, 3, 4, 5, 6, 7, 8],
                p=[0.35, 0.30, 0.15, 0.10, 0.05, 0.03, 0.01, 0.01],
            )
            
            # Generate order items
            subtotal = 0
            total_cost = 0
            item_count = 0
            
            for _ in range(num_items):
                product = random.choice(self.product_data)
                quantity = np.random.choice(
                    [1, 2, 3, 4, 5],
                    p=[0.60, 0.25, 0.10, 0.03, 0.02],
                )
                
                unit_price = product["unit_price"]
                unit_cost = product["cost_price"] or unit_price * 0.5
                
                discount_percent = random.choice([0, 0, 0, 5, 10, 15, 20])
                discount_amount = round(unit_price * quantity * discount_percent / 100, 2)
                line_total = round(unit_price * quantity - discount_amount, 2)
                line_cost = round(unit_cost * quantity, 2)
                
                order_items.append({
                    "order_item_id": str(uuid.uuid4()),
                    "order_id": order_id,
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount_percent": discount_percent,
                    "discount_amount": discount_amount,
                    "line_total": line_total,
                    "unit_cost": unit_cost,
                    "line_cost": line_cost,
                    "line_margin": line_total - line_cost,
                })
                
                subtotal += line_total
                total_cost += line_cost
                item_count += quantity
            
            # Order totals
            tax_rate = random.uniform(0.05, 0.10)
            tax_amount = round(subtotal * tax_rate, 2)
            
            shipping_amount = 0 if subtotal > 100 else random.choice([5.99, 9.99, 14.99])
            
            total_discount = sum(
                item["discount_amount"] 
                for item in order_items 
                if item["order_id"] == order_id
            )
            
            total_amount = round(subtotal + tax_amount + shipping_amount, 2)
            
            # Status based on timestamp
            days_old = (datetime.now() - order_timestamp).days
            if days_old > 7:
                status = random.choices(
                    [s[0] for s in ORDER_STATUSES],
                    weights=[s[1] for s in ORDER_STATUSES],
                )[0]
            else:
                status = random.choice(["pending", "confirmed", "processing", "shipped"])
            
            orders.append({
                "order_id": order_id,
                "order_number": f"ORD-{fake.unique.random_number(digits=10)}",
                "customer_id": customer_id,
                "order_timestamp": order_timestamp,
                "order_date_key": int(order_timestamp.strftime("%Y%m%d")),
                "status": status,
                "payment_status": "captured" if status != "cancelled" else "refunded",
                "item_count": item_count,
                "subtotal": subtotal,
                "discount_amount": total_discount,
                "tax_amount": tax_amount,
                "shipping_amount": shipping_amount,
                "total_amount": total_amount,
                "total_cost": total_cost,
                "gross_margin": total_amount - total_cost,
                "payment_method": random.choice(PAYMENT_METHODS),
                "shipping_method": random.choice(SHIPPING_METHODS),
                "currency_code": "USD",
                "device_type": random.choice(DEVICE_TYPES),
                "is_first_order": random.random() < 0.2,
                "has_promo_code": random.random() < 0.3,
                "promo_code": f"PROMO{random.randint(100, 999)}" if random.random() < 0.3 else None,
            })
        
        return pl.DataFrame(orders), pl.DataFrame(order_items)


class ClickstreamGenerator:
    """Generate clickstream/page view events"""
    
    def __init__(
        self,
        customers_df: pl.DataFrame,
        products_df: pl.DataFrame,
    ):
        self.customer_ids = customers_df["customer_id"].to_list()
        self.product_ids = products_df["product_id"].to_list()
    
    def generate(
        self,
        n: int = 50000,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> pl.DataFrame:
        """Generate n page view events"""
        start_date = start_date or datetime.now() - timedelta(days=30)
        end_date = end_date or datetime.now()
        
        page_types = [
            ("home", "/", 0.20),
            ("category", "/category/{}", 0.25),
            ("product", "/product/{}", 0.30),
            ("cart", "/cart", 0.10),
            ("checkout", "/checkout", 0.08),
            ("account", "/account", 0.05),
            ("search", "/search?q={}", 0.02),
        ]
        
        events = []
        
        # Generate sessions
        n_sessions = n // 5  # Average 5 pages per session
        
        for _ in range(n_sessions):
            session_id = str(uuid.uuid4())
            visitor_id = str(uuid.uuid4())
            
            # 40% of sessions are logged in users
            customer_id = random.choice(self.customer_ids) if random.random() < 0.4 else None
            
            session_start = fake.date_time_between(
                start_date=start_date,
                end_date=end_date,
            )
            
            device_type = random.choice(DEVICE_TYPES)
            browser = random.choice(BROWSERS)
            
            # UTM parameters (30% have them)
            has_utm = random.random() < 0.3
            utm_source = random.choice(["google", "facebook", "email", "direct"]) if has_utm else None
            utm_medium = random.choice(["cpc", "organic", "social", "email"]) if has_utm else None
            
            # Generate pages for this session
            n_pages = np.random.choice([1, 2, 3, 4, 5, 6, 7, 8], p=[0.15, 0.20, 0.25, 0.15, 0.10, 0.08, 0.04, 0.03])
            
            current_time = session_start
            
            for page_idx in range(n_pages):
                page_type, path_template, _ = random.choices(
                    page_types,
                    weights=[p[2] for p in page_types],
                )[0]
                
                if "{}" in path_template:
                    if "category" in page_type:
                        path = path_template.format(random.choice(CATEGORIES)[0])
                    elif "product" in page_type:
                        product_id = random.choice(self.product_ids)
                        path = path_template.format(product_id[:8])
                    else:
                        path = path_template.format(fake.word())
                else:
                    path = path_template
                
                events.append({
                    "page_view_id": str(uuid.uuid4()),
                    "session_id": session_id,
                    "visitor_id": visitor_id,
                    "customer_id": customer_id,
                    "event_timestamp": current_time,
                    "date_key": int(current_time.strftime("%Y%m%d")),
                    "page_url": f"https://example.com{path}",
                    "page_path": path,
                    "page_type": page_type,
                    "referrer_url": "https://google.com" if page_idx == 0 and has_utm else None,
                    "utm_source": utm_source,
                    "utm_medium": utm_medium,
                    "device_type": device_type,
                    "browser": browser,
                    "product_id": random.choice(self.product_ids) if page_type == "product" else None,
                    "event_type": "page_view",
                    "time_on_page_seconds": random.randint(5, 300),
                })
                
                # Add time between pages
                current_time += timedelta(seconds=random.randint(10, 180))
        
        return pl.DataFrame(events)


# =============================================================================
# MAIN GENERATOR
# =============================================================================

class DataGenerator:
    """Main data generator orchestrator"""
    
    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = Path(output_dir or settings.data_lake.raw_path)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_all(
        self,
        n_customers: int = 1000,
        n_products: int = 500,
        n_orders: int = 10000,
        n_page_views: int = 50000,
        save: bool = True,
    ) -> dict:
        """Generate complete dataset"""
        print(f"Generating synthetic e-commerce data...")
        
        # Generate customers
        print(f"  Generating {n_customers} customers...")
        customers_df = CustomerGenerator().generate(n_customers)
        
        # Generate products
        print(f"  Generating {n_products} products...")
        products_df = ProductGenerator().generate(n_products)
        
        # Generate orders
        print(f"  Generating {n_orders} orders...")
        order_gen = OrderGenerator(customers_df, products_df)
        orders_df, order_items_df = order_gen.generate(n_orders)
        
        # Generate clickstream
        print(f"  Generating {n_page_views} page views...")
        clickstream_df = ClickstreamGenerator(customers_df, products_df).generate(n_page_views)
        
        data = {
            "customers": customers_df,
            "products": products_df,
            "orders": orders_df,
            "order_items": order_items_df,
            "page_views": clickstream_df,
        }
        
        if save:
            self._save_data(data)
        
        print(f"Data generation complete!")
        return data
    
    def _save_data(self, data: dict) -> None:
        """Save generated data to files"""
        for name, df in data.items():
            # Save as Parquet
            parquet_path = self.output_dir / f"{name}.parquet"
            df.write_parquet(parquet_path)
            print(f"  Saved {name}: {len(df)} rows -> {parquet_path}")
            
            # Also save as CSV for batch loading tests
            csv_path = self.output_dir / f"{name}.csv"
            df.write_csv(csv_path)


if __name__ == "__main__":
    generator = DataGenerator()
    generator.generate_all()
