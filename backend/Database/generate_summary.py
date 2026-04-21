from sqlalchemy import text
from db import get_engine
from load_data import load_data
from feature_engineering import build_ai_summary
import numpy as np

print("🚀 Starting AI Product Summary Pipeline...")

engine = get_engine()

# -------------------------
# LOAD DATA
# -------------------------
orders, order_items, reviews, cpi, ppi, inventory = load_data()
print(f"✅ Data loaded | Orders: {len(orders)} | Products in inventory: {len(inventory)}")

# -------------------------
# BUILD FEATURES
# -------------------------
df = build_ai_summary(order_items, reviews, cpi, ppi, inventory)
print(f"✅ Features built | Rows to process: {len(df)}")

if df.empty:
    print("❌ No data returned from feature engineering. Exiting.")
    exit()

# -------------------------
# CLEAN DATA
# -------------------------
df = df.drop_duplicates(subset=["product_id"])
df["avg_rating"] = df["avg_rating"].fillna(0)  # before replace so fillna catches NaN not None
df = df.replace({np.nan: None})

# -------------------------
# SAFETY: VALID PRODUCT FILTER
# -------------------------
with engine.connect() as conn:
    result = conn.execute(text("SELECT product_id FROM products"))
    valid_product_ids = set(row[0] for row in result)

before = len(df)
df = df[df["product_id"].isin(valid_product_ids)]
skipped = before - len(df)

if skipped > 0:
    print(f"⚠️  Skipped {skipped} products not found in products table")

if df.empty:
    print("❌ No valid products to insert. Exiting.")
    exit()

print(f"✅ Inserting {len(df)} products into ai_product_summary...")

# -------------------------
# CONVERT TO DICT
# -------------------------
data = df.to_dict(orient="records")

# -------------------------
# SQL
# -------------------------
sql = text("""
INSERT INTO ai_product_summary (
    product_id,
    total_sales,
    total_revenue,
    avg_selling_price,
    estimated_cost,
    estimated_profit,
    total_views,
    total_cart,
    conversion_rate,
    avg_rating,
    cpi_value,
    ppi_value,
    summary_date,
    current_stock,
    stock_status,
    stock_turnover_rate,
    stock_risk_level
)
VALUES (
    :product_id,
    :total_sales,
    :total_revenue,
    :avg_selling_price,
    :estimated_cost,
    :estimated_profit,
    :total_views,
    :total_cart,
    :conversion_rate,
    :avg_rating,
    :cpi_value,
    :ppi_value,
    :summary_date,
    :current_stock,
    :stock_status,
    :stock_turnover_rate,
    :stock_risk_level
)
ON DUPLICATE KEY UPDATE
    total_sales         = VALUES(total_sales),
    total_revenue       = VALUES(total_revenue),
    avg_selling_price   = VALUES(avg_selling_price),
    estimated_cost      = VALUES(estimated_cost),
    estimated_profit    = VALUES(estimated_profit),
    total_views         = VALUES(total_views),
    total_cart          = VALUES(total_cart),
    conversion_rate     = VALUES(conversion_rate),
    avg_rating          = VALUES(avg_rating),
    cpi_value           = VALUES(cpi_value),
    ppi_value           = VALUES(ppi_value),
    summary_date        = VALUES(summary_date),
    current_stock       = VALUES(current_stock),
    stock_status        = VALUES(stock_status),
    stock_turnover_rate = VALUES(stock_turnover_rate),
    stock_risk_level    = VALUES(stock_risk_level)
""")

# DEBUG - check stock columns before insert
print("Sample data being inserted:")
for row in data[:3]:
    print({
        "product_id":         row.get("product_id"),
        "current_stock":      row.get("current_stock"),
        "stock_status":       row.get("stock_status"),
        "stock_turnover_rate":row.get("stock_turnover_rate"),
        "stock_risk_level":   row.get("stock_risk_level"),
    })

# -------------------------
# INSERT
# -------------------------
try:
    with engine.begin() as conn:
        conn.execute(sql, data)
    print(f"🎉 AI Summary Pipeline Completed — {len(df)} products inserted/updated successfully")
except Exception as e:
    print(f"❌ Insert failed: {e}")
    raise