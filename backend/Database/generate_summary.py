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
sales, reviews, cpi, ppi, inventory, categories, products = load_data()

print(
    f"✅ Data loaded | "
    f"Sales: {len(sales)} | "
    f"Products: {len(inventory)} | "
    f"Reviews: {len(reviews)}"
)

# -------------------------
# BUILD FEATURES
# -------------------------
df = build_ai_summary(sales, reviews, cpi, ppi, inventory, products)

print(f"✅ Features built | Rows: {len(df)}")

if df.empty:
    print("❌ No data returned from feature engineering. Exiting.")
    exit()

# -------------------------
# SAFE SCHEMA ENFORCEMENT
# -------------------------
EXPECTED_COLS = [
    "user_id",
    "selling_price",
    "avg_rating",
    "total_reviews",
    "review_summary",
    "sentiment_score",
    "top_complaint",
    "top_praise",
    "total_sales",
    "total_revenue",
    "avg_selling_price",
    "estimated_cost",
    "estimated_profit",
    "current_stock",
    "stock_status",
    "stock_turnover_rate",
    "stock_risk_level",
    "cpi_value",
    "ppi_value",
    "summary_date",
]

TEXT_COLS = {"review_summary", "stock_status", "stock_risk_level",
             "top_complaint", "top_praise", "user_id"}

for col in EXPECTED_COLS:
    if col not in df.columns:
        df[col] = None if col in TEXT_COLS else 0
    else:
        df[col] = df[col].where(df[col].notna(), None if col in TEXT_COLS else 0)

# -------------------------
# CLEAN DATA
# -------------------------
df = df.drop_duplicates(subset=["product_id"])
df = df.replace({np.nan: None})

# -------------------------
# VALID PRODUCT FILTER
# -------------------------
with engine.connect() as conn:
    result = conn.execute(text("SELECT product_id FROM products"))
    valid_product_ids = {row[0] for row in result}

before = len(df)
df = df[df["product_id"].isin(valid_product_ids)]
skipped = before - len(df)

if skipped > 0:
    print(f"⚠️ Skipped {skipped} invalid products")

if df.empty:
    print("❌ No valid products to insert. Exiting.")
    exit()

print(f"✅ Inserting {len(df)} products into ai_product_summary...")

# -------------------------
# CONVERT TO DICT
# -------------------------
data = df.to_dict(orient="records")

# -------------------------
# SQL QUERY
# -------------------------
sql = text("""
    INSERT INTO ai_product_summary (
        product_id,
        user_id,
        selling_price,
        total_sales,
        total_revenue,
        avg_selling_price,
        estimated_cost,
        estimated_profit,
        avg_rating,
        cpi_value,
        ppi_value,
        summary_date,
        current_stock,
        stock_status,
        stock_turnover_rate,
        stock_risk_level,
        total_reviews,
        review_summary,
        sentiment_score,
        top_complaint,
        top_praise
    )
    VALUES (
        :product_id,
        :user_id,
        :selling_price,
        :total_sales,
        :total_revenue,
        :avg_selling_price,
        :estimated_cost,
        :estimated_profit,
        :avg_rating,
        :cpi_value,
        :ppi_value,
        :summary_date,
        :current_stock,
        :stock_status,
        :stock_turnover_rate,
        :stock_risk_level,
        :total_reviews,
        :review_summary,
        :sentiment_score,
        :top_complaint,
        :top_praise
    )
    ON DUPLICATE KEY UPDATE
        user_id             = VALUES(user_id),
        selling_price       = VALUES(selling_price),
        total_sales         = VALUES(total_sales),
        total_revenue       = VALUES(total_revenue),
        avg_selling_price   = VALUES(avg_selling_price),
        estimated_cost      = VALUES(estimated_cost),
        estimated_profit    = VALUES(estimated_profit),
        avg_rating          = VALUES(avg_rating),
        cpi_value           = VALUES(cpi_value),
        ppi_value           = VALUES(ppi_value),
        summary_date        = VALUES(summary_date),
        current_stock       = VALUES(current_stock),
        stock_status        = VALUES(stock_status),
        stock_turnover_rate = VALUES(stock_turnover_rate),
        stock_risk_level    = VALUES(stock_risk_level),
        total_reviews       = VALUES(total_reviews),
        review_summary      = VALUES(review_summary),
        sentiment_score     = VALUES(sentiment_score),
        top_complaint       = VALUES(top_complaint),
        top_praise          = VALUES(top_praise)
""")

# -------------------------
# DEBUG SAMPLE
# -------------------------
print("\n📊 Sample output check:")
for row in data[:3]:
    print({
        "product_id":      row.get("product_id"),
        "user_id":         row.get("user_id"),
        "selling_price":   row.get("selling_price"),
        "avg_rating":      row.get("avg_rating"),
        "total_reviews":   row.get("total_reviews"),
        "sentiment_score": row.get("sentiment_score"),
        "stock_status":    row.get("stock_status"),
        "top_complaint":   row.get("top_complaint"),
        "top_praise":      row.get("top_praise"),
    })

# -------------------------
# INSERT
# -------------------------
try:
    with engine.begin() as conn:
        conn.execute(sql, data)
    print(f"🎉 Pipeline Completed — {len(df)} products processed successfully")
except Exception as e:
    print(f"❌ Insert failed: {e}")
    raise