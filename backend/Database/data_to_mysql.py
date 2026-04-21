import pandas as pd
from sqlalchemy import create_engine
from backend.Database.data_cleaning import load_and_clean, load_csv

# -----------------------------
# DB CONNECTION
# -----------------------------
engine = create_engine(
    "mysql+pymysql://root:1234@localhost/ez_system"
)

print("🚀 Starting data insertion pipeline...")


# -----------------------------
# LOAD CLEANED DATA (NOW 9 RETURNS)
# -----------------------------
(
    users,
    products,
    inventory,   # ⭐ NEW FIX HERE
    product_cost_history,
    orders,
    order_items,
    reviews,
    cpi,
    ppi
) = load_and_clean()

print("✅ All cleaned data loaded")


# -----------------------------
# LOAD RAW INVENTORY CSV (SAFE)
# -----------------------------
inventory = load_csv("inventory.csv")

# -----------------------------
# CLEAN INVENTORY
# -----------------------------
inventory.columns = (
    inventory.columns.str.lower()
    .str.strip()
    .str.replace(" ", "_")
)

if "product_id" in inventory:
    inventory["product_id"] = inventory["product_id"].astype(str).str.strip().str.upper()


# -----------------------------
# SAFE INSERT FUNCTION
# -----------------------------
def insert_table(df, table_name):
    if df is None or df.empty:
        print(f"⚠️ Skipping {table_name} (empty)")
        return

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        print(f"✅ {table_name} inserted ({len(df)} rows)")

    except Exception as e:
        print(f"❌ Error inserting {table_name}: {e}")


# -----------------------------
# INSERT ORDER (CRITICAL)
# -----------------------------

insert_table(users, "users")
insert_table(products, "products")
insert_table(product_cost_history, "product_cost_history")

# ⭐ INVENTORY
insert_table(inventory, "inventory")

insert_table(orders, "orders")
insert_table(order_items, "order_items")
insert_table(reviews, "reviews")

insert_table(cpi, "cpi_data")
insert_table(ppi, "ppi_data")

print("🎉 ALL DATA INSERTION COMPLETED SUCCESSFULLY")