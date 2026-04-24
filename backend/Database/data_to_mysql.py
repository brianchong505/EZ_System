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
# LOAD CLEANED DATA
# -----------------------------
(
    users,
    products,
    inventory,
    product_cost_history,
    orders,
    order_items,
    reviews,
    cpi,
    ppi,
) = load_and_clean()

print("✅ All cleaned data loaded")


# -----------------------------
# INSERT FUNCTION
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
# STEP 1: LOAD CATEGORIES FROM CSV
# -----------------------------
from backend.Database.data_cleaning import load_csv
categories_csv = load_csv("categories.csv")
insert_table(categories_csv, "categories")


# -----------------------------
# STEP 2: CREATE CATEGORY → ctg_id MAP
# -----------------------------
cat_df = pd.read_sql("SELECT ctg_id, category FROM categories", engine)
category_map = dict(zip(cat_df["category"], cat_df["ctg_id"]))


# -----------------------------
# STEP 3: CONVERT reviews.category → reviews.ctg_id
# -----------------------------
if reviews is not None and not reviews.empty:

    # your raw data likely still has "category"
    if "category" in reviews.columns:

        reviews["ctg_id"] = reviews["category"].map(category_map)

        # check missing mappings
        missing = reviews[reviews["ctg_id"].isna()]["category"].unique()
        if len(missing) > 0:
            print("⚠️ Unmapped categories:", missing)

        # remove old column
        reviews = reviews.drop(columns=["category"])

        # ensure correct types
        reviews["ctg_id"] = reviews["ctg_id"].astype("Int64")


# -----------------------------
# STEP 4: INSERT CATEGORIES DATA INTO REVIEW TABLE
# -----------------------------
if reviews is not None and not reviews.empty:
    # Create review records from categories data
    category_reviews = reviews.copy()
    
    # Add required review columns if they don't exist
    if "ctg_id" not in category_reviews.columns and "category_id" in category_reviews.columns:
        category_reviews.rename(columns={"category_id": "ctg_id"}, inplace=True)
    
    # Add a rating based on category (you can customize this logic)
    if "rating" not in category_reviews.columns:
        category_reviews["rating"] = 5  # default rating
    
    # Add review text
    if "review_text" not in category_reviews.columns:
        category_reviews["review_text"] = "Category: " + category_reviews.get("category", "Unknown")
    
    # Keep only necessary columns for review table
    review_columns_from_cat = []
    if "ctg_id" in category_reviews.columns:
        review_columns_from_cat.append("ctg_id")
    if "rating" in category_reviews.columns:
        review_columns_from_cat.append("rating")
    if "review_text" in category_reviews.columns:
        review_columns_from_cat.append("review_text")
    
    if review_columns_from_cat:
        category_reviews = category_reviews[review_columns_from_cat]
        insert_table(category_reviews, "review")
        print("✅ Categories data inserted into review table")


# -----------------------------
# INSERT ORDER (IMPORTANT FOR FOREIGN KEYS)
# -----------------------------
insert_table(users, "users")
insert_table(products, "products")
insert_table(product_cost_history, "product_cost_history")
insert_table(inventory, "inventory")
insert_table(orders, "orders")
insert_table(order_items, "order_items")
insert_table(reviews, "reviews")
insert_table(cpi, "cpi_data")
insert_table(ppi, "ppi_data")

print("🎉 ALL DATA INSERTION COMPLETED SUCCESSFULLY")