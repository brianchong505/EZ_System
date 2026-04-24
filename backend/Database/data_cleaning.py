import pandas as pd
import os

# -----------------------------
# BASE DIRECTORY
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# -----------------------------
# CLEAN COLUMN NAMES
# -----------------------------
def clean_columns(df):
    df.columns = (
        df.columns.str.lower()
        .str.strip()
        .str.replace(" ", "_")
    )
    return df


# -----------------------------
# CLEAN ID (SAFE FOR FK)
# -----------------------------
def clean_id(value):
    if pd.isna(value):
        return None
    return str(value).strip().upper()


# -----------------------------
# FIX YEAR
# -----------------------------
def fix_year(df):
    if "year" in df.columns:
        df["year"] = pd.to_datetime(df["year"], errors="coerce").dt.year
    return df


# -----------------------------
# FIX DATE
# -----------------------------
def fix_date(df, col):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df


# -----------------------------
# SAFE CSV LOADER
# -----------------------------
def load_csv(file_name):
    path = os.path.join(BASE_DIR, "data", file_name)

    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Missing file: {path}")

    return pd.read_csv(path)


# -----------------------------
# LOAD + CLEAN DATASET
# -----------------------------
def load_and_clean():

    # -----------------------------
    # LOAD CSV FILES
    # -----------------------------
    users = load_csv("users.csv")
    products = load_csv("products.csv")
    inventory = load_csv("inventory.csv")   # ⭐ NEW
    product_cost_history = load_csv("product_cost_history.csv")

    orders = load_csv("orders.csv")
    order_items = load_csv("order_items.csv")
    #reviews = load_csv("reviews.csv")

    cpi = load_csv("CPI.csv")
    ppi = load_csv("PPI.csv")
    reviews = load_csv("categories.csv")   # ⭐ NEW

    # -----------------------------
    # CLEAN COLUMN NAMES
    # -----------------------------
    dataframes = [
        users, products, inventory,
        product_cost_history,
        orders, order_items, 
        cpi, ppi,reviews,
    ]

    dataframes = [clean_columns(df) for df in dataframes]

    (
        users, products, inventory,
        product_cost_history,
        orders, order_items,
        cpi, ppi, reviews
    ) = dataframes

    # -----------------------------
    # CLEAN IDs
    # -----------------------------
    if "user_id" in users.columns:
        users["user_id"] = users["user_id"].apply(clean_id)

    if "product_id" in products.columns:
        products["product_id"] = products["product_id"].apply(clean_id)
        if "user_id" in products.columns:
            products["user_id"] = products["user_id"].apply(clean_id)

    if "product_id" in inventory.columns:
        inventory["product_id"] = inventory["product_id"].apply(clean_id)

    if "product_id" in product_cost_history.columns:
        product_cost_history["product_id"] = product_cost_history["product_id"].apply(clean_id)

    if "order_id" in orders.columns:
        orders["order_id"] = orders["order_id"].apply(clean_id)
        if "user_id" in orders.columns:
            orders["user_id"] = orders["user_id"].apply(clean_id)

    if "order_item_id" in order_items.columns:
        order_items["order_item_id"] = order_items["order_item_id"].apply(clean_id)
        order_items["order_id"] = order_items["order_id"].apply(clean_id)
        order_items["product_id"] = order_items["product_id"].apply(clean_id)

    #if "review_id" in reviews.columns:
        #reviews["review_id"] = reviews["review_id"].apply(clean_id)
        #reviews["product_id"] = reviews["product_id"].apply(clean_id)
        #if "user_id" in reviews.columns:
            #reviews["user_id"] = reviews["user_id"].apply(clean_id)

    if "category_id" in reviews.columns:
        reviews["category_id"] = reviews["category_id"].apply(clean_id)
    # -----------------------------
    # FIX DATES
    # -----------------------------
    product_cost_history = fix_date(product_cost_history, "recorded_date")

    # -----------------------------
    # FIX CPI / PPI
    # -----------------------------
    cpi = fix_year(cpi)
    ppi = fix_year(ppi)

    # -----------------------------
    # CLEAN DATA (SAFE BUT NOT OVER-DESTRUCTIVE)
    # -----------------------------
    users = users.drop_duplicates()
    products = products.drop_duplicates()
    inventory = inventory.drop_duplicates()
    product_cost_history = product_cost_history.drop_duplicates()

    orders = orders.drop_duplicates()
    order_items = order_items.drop_duplicates()
    #reviews = reviews.drop_duplicates()
    cpi = cpi.drop_duplicates()
    ppi = ppi.drop_duplicates()
    reviews = reviews.drop_duplicates()

    return (
        users,
        products,
        inventory,
        product_cost_history,
        orders,
        order_items,
        reviews,
        cpi,
        ppi
    )


# -----------------------------
# TEST RUN
# -----------------------------
if __name__ == "__main__":
    data = load_and_clean()

    print("Users:", len(data[0]))
    print("Products:", len(data[1]))
    print("Inventory:", len(data[2]))   # ⭐ NEW
    print("Product cost history:", len(data[3]))
    print("Orders:", len(data[4]))
    print("Order items:", len(data[5]))
    #print("Reviews:", len(data[6]))
    print("CPI:", len(data[7]))
    print("PPI:", len(data[8]))
    print("Reviews:", len(data[10]))   # ⭐ NEW   