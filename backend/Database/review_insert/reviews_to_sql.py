import pandas as pd
from sqlalchemy import create_engine

# -----------------------------
# DB CONNECTION
# -----------------------------
engine = create_engine("mysql+pymysql://root:1234@localhost/ez_system")

# -----------------------------
# CATEGORY → ctg_id MAP
# -----------------------------
category_map = {
    "Electronics": 1,
    "Home & Kitchen": 2,
    "Fashion": 3,
    "Beauty": 4,
    "Toys & Games": 5,
    "Books": 6,
    "Health & Personal Care": 7,
    "Sports & Outdoors": 8
}

# -----------------------------
# LOAD CSV FILE
# -----------------------------
df = pd.read_csv(r"D:\Flutter\umh\backend\review_insert\reviews.csv")
# -----------------------------
# CLEAN COLUMN NAMES (SAFE)
# -----------------------------
df.columns = df.columns.str.lower().str.strip()

# -----------------------------
# MAP CATEGORY → ctg_id
# -----------------------------
df["ctg_id"] = df["category"].map(category_map)

# -----------------------------
# DROP UNMAPPED ROWS (IMPORTANT)
# -----------------------------
missing = df[df["ctg_id"].isna()]["category"].unique()
if len(missing) > 0:
    print("⚠️ Unmapped categories found:", missing)

df = df.dropna(subset=["ctg_id"])

# -----------------------------
# REMOVE CATEGORY COLUMN
# -----------------------------
df = df.drop(columns=["category"])

# -----------------------------
# TYPE SAFETY
# -----------------------------
df["ctg_id"] = df["ctg_id"].astype(int)
df["rating"] = df["rating"].astype(int)

# -----------------------------
# INSERT INTO MYSQL
# -----------------------------
df.to_sql(
    name="reviews",
    con=engine,
    if_exists="append",
    index=False,
    method="multi",
    chunksize=1000
)

print(f"✅ Successfully inserted {len(df)} reviews from CSV")