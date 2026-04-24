import pandas as pd
import numpy as np
from collections import Counter


# =================================================
# HELPER: Compute sentiment score from ratings
# scale: -1.0 (all 1-star) to 1.0 (all 5-star)
# =================================================
def compute_sentiment(ratings_series):
    if ratings_series.empty:
        return None
    mean = ratings_series.mean()
    return round((mean - 3) / 2, 2)


# =================================================
# HELPER: Extract top complaint / top praise
# Negative = rating <= 2, Positive = rating >= 4
# =================================================
STOPWORDS = {
    "the", "a", "an", "is", "it", "in", "on", "at", "to", "and",
    "or", "but", "was", "this", "that", "my", "i", "for", "of",
    "with", "are", "be", "not", "very", "so", "its", "have", "has",
    "they", "we", "you", "he", "she", "their", "our", "your", "his",
    "her", "than", "more", "too", "also", "just", "really", "get",
    "got", "did", "do", "does", "no", "yes", "as", "by", "from",
    "would", "could", "should", "will", "can", "me", "us", "them"
}

def extract_top_theme(texts):
    if not texts:
        return None
    words = []
    for t in texts:
        if t:
            words.extend([
                w.lower().strip(".,!?\"'")
                for w in str(t).split()
                if len(w) > 3 and w.lower() not in STOPWORDS
            ])
    if not words:
        return None
    most_common = Counter(words).most_common(3)
    return ", ".join([w for w, _ in most_common])


# =================================================
# MAIN FEATURE ENGINEERING
# =================================================
def build_ai_summary(sales, reviews, cpi, ppi, inventory, products):

    # --- Normalise IDs ---
    sales["product_id"]     = sales["product_id"].astype(str).str.strip()
    inventory["product_id"] = inventory["product_id"].astype(str).str.strip()
    products["product_id"]  = products["product_id"].astype(str).str.strip()

    # --------------------------------------------------
    # 1. Sales aggregation from sales table
    # --------------------------------------------------
    sales_stats = (
        sales
        .groupby("product_id")
        .agg(total_sales=("quantity_sold", "sum"))
        .reset_index()
    )

    # --------------------------------------------------
    # 2. Review aggregation
    #    reviews → ctg_id → products → product_id
    # --------------------------------------------------
    if "ctg_id" in products.columns and "ctg_id" in reviews.columns:
        reviews  = reviews[reviews["ctg_id"].notna()].copy()
        products = products.copy()

        reviews["ctg_id"]  = pd.to_numeric(reviews["ctg_id"],  errors="coerce")
        products["ctg_id"] = pd.to_numeric(products["ctg_id"], errors="coerce")

        reviews  = reviews[reviews["ctg_id"].notna()]
        products_valid = products[products["ctg_id"].notna()].copy()

        reviews["ctg_id"]         = reviews["ctg_id"].astype(int)
        products_valid["ctg_id"]  = products_valid["ctg_id"].astype(int)

        reviews_with_product = reviews.merge(
            products_valid[["product_id", "ctg_id"]],
            on="ctg_id",
            how="left"
        )

        def agg_reviews(grp):
            texts     = grp["review_text"].dropna().tolist()
            ratings   = grp["rating"].dropna()
            neg_texts = grp.loc[grp["rating"] <= 2, "review_text"].dropna().tolist()
            pos_texts = grp.loc[grp["rating"] >= 4, "review_text"].dropna().tolist()

            return pd.Series({
                "avg_rating":      round(ratings.mean(), 2) if not ratings.empty else 0,
                "total_reviews":   len(grp),
                "review_summary":  " | ".join(texts[:20])[:2000] if texts else None,
                "sentiment_score": compute_sentiment(ratings),
                "top_complaint":   extract_top_theme(neg_texts),
                "top_praise":      extract_top_theme(pos_texts),
            })

        reviews_with_product = reviews_with_product[reviews_with_product["product_id"].notna()]

        if not reviews_with_product.empty:
            review_stats = (
                reviews_with_product
                .groupby("product_id")
                .apply(agg_reviews)
                .reset_index()
            )
        else:
            review_stats = pd.DataFrame(columns=[
                "product_id", "avg_rating", "total_reviews",
                "review_summary", "sentiment_score", "top_complaint", "top_praise"
            ])
    else:
        review_stats = pd.DataFrame(columns=[
            "product_id", "avg_rating", "total_reviews",
            "review_summary", "sentiment_score", "top_complaint", "top_praise"
        ])

    # Ensure all review columns exist before merge
    for col in ["avg_rating", "total_reviews", "review_summary",
                "sentiment_score", "top_complaint", "top_praise"]:
        if col not in review_stats.columns:
            review_stats[col] = None

    # --------------------------------------------------
    # 3. Merge — inventory is the base (all products)
    # --------------------------------------------------
    df = inventory.merge(
        products[["product_id", "user_id", "cost_price", "selling_price", "ctg_id"]],
        on="product_id", how="left"
    )
    df = df.merge(sales_stats,  on="product_id", how="left")
    df = df.merge(review_stats, on="product_id", how="left")

    # --------------------------------------------------
    # 4. Fill missing values
    # --------------------------------------------------
    df["total_sales"]   = df["total_sales"].fillna(0).astype(int)
    df["total_reviews"] = df["total_reviews"].fillna(0).astype(int)
    df["avg_rating"]    = df["avg_rating"].fillna(0)

    # --------------------------------------------------
    # 5. Derived financial features
    #    avg_selling_price = selling_price from products
    #    estimated_cost    = cost_price from products (per unit)
    #    total_revenue     = total_sales * avg_selling_price
    #    estimated_profit  = total_revenue - (total_sales * cost_price)
    # --------------------------------------------------
    df["avg_selling_price"] = df["selling_price"].fillna(0)
    df["estimated_cost"]    = df["cost_price"].fillna(0)
    df["total_revenue"]     = df["total_sales"] * df["avg_selling_price"]
    df["estimated_profit"]  = df["total_revenue"] - (df["total_sales"] * df["estimated_cost"])

    # --------------------------------------------------
    # 6. Macro indicators
    # --------------------------------------------------
    df["cpi_value"]    = cpi["cpi_value"].iloc[-1] if not cpi.empty else 1.83
    df["ppi_value"]    = ppi["ppi_value"].iloc[-1] if not ppi.empty else 119.60
    df["summary_date"] = pd.Timestamp.today().date()

    # --------------------------------------------------
    # 7. Stock features
    # --------------------------------------------------
    df["current_stock"] = df["stock_quantity"].fillna(0).astype(int)
    reorder = df["reorder_level"].fillna(10)

    df["stock_status"] = np.where(
        df["current_stock"] == 0, "Out of Stock",
        np.where(df["current_stock"] <= reorder * 0.5, "Critical",
        np.where(df["current_stock"] <= reorder, "Low", "Sufficient"))
    )

    df["stock_turnover_rate"] = np.where(
        df["current_stock"] > 0,
        (df["total_sales"] / df["current_stock"]).round(2),
        0.0
    )

    df["stock_risk_level"] = np.where(
        df["stock_turnover_rate"] >= 3, "HIGH", "LOW"
    )

    # --------------------------------------------------
    # 8. Drop intermediate columns only
    #    Keep: user_id, selling_price — needed for DB insert
    # --------------------------------------------------
    df.drop(
        columns=["stock_quantity", "reorder_level", "cost_price", "ctg_id"],
        inplace=True, errors="ignore"
    )

    return df