import pandas as pd
import numpy as np

def build_ai_summary(order_items, reviews, cpi, ppi, inventory):

    # -------------------------
    # FORCE MATCHING product_id TYPE
    # -------------------------
    order_items["product_id"] = order_items["product_id"].astype(str).str.strip()
    inventory["product_id"]   = inventory["product_id"].astype(str).str.strip()
    reviews["product_id"]     = reviews["product_id"].astype(str).str.strip()

    # -------------------------
    # AGGREGATION
    # -------------------------
    product_stats = order_items.groupby("product_id").agg({
        "quantity":   "sum",
        "item_price": "sum"
    }).reset_index()

    product_stats.rename(columns={
        "quantity":   "total_sales",
        "item_price": "total_revenue"
    }, inplace=True)

    # -------------------------
    # REVIEWS
    # -------------------------
    review_stats = reviews.groupby("product_id").agg({
        "rating": "mean"
    }).reset_index().rename(columns={"rating": "avg_rating"})

    # -------------------------
    # MERGE
    # -------------------------
    df = product_stats.merge(review_stats, on="product_id", how="left")
    df = df.merge(inventory[["product_id", "stock_quantity", "reorder_level"]], on="product_id", how="inner")

    # -------------------------
    # DEBUG: check if stock merged correctly
    # -------------------------
    null_stock = df["stock_quantity"].isna().sum()
    print(f"⚠️  Products with NULL stock after merge: {null_stock} / {len(df)}")
    if null_stock > 0:
        print("Sample unmatched product_ids:")
        print(df[df["stock_quantity"].isna()]["product_id"].head(10).tolist())
        print("Inventory product_ids sample:")
        print(inventory["product_id"].head(10).tolist())

    # -------------------------
    # BASE FEATURES
    # -------------------------
    df["avg_selling_price"] = df["total_revenue"] / df["total_sales"]
    df["estimated_cost"]    = df["total_revenue"] * 0.6
    df["estimated_profit"]  = df["total_revenue"] - df["estimated_cost"]
    df["conversion_rate"]   = 0
    df["total_views"]       = 0
    df["total_cart"]        = 0

    df["cpi_value"]    = cpi["cpi_value"].iloc[-1]
    df["ppi_value"]    = ppi["ppi_value"].iloc[-1]
    df["summary_date"] = pd.Timestamp.today().date()

    # -------------------------
    # STOCK FEATURES
    # -------------------------
    df["current_stock"] = df["stock_quantity"].fillna(0).astype(int)
    reorder             = df["reorder_level"].fillna(10)

    df["stock_status"] = np.where(
        df["current_stock"] == 0, "Out of Stock",
        np.where(
            df["current_stock"] <= reorder * 0.5, "Critical",
            np.where(
                df["current_stock"] <= reorder, "Low",
                "Sufficient"
            )
        )
    )

    df["stock_turnover_rate"] = np.where(
        df["current_stock"] > 0,
        (df["total_sales"] / df["current_stock"]).round(2),
        0.0
    )

    df["stock_risk_level"] = np.where(
        df["stock_turnover_rate"] >= 3, "High Risk",
        np.where(
            df["stock_turnover_rate"] >= 1, "Medium Risk",
            "Low Risk"
        )
    )

    df.drop(columns=["stock_quantity", "reorder_level"], inplace=True)

    return df