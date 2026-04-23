import pandas as pd
import numpy as np

def build_ai_summary(sales_df, reviews, cpi, ppi, inventory, products=None):
    """
    Build AI summary using sales table data from database
    
    Args:
        sales_df: Sales data (with product_id, quantity_sold, total_price, cost_price)
        reviews: Customer reviews
        cpi: Consumer Price Index data
        ppi: Producer Price Index data
        inventory: Stock inventory data
        products: Products data (required, for selling_price)
    """

    # -------------------------
    # FORCE MATCHING product_id TYPE
    # -------------------------
    sales_df["product_id"] = sales_df["product_id"].astype(str).str.strip()
    inventory["product_id"] = inventory["product_id"].astype(str).str.strip()
    reviews["product_id"] = reviews["product_id"].astype(str).str.strip()
    
    if products is not None:
        products["product_id"] = products["product_id"].astype(str).str.strip()
    else:
        raise ValueError("Products dataframe is required for selling_price")

    # -------------------------
    # AGGREGATION FROM SALES TABLE
    # -------------------------
    # Group by product_id and sum quantity_sold and cost_price
    product_stats = sales_df.groupby("product_id").agg({
        "quantity_sold": "sum",
        "cost_price": "sum"
    }).reset_index()

    product_stats.rename(columns={
        "quantity_sold": "total_sales",
        "cost_price": "total_cost"
    }, inplace=True)

    # -------------------------
    # REVIEWS
    # -------------------------
    review_stats = reviews.groupby("product_id").agg({
        "rating": "mean"
    }).reset_index().rename(columns={"rating": "avg_rating"})

    # -------------------------
    # MERGE WITH PRODUCTS FOR SELLING PRICE
    # -------------------------
    df = product_stats.merge(
        products[["product_id", "selling_price"]], 
        on="product_id", 
        how="left"
    )
    df.rename(columns={"selling_price": "avg_selling_price"}, inplace=True)
    
    # -------------------------
    # MERGE WITH REVIEWS
    # -------------------------
    df = df.merge(review_stats, on="product_id", how="left")
    
    # -------------------------
    # MERGE WITH INVENTORY
    # -------------------------
    df = df.merge(inventory[["product_id", "stock_quantity", "reorder_level"]], on="product_id", how="inner")

    # -------------------------
    # DEBUG: check if data merged correctly
    # -------------------------
    null_selling = df["avg_selling_price"].isna().sum()
    if null_selling > 0:
        print(f"WARNING: {null_selling} products missing selling_price")
    
    null_stock = df["stock_quantity"].isna().sum()
    print(f"Products with NULL stock after merge: {null_stock} / {len(df)}")

    # -------------------------
    # BASE FEATURES
    # -------------------------
    # Calculate total_revenue = total_sales * avg_selling_price
    df["total_revenue"] = (df["total_sales"] * df["avg_selling_price"]).round(2)
    
    # Use actual cost from sales table
    df["estimated_cost"] = df["total_cost"]
    df["estimated_profit"] = (df["total_revenue"] - df["estimated_cost"]).round(2)
    
    df["conversion_rate"] = 0
    df["total_views"] = 0
    df["total_cart"] = 0

    df["cpi_value"] = cpi["cpi_value"].iloc[-1]
    df["ppi_value"] = ppi["ppi_value"].iloc[-1]
    df["summary_date"] = pd.Timestamp.today().date()

    # -------------------------
    # STOCK FEATURES
    # -------------------------
    df["current_stock"] = df["stock_quantity"].fillna(0).astype(int)
    reorder = df["reorder_level"].fillna(10)

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

    # Drop temporary columns
    df.drop(columns=["stock_quantity", "reorder_level", "total_cost"], inplace=True)

    print(f"Successfully built AI summary for {len(df)} products from sales data")
    return df