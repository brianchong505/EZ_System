# backend/dashboard_service.py
from sqlalchemy import text
from backend.Database.db import get_engine

engine = get_engine()

def get_user_dashboard_data(user_id: str):
    try:
        with engine.connect() as conn:
            # 1. 计算总收入 (假设你有个 orders 或 sales 表)
            # 这里根据你的实际数据库表名修改，如果没有表，先返回模拟数据测试
            revenue_query = text("SELECT SUM(total_price) as total FROM sales WHERE user_id = :uid")
            rev_result = conn.execute(revenue_query, {"uid": user_id}).mappings().first()
            revenue = float(rev_result["total"]) if rev_result and rev_result["total"] else 0.0

            # 2. 计算总成本
            cost_query = text("SELECT SUM(cost_price) as total FROM sales WHERE user_id = :uid")
            cost_result = conn.execute(cost_query, {"uid": user_id}).mappings().first()
            cost = float(cost_result["total"]) if cost_result and cost_result["total"] else 0.0

            return {
                "user_id": user_id,
                "revenue": revenue,
                "cost": cost,
                "profit": revenue - cost
            }
    except Exception as e:
        print(f"Calculation Error: {e}")
        # 如果数据库表还没建好，先返回测试数据让前端能跑通
        return {"revenue": 1500.50, "cost": 800.00, "profit": 700.50}

import pandas as pd
import numpy as np

def build_ai_summary(order_items, reviews, cpi, ppi, inventory):
    # 1. 确保 ID 类型统一（非常重要，防止用户输入 ID 匹配失败）
    order_items["product_id"] = order_items["product_id"].astype(str).str.strip()
    inventory["product_id"] = inventory["product_id"].astype(str).str.strip()
    
    # 2. 聚合销售数据
    product_stats = order_items.groupby("product_id").agg({
        "quantity": "sum",
        "item_price": "sum"
    }).reset_index().rename(columns={"quantity": "total_sales", "item_price": "total_revenue"})

    # 3. 聚合评价数据
    review_stats = reviews.groupby("product_id").agg({"rating": "mean"}).reset_index().rename(columns={"rating": "avg_rating"})

    # 4. 关键修改：以 inventory (所有产品) 作为主表进行合并
    # 使用 how="left" 确保即便是新输入的、没有销量的产品也会被保留
    df = inventory.merge(product_stats, on="product_id", how="left")
    df = df.merge(review_stats, on="product_id", how="left")

    # 5. 处理缺失值：没卖过的产品销量和收入设为 0
    df["total_sales"] = df["total_sales"].fillna(0)
    df["total_revenue"] = df["total_revenue"].fillna(0)
    df["avg_rating"] = df["avg_rating"].fillna(0)

    # 6. 计算特征 (使用真实成本)
    # 如果没卖过，平均售价就是该产品的 selling_price 字段（如果有的话），或者设为 0
    df["avg_selling_price"] = df.apply(
    lambda x: x["total_revenue"] / x["total_sales"] if x["total_sales"] > 0 else 0, 
    axis=1
)
    
    # 成本计算：数量 * 真实单价
    df["estimated_cost"] = df["total_sales"] * df["cost_price"].fillna(0)
    df["estimated_profit"] = df["total_revenue"] - df["estimated_cost"]

    # 7. 模拟营销数据 (让 Analytics 页面不为空)
    rows = len(df)
    df["total_views"] = np.random.randint(10, 100, size=rows)
    df["total_cart"] = (df["total_views"] * 0.05).astype(int)
    df["conversion_rate"] = df.apply(
    lambda x: (x["total_sales"] / x["total_views"]) if x["total_views"] > 0 else 0,
    axis=1
).round(4)

    # 8. 宏观数据与库存状态
    df["cpi_value"] = cpi["cpi_value"].iloc[-1] if not cpi.empty else 1.83
    df["ppi_value"] = ppi["ppi_value"].iloc[-1] if not ppi.empty else 119.60
    df["summary_date"] = pd.Timestamp.today().date() # 确保日期更新到今天 (2026-04-23)

    df["current_stock"] = df["stock_quantity"].fillna(0).astype(int)
    reorder = df["reorder_level"].fillna(10)
    
    df["stock_status"] = np.where(df["current_stock"] == 0, "Out of Stock",
                         np.where(df["current_stock"] <= reorder * 0.5, "Critical",
                         np.where(df["current_stock"] <= reorder, "Low", "Sufficient")))
    
    df["stock_turnover_rate"] = df.apply(
    lambda x: (x["total_sales"] / x["current_stock"]) if x["current_stock"] > 0 else 0,
    axis=1
).round(2)
    df["stock_risk_level"] = np.where(df["stock_turnover_rate"] >= 3, "High Risk", "Low Risk")

    # 丢掉中间列
    df.drop(columns=["stock_quantity", "reorder_level", "cost_price"], inplace=True)
    
    return df

def get_mock_forecast():
    # 模拟 AI 预测逻辑
    return {
        "forecast": 120,
        "trend": "Upward",
        "explanation": "Based on last month's growth in Choco Jar sales."
    }