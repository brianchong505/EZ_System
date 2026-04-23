# backend/dashboard_service.py
from sqlalchemy import text
from db import get_engine

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

def get_mock_forecast():
    # 模拟 AI 预测逻辑
    return {
        "forecast": 120,
        "trend": "Upward",
        "explanation": "Based on last month's growth in Choco Jar sales."
    }