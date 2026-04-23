import os
import json
import numpy as np
from sqlalchemy import text
from db import get_engine

engine = get_engine()

# =================================================
# CONFIG (API KEY WILL BE PROVIDED LATER)
# =================================================
AI_API_KEY = os.getenv("AI_API_KEY", None)

# =================================================
# 1. RULE-BASED ENGINE (CORE LOGIC)
# =================================================
def rule_engine(row):
    """
    Deterministic business intelligence layer
    """

    output = {
        "recommendation": [],
        "trade_off_analysis": [],
        "impact_analysis": [],
        "forecast": {}
    }

    # ---------------------------
    # LOW CONVERSION ISSUE
    # ---------------------------
    if row["conversion_rate"] is not None and row["conversion_rate"] < 0.02:
        output["recommendation"].append("Improve pricing or product page optimization")
        output["impact_analysis"].append("Low conversion reduces revenue efficiency")

    # ---------------------------
    # HIGH VIEWS BUT LOW CART
    # ---------------------------
    if row["total_views"] > 1000 and row["total_cart"] < 50:
        output["recommendation"].append("Improve product visibility-to-cart funnel")
        output["trade_off_analysis"].append(
            "High exposure but weak purchase intent suggests UX or pricing mismatch"
        )

    # ---------------------------
    # NEGATIVE PROFIT
    # ---------------------------
    if row["estimated_profit"] is not None and row["estimated_profit"] < 0:
        output["recommendation"].append("Increase price or reduce cost structure")
        output["impact_analysis"].append("Product is currently operating at a loss")

    # ---------------------------
    # LOW RATING
    # ---------------------------
    if row["avg_rating"] is not None and row["avg_rating"] < 3:
        output["recommendation"].append("Improve product quality or supplier reliability")
        output["impact_analysis"].append("Poor rating will reduce long-term demand")

    # ---------------------------
    # SIMPLE FORECAST (BASELINE)
    # ---------------------------
    if row["total_revenue"] is not None:
        output["forecast"]["next_period_revenue"] = round(float(row["total_revenue"]) * 1.05, 2)

    # Convert lists to strings for DB storage
    # 找到 rule_engine 函数末尾，修改为：
    return {
        "recommendation": "; ".join(output["recommendation"]),
        "trade_off_analysis": "; ".join(output["trade_off_analysis"]),
        "impact_analysis": "; ".join(output["impact_analysis"]),
        "forecast": output["forecast"]  # <--- 保持字典格式，不要用 json.dumps
    }


# =================================================
# 2. LLM ENGINE (READY FOR API KEY LATER)
# =================================================
def llm_engine(row, rule_output):
    """
    AI reasoning layer (activated when API key is available)
    """

    if not AI_API_KEY:
        # SAFE FALLBACK MODE
        return {
            "recommendation": rule_output["recommendation"],
            "trade_off_analysis": rule_output["trade_off_analysis"],
            "impact_analysis": rule_output["impact_analysis"]
        }

    # ---------------------------
    # IMPORT LATER (when API is provided)
    # ---------------------------
    # Example placeholder (OpenAI / GLM style)
    prompt = f"""
You are a business AI analyst.

Product Data:
- Sales: {row['total_sales']}
- Revenue: {row['total_revenue']}
- Profit: {row['estimated_profit']}
- Conversion Rate: {row['conversion_rate']}
- Rating: {row['avg_rating']}
- CPI: {row['cpi_value']}
- PPI: {row['ppi_value']}

Rule-based insights:
{rule_output}

Task:
1. Explain product performance
2. Provide business recommendation
3. Analyze trade-offs
4. Predict next trend

Return JSON format:
{{
  "recommendation": "...",
  "trade_off_analysis": "...",
  "impact_analysis": "..."
}}
"""

    # ---------------------------
    # PLACEHOLDER RESPONSE (WAIT FOR KEY)
    # ---------------------------
    return {
        "recommendation": rule_output["recommendation"],
        "trade_off_analysis": rule_output["trade_off_analysis"],
        "impact_analysis": rule_output["impact_analysis"]
    }


# =================================================
# 3. MAIN AI ENGINE PIPELINE
# =================================================
# 找到 run_ai_engine 函数
def run_ai_engine():
    print("🚀 Running AI Engine...")

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM ai_product_summary")).mappings().all()

    results = []

    # ... 前面的代码保持不变 ...
    for row in rows:
        rule_output = rule_engine(row)
        final_output = llm_engine(row, rule_output)

        # --- 这里的逻辑是“防弹”的 ---
        raw_forecast = rule_output.get("forecast", {})
        
        # 核心修复：如果它是字符串，我们就尝试解析它；如果解析失败或者是空的，给它个默认字典
        if isinstance(raw_forecast, str):
            try:
                forecast_dict = json.loads(raw_forecast)
            except:
                forecast_dict = {}
        else:
            forecast_dict = raw_forecast if raw_forecast is not None else {}

        results.append({
            "product_id": row["product_id"],
            "recommendation": final_output["recommendation"],
            "trade_off_analysis": final_output["trade_off_analysis"],
            "impact_analysis": final_output["impact_analysis"],
            # 使用我们处理后的 forecast_dict 来 get
            "predicted_revenue": forecast_dict.get("next_period_revenue") if isinstance(forecast_dict, dict) else None,
            "predicted_cost": float(row.get("estimated_cost")) if row.get("estimated_cost") is not None else None
        })

    return results


# =================================================
# 4. INSERT INTO ai_results TABLE
# =================================================
def save_results(results):
    print("💾 Saving AI results...")

    for r in results:
        if isinstance(r.get("forecast"), dict):
            r["forecast"] = json.dumps(r["forecast"])

    sql = text("""
        INSERT INTO ai_results (
            product_id,
            recommendation,
            trade_off_analysis,
            impact_analysis,
            predicted_revenue,
            predicted_cost
        )
        VALUES (
            :product_id,
            :recommendation,
            :trade_off_analysis,
            :impact_analysis,
            :predicted_revenue,
            :predicted_cost
        )
        ON DUPLICATE KEY UPDATE
            recommendation = VALUES(recommendation),
            trade_off_analysis = VALUES(trade_off_analysis),
            impact_analysis = VALUES(impact_analysis),
            predicted_revenue = VALUES(predicted_revenue),
            predicted_cost = VALUES(predicted_cost)
    """)

    with engine.begin() as conn:
        conn.execute(sql, results)

    print("✅ AI results saved successfully")


# =================================================
# 5. EXECUTION ENTRY POINT
# =================================================
if __name__ == "__main__":
    data = run_ai_engine()
    save_results(data)
    print("🎯 AI Engine Completed")