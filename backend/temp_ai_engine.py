import os
import json
import requests
import re
from sqlalchemy import text
from db import get_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
engine = get_engine()

# =================================================
# CONFIG
# =================================================
AI_API_KEY = os.getenv("AI_API_KEY", None)
AI_API_URL = os.getenv("AI_API_URL")
AI_API_MODEL = os.getenv("AI_API_MODEL")

# =================================================
# 1. RULE-BASED ENGINE
# =================================================
def rule_engine(row):
    output = {
        "recommendation": [],
        "trade_off_analysis": [],
        "impact_analysis": [],
        "forecast": {}
    }

    if row["conversion_rate"] is not None and float(row["conversion_rate"]) < 0.02:
        output["recommendation"].append("Improve pricing or product page optimization")
        output["impact_analysis"].append("Low conversion reduces revenue efficiency")

    if row["total_views"] > 1000 and row["total_cart"] < 50:
        output["recommendation"].append("Improve product visibility-to-cart funnel")
        output["trade_off_analysis"].append(
            "High exposure but weak purchase intent suggests UX or pricing mismatch"
        )

    if row["estimated_profit"] is not None and float(row["estimated_profit"]) < 0:
        output["recommendation"].append("Increase price or reduce cost structure")
        output["impact_analysis"].append("Product is currently operating at a loss")

    if row["avg_rating"] is not None and float(row["avg_rating"]) < 3:
        output["recommendation"].append("Improve product quality or supplier reliability")
        output["impact_analysis"].append("Poor rating will reduce long-term demand")

    if row["total_revenue"] is not None:
        output["forecast"]["next_period_revenue"] = round(float(row["total_revenue"]) * 1.05, 2)

    return {
        "recommendation": "; ".join(output["recommendation"]),
        "trade_off_analysis": "; ".join(output["trade_off_analysis"]),
        "impact_analysis": "; ".join(output["impact_analysis"]),
        "forecast": output["forecast"]
    }

# =================================================
# 2. LLM ENGINE (ILMU API Integration)
# =================================================
def llm_engine(row, rule_output):
    if not AI_API_KEY:
        return {
            "recommendation": rule_output["recommendation"],
            "trade_off_analysis": rule_output["trade_off_analysis"],
            "impact_analysis": rule_output["impact_analysis"]
        }

    prompt = f"""
Analyze this SPECIFIC product performance and provide a unique strategy.
Product ID: {row['product_id']}
Metrics:
- Total Sales: {row['total_sales']} units
- Total Revenue: RM{row['total_revenue']}
- Estimated Profit: RM{row['estimated_profit']}
- Conversion Rate: {row['conversion_rate']}%
- Average Rating: {row['avg_rating']}/5

Return ONLY a JSON object:
{{
  "recommendation": "Specific action for this product",
  "trade_off_analysis": "What is gained vs lost by this action",
  "impact_analysis": "Long term business impact"
}}
"""

    try:
        response = requests.post(
            AI_API_URL,
            headers={
                "x-api-key": AI_API_KEY,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json"
            },
            json={
                "model": AI_API_MODEL or "ilmu-glm-5.1",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 500,   # updated to 500
                "temperature": 0.2
            }
        )
        data = response.json()

        if "content" in data and len(data["content"]) > 0:
            content_text = data["content"][0].get("text", "").strip()
            json_match = re.search(r'\{.*\}', content_text, re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group(0))
                return {
                    "recommendation": parsed.get("recommendation", rule_output["recommendation"]),
                    "trade_off_analysis": parsed.get("trade_off_analysis", "Review resource allocation."),
                    "impact_analysis": parsed.get("impact_analysis", rule_output["impact_analysis"])
                }

        return {
            "recommendation": rule_output["recommendation"],
            "trade_off_analysis": "No additional AI analysis available.",
            "impact_analysis": rule_output["impact_analysis"]
        }

    except Exception as e:
        print(f"Error for Product {row['product_id']}: {e}")
        return rule_output

# =================================================
# 3. MAIN AI ENGINE PIPELINE
# =================================================
def run_ai_engine(user_id: str):
    print(f"🚀 Running AI Engine for user: {user_id}")

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM ai_product_summary WHERE user_id = :uid"),
            {"uid": user_id}
        ).mappings().all()

    if not rows:
        print("⚠️ No products found for this user.")
        return []

    results = []
    for row in rows:
        print(f"  - Analyzing {row['product_id']}...")
        rule_output = rule_engine(row)
        final_output = llm_engine(row, rule_output)
        forecast = rule_output.get("forecast", {})

        results.append({
            "product_id": row["product_id"],
            "user_id": user_id,
            "recommendation": final_output.get("recommendation"),
            "trade_off_analysis": final_output.get("trade_off_analysis"),
            "impact_analysis": final_output.get("impact_analysis"),
            "predicted_revenue": forecast.get("next_period_revenue", 0),
            "predicted_cost": float(row.get("estimated_cost") or 0)
        })

    return results

# =================================================
# 4. SAVE RESULTS
# =================================================
def save_results(results):
    if not results:
        return

    print(f"💾 Saving {len(results)} results for user_id={results[0]['user_id']}...")

    sql = text("""
        INSERT INTO ai_results (
            product_id,
            user_id,
            recommendation,
            trade_off_analysis,
            impact_analysis,
            predicted_revenue,
            predicted_cost
        )
        VALUES (
            :product_id,
            :user_id,
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

    print("✅ AI results updated.")
