import os
import json
import requests
import re
from datetime import date
from sqlalchemy import text
from backend.Database.db import get_engine
from dotenv import load_dotenv

load_dotenv()
engine = get_engine()

# =================================================
# CONFIG
# =================================================
AI_API_KEY   = os.getenv("AI_API_KEY", None)
AI_API_URL   = os.getenv("AI_API_URL")
AI_API_MODEL = os.getenv("AI_API_MODEL", "ilmu-glm-5.1")
MODEL_VERSION = f"{AI_API_MODEL}-v1.0"

# =================================================
# 1. RULE-BASED ENGINE
# =================================================
def rule_engine(row):
    output = {
        "recommendation":    [],
        "trade_off_analysis": [],
        "impact_analysis":   [],
        "forecast":          {},
        "explanation_trace": [],
        "confidence_score":  100,
    }

    # --- Low conversion ---
    if row["conversion_rate"] is not None and float(row["conversion_rate"]) < 0.02:
        output["recommendation"].append("Improve pricing or product page optimization")
        output["impact_analysis"].append("Low conversion reduces revenue efficiency")
        output["explanation_trace"].append({
            "rule": "LOW_CONVERSION",
            "trigger": f"conversion_rate={row['conversion_rate']} < 0.02",
            "explanation": f"Only {float(row['conversion_rate'])*100:.2f}% of visitors purchased — industry minimum is 2%",
            "severity": "HIGH"
        })

    # --- High views, low cart ---
    if row["total_views"] > 1000 and row["total_cart"] < 50:
        output["recommendation"].append("Improve product visibility-to-cart funnel")
        output["trade_off_analysis"].append(
            "High exposure but weak purchase intent suggests UX or pricing mismatch"
        )
        output["explanation_trace"].append({
            "rule": "FUNNEL_DROP",
            "trigger": f"views={row['total_views']}, cart={row['total_cart']}",
            "explanation": f"{row['total_views']} people viewed but only {row['total_cart']} added to cart — severe funnel leakage",
            "severity": "MEDIUM"
        })

    # --- Negative profit ---
    if row["estimated_profit"] is not None and float(row["estimated_profit"]) < 0:
        output["recommendation"].append("Increase price or reduce cost structure")
        output["impact_analysis"].append("Product is currently operating at a loss")
        output["explanation_trace"].append({
            "rule": "NEGATIVE_PROFIT",
            "trigger": f"estimated_profit={row['estimated_profit']}",
            "explanation": f"Selling at a loss of RM{abs(float(row['estimated_profit'])):.2f} — every sale costs money",
            "severity": "CRITICAL"
        })

    # --- Low rating ---
    if row["avg_rating"] is not None and float(row["avg_rating"]) < 3:
        output["recommendation"].append("Improve product quality or supplier reliability")
        output["impact_analysis"].append("Poor rating will reduce long-term demand")
        output["explanation_trace"].append({
            "rule": "LOW_RATING",
            "trigger": f"avg_rating={row['avg_rating']} < 3.0",
            "explanation": f"Rating of {row['avg_rating']} signals customer dissatisfaction — directly suppresses repeat purchases",
            "severity": "HIGH"
        })

    # --- Stock risk ---
    if row.get("stock_risk_level") == "HIGH":
        output["recommendation"].append("Urgently restock to avoid stockout")
        output["impact_analysis"].append("Stockout will lead to lost sales and poor customer experience")
        output["explanation_trace"].append({
            "rule": "HIGH_STOCK_RISK",
            "trigger": f"stock_risk_level=HIGH, current_stock={row.get('current_stock')}",
            "explanation": f"Only {row.get('current_stock')} units remaining — stockout is imminent",
            "severity": "CRITICAL"
        })

    # --- Low turnover ---
    if row.get("stock_turnover_rate") is not None and float(row["stock_turnover_rate"]) < 0.5:
        output["recommendation"].append("Reduce inventory or run promotions to clear stock")
        output["trade_off_analysis"].append("Low turnover ties up capital but discounting reduces margin")
        output["explanation_trace"].append({
            "rule": "LOW_TURNOVER",
            "trigger": f"stock_turnover_rate={row['stock_turnover_rate']} < 0.5",
            "explanation": f"Turnover rate of {row['stock_turnover_rate']} means stock sits unsold — capital locked in idle inventory",
            "severity": "MEDIUM"
        })

    # --- Dead stock ---
    if row.get("total_sales") == 0 and row.get("current_stock", 0) > 0:
        output["recommendation"].append("Consider discontinuing or heavily discounting this product")
        output["impact_analysis"].append("Dead stock increases holding cost and reduces cash flow")
        output["explanation_trace"].append({
            "rule": "DEAD_STOCK",
            "trigger": f"total_sales=0, current_stock={row.get('current_stock')}",
            "explanation": "Zero sales with stock on hand — product has no market demand this period",
            "severity": "CRITICAL"
        })

    # --- PPI: supplier cost pressure ---
    if row.get("ppi_value") is not None and float(row["ppi_value"]) > 110:
        output["recommendation"].append("Supplier cost rising — consider price adjustment or new supplier")
        output["trade_off_analysis"].append("Raising price may reduce demand but protects margin")
        output["explanation_trace"].append({
            "rule": "HIGH_PPI",
            "trigger": f"ppi_value={row['ppi_value']} > 110",
            "explanation": f"PPI of {row['ppi_value']} indicates upstream cost inflation — margin will compress if price stays fixed",
            "severity": "MEDIUM"
        })

    # --- CPI: consumer pressure ---
    if row.get("cpi_value") is not None and float(row["cpi_value"]) > 110:
        output["recommendation"].append("Consumers are price sensitive — avoid aggressive pricing")
        output["explanation_trace"].append({
            "rule": "HIGH_CPI",
            "trigger": f"cpi_value={row['cpi_value']} > 110",
            "explanation": f"CPI of {row['cpi_value']} means consumers are under inflation pressure — price hikes risk demand collapse",
            "severity": "MEDIUM"
        })

    # --- Confidence: penalise missing data ---
    if row.get("cpi_value") is None:
        output["confidence_score"] -= 10
    if row.get("ppi_value") is None:
        output["confidence_score"] -= 10
    if row.get("stock_turnover_rate") is None:
        output["confidence_score"] -= 5
    output["confidence_score"] = max(output["confidence_score"], 0)

    # --- Business score ---
    score = 100
    if row.get("conversion_rate") is not None and float(row["conversion_rate"]) < 0.02:
        score -= 20
    if row.get("avg_rating") is not None and float(row["avg_rating"]) < 3:
        score -= 20
    if row.get("stock_risk_level") == "HIGH":
        score -= 15
    if row.get("estimated_profit") is not None and float(row["estimated_profit"]) < 0:
        score -= 25
    output["business_score"] = max(score, 0)

    # --- Stock action ---
    stock_action = "NORMAL"
    if row.get("stock_risk_level") == "HIGH":
        stock_action = "RESTOCK"
    elif row.get("stock_turnover_rate") is not None and float(row["stock_turnover_rate"]) < 0.5:
        stock_action = "CLEAR"
    elif row.get("current_stock") is not None and int(row["current_stock"]) > 1000:
        stock_action = "OVERSTOCK"
    output["stock_action"] = stock_action

    # --- Forecast ---
    if row["total_revenue"] is not None:
        output["forecast"]["next_period_revenue"] = round(float(row["total_revenue"]) * 1.05, 2)

    return {
        "recommendation":    "; ".join(output["recommendation"]),
        "trade_off_analysis": "; ".join(output["trade_off_analysis"]),
        "impact_analysis":   "; ".join(output["impact_analysis"]),
        "forecast":          output["forecast"],
        "explanation_trace": output["explanation_trace"],
        "business_score":    output["business_score"],
        "confidence_score":  int(output["confidence_score"]),
        "stock_action":      output["stock_action"],
    }


# =================================================
# 2. LLM ENGINE
# =================================================
def llm_engine(row, rule_output):
    if not AI_API_KEY:
        return {
            "recommendation":    rule_output["recommendation"],
            "trade_off_analysis": rule_output["trade_off_analysis"],
            "impact_analysis":   rule_output["impact_analysis"],
            "llm_explanation":   "AI not available — rule-based analysis only"
        }

    trace_text = "\n".join([
        f"- [{t['severity']}] {t['rule']}: {t['explanation']} (triggered by: {t['trigger']})"
        for t in rule_output.get("explanation_trace", [])
    ]) or "No rule violations detected."

    # ↓ ADD THIS BLOCK
    prompt = f"""
You are a senior business intelligence analyst explaining your reasoning to a business owner.

The rule engine has already identified these issues for product {row['product_id']}:

{trace_text}

Product context:
- Total Sales    : {row['total_sales']} units
- Total Revenue  : RM{row['total_revenue']}
- Estimated Profit: RM{row['estimated_profit']}
- Conversion Rate: {row['conversion_rate']}%
- Average Rating : {row['avg_rating']}/5
- Current Stock  : {row.get('current_stock')}
- Stock Turnover : {row.get('stock_turnover_rate')}
- Stock Risk     : {row.get('stock_risk_level')}
- CPI            : {row.get('cpi_value')}
- PPI            : {row.get('ppi_value')}
- Business Score : {rule_output['business_score']}/100
- Confidence     : {rule_output['confidence_score']}%
- Current Forecast (rule-based): RM{rule_output['forecast'].get('next_period_revenue')}

Your task:
1. Explain in plain language WHY this product is performing this way
2. Give ONE clear priority action the business should take first
3. Explain the trade-off of that action honestly
4. State the impact if nothing is done
5. Based on current trends, provide an adjusted revenue forecast for next period

Return ONLY a JSON object, no markdown:
{{
  "recommendation": "specific actionable recommendation",
  "trade_off_analysis": "honest trade-off of the recommended action",
  "impact_analysis": "what happens if nothing is done",
  "llm_explanation": "plain English explanation of WHY this product is in this situation, 2-3 sentences",
  "forecast": {{
    "next_period_revenue": <number only, no RM symbol>,
    "forecast_note": "one sentence explaining the forecast adjustment"
  }}
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
                "model": AI_API_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1500,
                "temperature": 0.2
            },
            timeout=60
        )
        data = response.json()

        if "content" in data and len(data["content"]) > 0:
            content_text = data["content"][0].get("text", "").strip()
            json_match = re.search(r'\{.*\}', content_text, re.DOTALL)

            if json_match:
                parsed = json.loads(json_match.group(0))

                llm_forecast = parsed.get("forecast", {})
                rule_forecast = rule_output.get("forecast", {})
                merged_forecast = {**rule_forecast, **llm_forecast}

                return {
                    "recommendation":     parsed.get("recommendation",     rule_output["recommendation"]),
                    "trade_off_analysis": parsed.get("trade_off_analysis", rule_output["trade_off_analysis"]),
                    "impact_analysis":    parsed.get("impact_analysis",    rule_output["impact_analysis"]),
                    "llm_explanation":    parsed.get("llm_explanation",    ""),
                    "forecast":           merged_forecast
                }

        # Reached here means: no content block OR no JSON found in response
        return {
            "recommendation":     rule_output["recommendation"],
            "trade_off_analysis": rule_output["trade_off_analysis"],
            "impact_analysis":    rule_output["impact_analysis"],
            "llm_explanation":    "AI response could not be parsed — rule-based fallback used",
            "forecast":           rule_output.get("forecast", {})
        }

    except Exception as e:
        print(f"  ⚠️  LLM error for {row['product_id']}: {e}")
        return {
            "recommendation":    rule_output["recommendation"],
            "trade_off_analysis": rule_output["trade_off_analysis"],
            "impact_analysis":   rule_output["impact_analysis"],
            "llm_explanation":   f"LLM call failed: {str(e)}"
        }


# =================================================
# 3. MAIN PIPELINE
# =================================================
def run_ai_engine(user_id: str):
    print(f"🚀 Running AI Engine for user: {user_id}")

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM ai_product_summary WHERE user_id = :uid"),
            {"uid": user_id}
        ).mappings().all()

    if not rows:
        print("⚠️  No products found for this user.")
        return []

    results = []

    for row in rows:
        print(f"  - Analyzing {row['product_id']}...")

        rule_output  = rule_engine(row)
        final_output = llm_engine(row, rule_output)

        # Use LLM forecast if available, fall back to rule forecast
        forecast_dict = final_output.get("forecast") or rule_output.get("forecast", {})

        results.append({
            "product_id":         row["product_id"],
            "user_id":            user_id,
            "recommendation":     final_output.get("recommendation", ""),
            "trade_off_analysis": final_output.get("trade_off_analysis", ""),
            "impact_analysis":    final_output.get("impact_analysis", ""),
            "llm_explanation":    final_output.get("llm_explanation", ""),
            "forecast":             json.dumps(forecast_dict),  # saved as TEXT,
            "predicted_revenue":  forecast_dict.get("next_period_revenue"),
            "predicted_cost":     float(row["estimated_cost"]) if row.get("estimated_cost") is not None else None,
            "business_score":     rule_output["business_score"],
            "confidence_score":   rule_output["confidence_score"],
            "stock_action":       rule_output["stock_action"],
            "explanation_trace":  json.dumps(rule_output["explanation_trace"]),
            "model_version":      MODEL_VERSION,
            "summary_date":       date.today().isoformat(),
        })

    return results


# =================================================
# 4. SAVE TO DB
# =================================================
def save_results(results):
    if not results:
        print("⚠️  Nothing to save.")
        return

    print(f"💾 Saving {len(results)} results for user_id={results[0]['user_id']}...")

    sql = text("""
        INSERT INTO ai_results (
            product_id,
            user_id,
            recommendation,
            trade_off_analysis,
            impact_analysis,
            llm_explanation,
            forecast,
            predicted_revenue,
            predicted_cost,
            business_score,
            confidence_score,
            stock_action,
            explanation_trace,
            model_version,
            summary_date
        )
        VALUES (
            :product_id,
            :user_id,
            :recommendation,
            :trade_off_analysis,
            :impact_analysis,
            :llm_explanation,
            :forecast,
            :predicted_revenue,
            :predicted_cost,
            :business_score,
            :confidence_score,
            :stock_action,
            :explanation_trace,
            :model_version,
            :summary_date
        )
        ON DUPLICATE KEY UPDATE
            recommendation     = VALUES(recommendation),
            trade_off_analysis = VALUES(trade_off_analysis),
            impact_analysis    = VALUES(impact_analysis),
            llm_explanation    = VALUES(llm_explanation),
            forecast           = VALUES(forecast),
            predicted_revenue  = VALUES(predicted_revenue),
            predicted_cost     = VALUES(predicted_cost),
            business_score     = VALUES(business_score),
            confidence_score   = VALUES(confidence_score),
            stock_action       = VALUES(stock_action),
            explanation_trace  = VALUES(explanation_trace),
            model_version      = VALUES(model_version),
            summary_date       = VALUES(summary_date)
    """)

    with engine.begin() as conn:
        conn.execute(sql, results)

    print(f"✅ {len(results)} products saved to ai_results.")


# =================================================
# 5. ENTRY POINT
# =================================================
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Run for specific user
        uid = sys.argv[1]
        data = run_ai_engine(uid)
        save_results(data)
    else:
        # Run for ALL users in the table
        with engine.connect() as conn:
            user_rows = conn.execute(
                text("SELECT DISTINCT user_id FROM ai_product_summary WHERE user_id IS NOT NULL")
            ).fetchall()

        for (uid,) in user_rows:
            print(f"\n{'='*50}")
            data = run_ai_engine(uid)
            save_results(data)

    print("🎯 AI Engine Completed")