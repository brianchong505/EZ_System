from dotenv import load_dotenv, find_dotenv
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from pydantic import BaseModel
from services.analysis_service import update_ai_summary_table

# --- 1. PATH FIX ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

# --- 2. LOCAL IMPORTS ---
from backend.Database.db import get_engine
from temp_ai_engine import run_ai_engine, save_results

# ---------------------------
# Environment setup
# ---------------------------
env_path = find_dotenv()
print("Using .env file:", env_path)
load_dotenv(dotenv_path=env_path)
print("Loaded AI_API_KEY:", os.getenv("AI_API_KEY"))

app = FastAPI()
engine = get_engine()

# Allow frontend (Flutter) to call backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # adjust if you want stricter rules
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------
# Analytics route (per user)
# ---------------------------
@app.get("/analytics/{user_id}")
def analytics(user_id: str):
    """
    Run AI engine for given user_id and return results.
    """
    data = run_ai_engine(user_id)
    save_results(data)
    return {"ai_result": data}

# ---------------------------
# Batch refresh route (all users)
# ---------------------------
@app.post("/refresh_all")
def refresh_all():
    """
    Refresh AI suggestions for ALL users.
    """
    with engine.connect() as conn:
        # ✅ 登录时同时返回 name 字段
        query = text("SELECT user_id, password_hash, name FROM users WHERE email = :email")
        result = conn.execute(query, {"email": request.email}).mappings().first()
        
        if result and result["password_hash"] == request.password:
            return {
                "status": "success", 
                "user_id": str(result["user_id"]),
                "name": result["name"] or "My Business"
            }
    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.get("/users/{user_id}")
async def get_user(user_id: str):
    with engine.connect() as conn:
        query = text("SELECT user_id, email, name FROM users WHERE user_id = :uid")
        result = conn.execute(query, {"uid": user_id}).mappings().first()
        
        if result:
            return {
                "user_id": result["user_id"],
                "email": result["email"],
                "name": result["name"] or "New SME User"
            }
    raise HTTPException(status_code=404, detail="User not found")

@app.put("/users/{user_id}")
async def update_user(user_id: str, data: dict):
    try:
        with engine.begin() as conn:
            # 这里的字段要和你的数据库匹配，假设只有 name
            conn.execute(
                text("UPDATE users SET name = :name WHERE user_id = :uid"),
                {"name": data['name'], "uid": user_id}
            )
        return {"status": "success", "message": "Profile updated"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- 6. PRODUCT ROUTES ---

@app.get("/products/{user_id}")
async def fetch_user_products(user_id: str):
    return product_service.get_products_by_user(user_id)

# main.py 中的相关部分
@app.post("/products/")
async def add_new_product(data: dict):
    success = product_service.create_product(data)
    if not success:
        raise HTTPException(status_code=400, detail="Insertion failed")

    # 注意：这里的 Key 必须和 Flutter 传过来的一模一样（比如 'user_id' 还是 'userId'）
    u_id = data.get("user_id") 
    p_id = data.get("product_id")

    print(f"DEBUG: Syncing for User: {u_id}, Product: {p_id}") # 看看控制台有没有打印这行
    
    # 这里的参数顺序：engine, 字符串, 字符串
    update_ai_summary_table(engine, u_id, p_id) 
    
    return {"status": "success"}
    

@app.put("/products/{product_id}")
async def update_product(product_id: str, data: dict):
    success = product_service.update_product_and_stock(product_id, data)
    if not success:
        raise HTTPException(status_code=400, detail="Update failed")
    u_id = data.get('user_id')
    p_id = data.get('product_id')
    if u_id and p_id:
        update_ai_summary_table(engine, u_id, p_id)
    return {"status": "success"}

# 在 main.py 中添加这个路由
@app.delete("/products/{product_id}")
async def delete_existing_product(product_id: str):
    try:
        with engine.begin() as conn:
            # 1. 先删 stock 表记录（因为它引用了 product_id）
            conn.execute(text("DELETE FROM stock WHERE product_id = :pid"), {"pid": product_id})
            
            # 2. ✅ 新增：先删掉 sales 表中引用了该产品的记录
            conn.execute(text("DELETE FROM sales WHERE product_id = :pid"), {"pid": product_id})
            
            # 3. 最后删 products 表
            conn.execute(text("DELETE FROM products WHERE product_id = :pid"), {"pid": product_id})
            
        return {"status": "success"}
    except Exception as e:
        print(f"ERROR: {e}")
        raise HTTPException(status_code=400, detail=f"Database Error: {str(e)}")

# --- 7. DASHBOARD & SALES ---

# ✅ 必须把这个移到前面！
@app.get("/dashboard/demand-forecast")
async def get_ai_forecast():
    return {
        "forecast": 150,
        "trend": "increasing",
        "explanation": "Higher demand expected based on last month's Choco Jar sales."
    }

# ❌ 把这个带变量的移到后面
@app.get("/dashboard/{user_id}")
async def get_dashboard(user_id: str):
    print(f"DEBUG: Dashboard fetching for user: {user_id}")
    with engine.connect() as conn:
        res = conn.execute(text("""
            SELECT SUM(selling_price) as rev, SUM(cost_price) as cost 
            FROM sales WHERE user_id = :uid
        """), {"uid": user_id}).mappings().first()
        
        return {
            "revenue": float(res['rev'] or 0.0),
            "cost": float(res['cost'] or 0.0)
        }

# 修改售出：减去对应的数量
@app.post("/sales/record")
async def record_sale(data: dict):
    qty = data.get('quantity', 1)  # 获取卖出的数量
    
    # ✅ 计算这一单的总售价和总成本
    total_revenue = data['price'] * qty
    total_cost = data['cost'] * qty

    with engine.begin() as conn:
        # 1. 扣除库存
        conn.execute(text("""
            UPDATE stock SET quantity = quantity - :qty 
            WHERE product_id = :pid AND quantity >= :qty
        """), {"pid": data['product_id'], "qty": qty})
        
        # 2. 记录销售：存入总价 (Total) 而不是单价 (Unit Price)
        conn.execute(text("""
            INSERT INTO sales (user_id, product_id, selling_price, cost_price, sale_date)
            VALUES (:uid, :pid, :price, :cost, NOW())
        """), {
            "uid": data['user_id'], 
            "pid": data['product_id'], 
            "price": total_revenue, # ✅ 这里现在存的是 16.0 (8 * 2)
            "cost": total_cost      # ✅ 这里现在存的是 10.0 (5 * 2)
        })
    u_id = data.get('user_id')
    p_id = data.get('product_id')
    if u_id and p_id:
        update_ai_summary_table(engine, u_id, p_id)
    return {"status": "success"}

# 新增补货：直接增加库存
@app.post("/products/restock")
async def restock_product(data: dict):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE stock SET quantity = quantity + :adj 
            WHERE product_id = :pid
        """), {"pid": data['product_id'], "adj": data['adjustment']})
    return {"status": "success"}

# --- 在 main.py 的路由部分 ---

@app.get("/alerts/{user_id}")
async def get_business_alerts(user_id: str):
    alerts = []
    
    try:
        with engine.connect() as conn:
            # --- 维度 1: 库存过低检测 (Low Stock) ---
            stock_query = text("""
                SELECT p.product_name, s.quantity 
                FROM products p 
                JOIN stock s ON p.product_id = s.product_id 
                WHERE p.user_id = :uid AND s.quantity < 10
            """)
            low_stock = conn.execute(stock_query, {"uid": user_id}).mappings().all()
            for item in low_stock:
                alerts.append({
                    "type": "critical",
                    "title": "库存紧缺 (Low Stock)",
                    "message": f"产品 '{item['product_name']}' 仅剩 {item['quantity']} 件，请尽快补货。"
                })

            # --- 维度 2: 利润异常/成本过高检测 (Low Profit Margin) ---
            # 假设你希望利润率低于 10% 时报警
            profit_query = text("""
                SELECT product_name, selling_price, 
                (SELECT AVG(cost_price) FROM sales WHERE product_id = p.product_id) as avg_cost
                FROM products p WHERE user_id = :uid
            """)
            margin_check = conn.execute(profit_query, {"uid": user_id}).mappings().all()
            for item in margin_check:
                if item['avg_cost'] and (item['selling_price'] - item['avg_cost']) / item['selling_price'] < 0.1:
                    alerts.append({
                        "type": "warning",
                        "title": "利润预警 (Low Profit)",
                        "message": f"产品 '{item['product_name']}' 的利润率过低，请检查定价或进货成本。"
                    })

            # --- 维度 3: 市场/通胀模拟 (Inflation Alert) ---
            # 这种通常来自 AI 预测或外部参数，这里模拟当平均成本上涨时提醒
            alerts.append({
                "type": "info",
                "title": "通胀提醒 (Inflation)",
                "message": "近期原材料成本普遍上涨 6%，建议适当调整售价以维持利润。"
            })

        return alerts
    except Exception as e:
        print(f"Alert Logic Error: {e}")
        return []
    
# --- 8. AI SUMMARY PIPELINE ---
@app.post("/analytics/sync/{user_id}")
async def sync_analytics_data(user_id: str):
    success = update_ai_summary_table(engine, user_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to sync analysis data")
    return {"status": "success", "message": "Summary table updated"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
