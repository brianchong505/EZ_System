import sys
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from pydantic import BaseModel

# --- 1. PATH FIX ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

# --- 2. LOCAL IMPORTS ---
from db import get_engine
from services import product_service 

from services.dashboard_service import get_user_dashboard_data, get_mock_forecast 
app = FastAPI(title="EZ_System SME API")
engine = get_engine()

# --- 3. CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 4. MODELS ---
class LoginRequest(BaseModel):
    email: str
    password: str

# 增加注册请求模型，确保 business_name 被接收
class RegisterRequest(BaseModel):
    user_id: str
    email: str
    password: str
    business_name: str 

# --- 5. USER & AUTH ROUTES ---

@app.post("/auth/register")
async def register(request: RegisterRequest):
    """ ✅ 新增注册接口：将 Business Name 存入数据库的 name 字段 """
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO users (user_id, email, password_hash, name) 
                VALUES (:uid, :email, :pw, :name)
            """), {
                "uid": request.user_id, 
                "email": request.email, 
                "pw": request.password, 
                "name": request.business_name # 关联 Business Name
            })
        return {"status": "success"}
    except Exception as e:
        # 如果 ID 或 Email 重复，会报错
        raise HTTPException(status_code=400, detail=f"Registration failed: {str(e)}")

@app.post("/auth/login")
def login(request: LoginRequest):
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

# --- 6. PRODUCT ROUTES ---

@app.get("/products/{user_id}")
async def fetch_user_products(user_id: str):
    return product_service.get_products_by_user(user_id)

# main.py 中的相关部分
@app.post("/products/")
async def add_new_product(data: dict):
    # 打印收到的数据，看 stock 有没有传过来
    print(f"DEBUG: Received product data: {data}")
    success = product_service.create_product(data)
    if not success:
        raise HTTPException(status_code=400, detail="Database insertion failed")
    return {"status": "success"}

@app.put("/products/{product_id}")
async def update_product(product_id: str, data: dict):
    success = product_service.update_product_and_stock(product_id, data)
    if not success:
        raise HTTPException(status_code=400, detail="Update failed")
    return {"status": "success"}

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

@app.post("/sales/record")
async def record_sale(data: dict):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO sales (user_id, product_id, selling_price, cost_price, sale_date)
            VALUES (:uid, :pid, :price, :cost, NOW())
        """), {"uid": data['user_id'], "pid": data['product_id'], "price": data['price'], "cost": data['cost']})
        
        conn.execute(text("""
            UPDATE stock SET quantity = quantity - 1 
            WHERE product_id = :pid AND quantity > 0
        """), {"pid": data['product_id']})
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

    