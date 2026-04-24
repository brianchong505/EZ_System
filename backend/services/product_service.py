from sqlalchemy import text
from backend.Database.db import get_engine

engine = get_engine()

def get_products_by_user(user_id: str):
    try:
        with engine.connect() as conn:
            # Use explicit naming for the parameter
            query = text("""
                SELECT 
                    p.product_id, 
                    p.product_name, 
                    p.selling_price,
                    p.cost_price, 
                    COALESCE(s.quantity, 0) as stock
                FROM products p
                LEFT JOIN stock s ON p.product_id = s.product_id
                WHERE p.user_id = :uid
            """)
            
            # Pass the dictionary with the matching key 'uid'
            result = conn.execute(query, {"uid": user_id}).mappings().all()
            
            products = [dict(row) for row in result]
            print(f"DEBUG: Found {len(products)} products for user {user_id}") # Check your terminal!
            return products
    except Exception as e:
        print(f"SQL Error: {e}")
        return []
    
# services/product_service.py

def create_product(data):
    try:
        with engine.begin() as conn:
            # 1. 插入产品表，必须显式包含 cost_price
            conn.execute(
                text("""
                    INSERT INTO products (product_id, user_id, product_name, cost_price, selling_price)
                    VALUES (:pid, :uid, :name, :cost, :price)
                """),
                {
                    "pid": data['product_id'],
                    "uid": data['user_id'],
                    "name": data['product_name'],
                    "cost": data.get('cost_price', 0.0), # ✅ 这里一定要取前端传来的值
                    "price": data['selling_price']
                }
            )
            # 2. 插入库存表
            conn.execute(
                text("INSERT INTO stock (product_id, quantity) VALUES (:pid, :qty)"),
                {"pid": data['product_id'], "qty": data.get('stock', 0)}
            )
        return True
    except Exception as e:
        print(f"Service Error: {e}")
        return False

def update_product_and_stock(product_id, data):
    try:
        with engine.begin() as conn:
            # 更新产品表
            conn.execute(
                text("""
                    UPDATE products 
                    SET product_name = :name, cost_price = :cost, selling_price = :price 
                    WHERE product_id = :pid
                """),
                {
                    "name": data['product_name'],
                    "cost": data.get('cost_price', 0.0), # ✅ 确保更新时也包含成本
                    "price": data['selling_price'],
                    "pid": product_id
                }
            )
            # 如果也需要更新库存数量
            if 'stock' in data:
                conn.execute(
                    text("UPDATE stock SET quantity = :qty WHERE product_id = :pid"),
                    {"qty": data['stock'], "pid": product_id}
                )
        return True
    except Exception as e:
        print(f"Update Service Error: {e}")
        return False
    
def update_product_and_stock(product_id: str, data: dict):
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE products SET product_name = :product_name, selling_price = :selling_price
                WHERE product_id = :product_id
            """), {"product_name": data["product_name"], "selling_price": data["selling_price"], "product_id": product_id})
            
            conn.execute(text("""
                UPDATE stock SET quantity = :stock WHERE product_id = :product_id
            """), {"stock": data["stock"], "product_id": product_id})
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False