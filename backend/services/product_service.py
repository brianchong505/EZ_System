from sqlalchemy import text
from db import get_engine

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
    
def create_product(data):
    try:
        # 使用 begin() 开启事务，如果其中一个 INSERT 失败，全部都会回滚，保证数据一致性
        with engine.begin() as conn:
            # 1. 插入产品基本信息到 products 表
            conn.execute(text("""
                INSERT INTO products (product_id, user_id, product_name, selling_price)
                VALUES (:pid, :uid, :name, :price)
            """), {
                "pid": data['product_id'],
                "uid": data['user_id'],
                "name": data['product_name'],
                "price": data['selling_price']
            })

            # 2. 插入初始库存到独立的 stock 表
            # 假设你的 stock 表字段是 product_id 和 quantity
            conn.execute(text("""
                INSERT INTO stock (product_id, quantity)
                VALUES (:pid, :qty)
            """), {
                "pid": data['product_id'],
                "qty": data['stock']
            })
        return True
    except Exception as e:
        print(f"Database Error: {e}")
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