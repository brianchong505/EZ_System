import pandas as pd
from sqlalchemy import text
import numpy as np
import traceback 

def update_ai_summary_table(engine, user_id, product_id=None):
    """
    针对特定的 product_id 进行实时汇总，并同步至 ai_product_summary 表
    """
    try:
        from .dashboard_service import build_ai_summary
        
        # 1. 数据抓取阶段
        with engine.connect() as conn:
            # 抓取库存与基础价格
            inventory = pd.read_sql(text("""
                SELECT p.product_id, IFNULL(s.quantity, 0) as stock_quantity, 
                       p.cost_price, p.selling_price, 10 as reorder_level 
                FROM products p 
                LEFT JOIN stock s ON p.product_id = s.product_id 
                WHERE p.product_id = :pid AND p.user_id = :uid
            """), conn, params={"pid": product_id, "uid": user_id})

            # 抓取销售记录
            order_items = pd.read_sql(text("""
                SELECT product_id, selling_price as item_price, 1 as quantity 
                FROM sales WHERE product_id = :pid AND user_id = :uid
            """), conn, params={"pid": product_id, "uid": user_id})
            
            # 定义必要的 Mock 数据防止 NameError
            reviews = pd.DataFrame(columns=["product_id", "rating"])
            cpi = pd.DataFrame({"cpi_value": [1.83]})
            ppi = pd.DataFrame({"ppi_value": [119.60]})

        # 检查数据是否存在
        if inventory.empty:
            print(f"⚠️ 找不到产品 {product_id}，同步取消")
            return False

        # 2. 计算阶段 (确保 dashboard_service 中已修复除以零逻辑)
        df_final = build_ai_summary(order_items, reviews, cpi, ppi, inventory)
        
        # 3. 数据清洗阶段 (解决 Unknown Column 报错的核心)
        # 确保包含 user_id
        df_final["user_id"] = user_id
        
        # 统一列名：如果代码里叫 selling_price 但数据库叫 avg_selling_price，在此统一
        if 'selling_price' in df_final.columns and 'avg_selling_price' not in df_final.columns:
            df_final = df_final.rename(columns={'selling_price': 'avg_selling_price'})

        # 获取数据库中 ai_product_summary 表的真实列名
        with engine.connect() as conn:
            db_columns_query = conn.execute(text("SHOW COLUMNS FROM ai_product_summary"))
            db_columns = [row[0] for row in db_columns_query.fetchall()]
        
        # 只保留数据库中存在的列，过滤掉多余的字段防止报错
        final_columns = [col for col in df_final.columns if col in db_columns]
        df_to_save = df_final[final_columns]

        # 4. 写入数据库
        with engine.begin() as conn:
            # 清理旧数据
            conn.execute(text("DELETE FROM ai_product_summary WHERE product_id = :pid AND user_id = :uid"), 
                         {"pid": product_id, "uid": user_id})
            
            # 写入新数据
            df_to_save.to_sql('ai_product_summary', con=conn, if_exists='append', index=False)
            print(f"✅ 同步成功: {product_id} 已存入汇总表")
            
        return True

    except Exception as e:
        print("❌ 同步崩溃！详情：")
        traceback.print_exc()
        return False