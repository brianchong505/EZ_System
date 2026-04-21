import pandas as pd
from db import get_engine

engine = get_engine()

def load_data():
    orders      = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)
    reviews     = pd.read_sql("SELECT * FROM reviews", engine)
    cpi         = pd.read_sql("SELECT * FROM cpi", engine)
    ppi         = pd.read_sql("SELECT * FROM ppi", engine)
    inventory   = pd.read_sql("SELECT product_id, stock_quantity, reorder_level FROM inventory", engine)

    return orders, order_items, reviews, cpi, ppi, inventory