import pandas as pd
from db import get_engine

engine = get_engine()

def load_data():
    sales      = pd.read_sql("SELECT * FROM sales", engine)
    reviews    = pd.read_sql("SELECT * FROM reviews", engine)
    cpi        = pd.read_sql("SELECT * FROM cpi", engine)
    ppi        = pd.read_sql("SELECT * FROM ppi", engine)
    inventory  = pd.read_sql("SELECT product_id, stock_quantity, reorder_level FROM inventory", engine)
    categories = pd.read_sql("SELECT * FROM categories", engine)
    products   = pd.read_sql("SELECT product_id, user_id, ctg_id, cost_price, selling_price FROM products", engine)
    return sales, reviews, cpi, ppi, inventory, categories, products