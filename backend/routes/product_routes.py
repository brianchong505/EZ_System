from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from db import get_db

router = APIRouter()


# -------------------------
# CREATE PRODUCT
# -------------------------
@router.post("/")
def create_product(product: dict, db: Session = Depends(get_db)):

    sql = text("""
        INSERT INTO products (user_id, name, cost, price, stock)
        VALUES (:user_id, :name, :cost, :price, :stock)
    """)

    db.execute(sql, product)
    db.commit()

    return {"message": "Product created"}


# -------------------------
# GET PRODUCTS
# -------------------------
@router.get("/{user_id}")
def get_products(user_id: int, db: Session = Depends(get_db)):

    sql = text("""
        SELECT * FROM products
        WHERE user_id = :user_id
    """)

    result = db.execute(sql, {"user_id": user_id}).mappings().all()

    return result