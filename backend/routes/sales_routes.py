from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from db import get_db

router = APIRouter()


# -------------------------
# ADD SALES
# -------------------------
@router.post("/")
def add_sales(sales: dict, db: Session = Depends(get_db)):

    sql = text("""
        INSERT INTO order_items (user_id, product_name, cost, price, units_sold)
        VALUES (:user_id, :product_name, :cost, :price, :units_sold)
    """)

    db.execute(sql, sales)
    db.commit()

    return {"message": "Sales recorded"}


# -------------------------
# GET SALES
# -------------------------
@router.get("/{user_id}")
def get_sales(user_id: int, db: Session = Depends(get_db)):

    sql = text("""
        SELECT * FROM order_items
        WHERE user_id = :user_id
    """)

    result = db.execute(sql, {"user_id": user_id}).mappings().all()

    return result