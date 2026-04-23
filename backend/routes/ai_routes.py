from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from db import get_db

router = APIRouter()

@router.get("/summary/{product_id}")
def get_ai_summary(product_id: str, db: Session = Depends(get_db)):

    sql = text("""
        SELECT * FROM ai_product_summary
        WHERE product_id = :product_id
    """)

    result = db.execute(sql, {"product_id": product_id}).mappings().first()

    return result