from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from db import get_db

router = APIRouter()


# -------------------------
# REGISTER
# -------------------------
@router.post("/register")
def register(user: dict, db: Session = Depends(get_db)):

    sql = text("""
        INSERT INTO users (name, email, password, category)
        VALUES (:name, :email, :password, :category)
    """)

    db.execute(sql, user)
    db.commit()

    return {"message": "User registered successfully"}


# -------------------------
# LOGIN
# -------------------------
@router.post("/login")
def login(user: dict, db: Session = Depends(get_db)):

    sql = text("""
        SELECT * FROM users
        WHERE email = :email AND password = :password
    """)

    result = db.execute(sql, user).mappings().first()

    if result:
        return {"message": "Login success", "user_id": result["user_id"]}

    return {"error": "Invalid credentials"}