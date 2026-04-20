from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from passlib.context import CryptContext
from jose import jwt
from datetime import datetime, timedelta

app = FastAPI()

SECRET_KEY = "secret123"
ALGORITHM = "HS256"

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Fake DB
users_db = {}

# Models
class RegisterModel(BaseModel):
    name: str
    email: EmailStr
    password: str
    business_type: str

class LoginModel(BaseModel):
    email: EmailStr
    password: str

# Helpers
def hash_password(password):
    return pwd_context.hash(password)

def verify_password(password, hashed):
    return pwd_context.verify(password, hashed)

def create_token(email):
    payload = {
        "sub": email,
        "exp": datetime.utcnow() + timedelta(hours=24)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

# Routes
@app.post("/register")
def register(user: RegisterModel):
    if user.email in users_db:
        raise HTTPException(status_code=400, detail="Email already exists")

    users_db[user.email] = {
        "name": user.name,
        "email": user.email,
        "password": hash_password(user.password),
        "business_type": user.business_type
    }

    return {"message": "Registered successfully"}

@app.post("/login")
def login(user: LoginModel):
    db_user = users_db.get(user.email)

    if not db_user:
        raise HTTPException(status_code=401, detail="User not found")

    if not verify_password(user.password, db_user["password"]):
        raise HTTPException(status_code=401, detail="Wrong password")

    token = create_token(user.email)

    return {
        "access_token": token,
        "token_type": "bearer"
    }

#hi
