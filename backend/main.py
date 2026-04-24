from dotenv import load_dotenv, find_dotenv
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from db import get_engine
from temp_ai_engine import run_ai_engine, save_results

# ---------------------------
# Environment setup
# ---------------------------
env_path = find_dotenv()
print("Using .env file:", env_path)
load_dotenv(dotenv_path=env_path)
print("Loaded AI_API_KEY:", os.getenv("AI_API_KEY"))

app = FastAPI()
engine = get_engine()

# Allow frontend (Flutter) to call backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # adjust if you want stricter rules
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------
# Analytics route (per user)
# ---------------------------
@app.get("/analytics/{user_id}")
def analytics(user_id: str):
    """
    Run AI engine for given user_id and return results.
    """
    data = run_ai_engine(user_id)
    save_results(data)
    return {"ai_result": data}

# ---------------------------
# Batch refresh route (all users)
# ---------------------------
@app.post("/refresh_all")
def refresh_all():
    """
    Refresh AI suggestions for ALL users.
    """
    with engine.connect() as conn:
        user_ids = conn.execute(
            text("SELECT DISTINCT user_id FROM ai_product_summary WHERE user_id IS NOT NULL")
        ).fetchall()

    refreshed = []
    for (uid,) in user_ids:
        print(f"🚀 Running AI Engine for user_id={uid}")
        results = run_ai_engine(uid)
        valid_results = [r for r in results if r.get("product_id")]
        if valid_results:
            save_results(valid_results)
            refreshed.append(uid)

    return {"message": "Refresh complete", "users_refreshed": refreshed}

# ---------------------------
# Root route
# ---------------------------
@app.get("/")
def root():
    return {"message": "EZ System backend is running"}

# ---------------------------
# Entry point
# ---------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
