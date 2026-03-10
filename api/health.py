"""
Vercel serverless: GET /api/health
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI

from main import get_conn

app = FastAPI()


@app.get("/api/health")
@app.get("/")
def health():
    try:
        conn = get_conn()
        conn.close()
        db_ok = True
    except Exception as e:
        db_ok = str(e)
    return {"status": "ok", "db_connected": db_ok}
