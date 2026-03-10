"""
Vercel serverless: POST /api/setup_enriquecida
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI

from main import VIEW_PEDIDOS_ENRIQUECIDA, get_conn

app = FastAPI()


@app.post("/api/setup_enriquecida")
@app.post("/")
def setup_enriquecida():
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(VIEW_PEDIDOS_ENRIQUECIDA)
            conn.commit()
            return {"status": "ok", "message": "View public.pedidos_enriquecida criada/atualizada."}
        finally:
            conn.close()
    except Exception as e:
        return {"status": "error", "message": str(e)}
