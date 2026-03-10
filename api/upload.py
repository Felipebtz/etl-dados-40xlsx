"""
Vercel serverless: POST /api/upload (rota nativa, evita 404).
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, File, Form, HTTPException, UploadFile

from main import CONFIG, get_conn, process_file

app = FastAPI()


@app.post("/api/upload")
@app.post("/")  # fallback caso a Vercel passe path como /
async def upload(
    files: list[UploadFile] = File(...),
    replace_pedidos: bool = Form(False),
):
    if not files:
        raise HTTPException(400, "Nenhum arquivo enviado.")
    if len(files) > 40:
        raise HTTPException(400, "Máximo de 40 arquivos por vez.")

    if replace_pedidos:
        try:
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE public.pedidos RESTART IDENTITY CASCADE")
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            raise HTTPException(500, f"Erro ao limpar pedidos: {e}")

    file_data = [(f.filename, await f.read()) for f in files]
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as pool:
        tasks = [loop.run_in_executor(pool, process_file, fn, ct) for fn, ct in file_data]
        results = await asyncio.gather(*tasks)

    ok = [r for r in results if r["status"] == "ok"]
    failed = [r for r in results if r["status"] != "ok"]
    return {"total": len(results), "success": len(ok), "failed": len(failed), "results": list(results)}
