"""
Entrypoint Vercel na raiz: encaminha todas as rotas para o FastAPI (main.app).
Corrige o path para evitar 404 (ex.: /index.py/upload -> /upload).
"""
import os
import sys

# Garante que main.py está no path (mesma pasta na raiz)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from main import app as _app


async def _handler(scope, receive, send):
    path = (scope.get("path") or "").strip()
    # Vercel pode enviar /index.py, /index.py/upload, ou já / e /upload
    for prefix in ("/index.py", "/api/index.py", "/api/index"):
        if path == prefix or path.startswith(prefix + "/"):
            rest = path[len(prefix):] if path.startswith(prefix + "/") else ""
            scope = dict(scope)
            scope["path"] = rest if rest else "/"
            if "raw_path" in scope:
                scope["raw_path"] = scope["path"].encode("utf-8")
            break
    await _app(scope, receive, send)


app = _handler
