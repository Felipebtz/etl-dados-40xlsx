"""
Vercel serverless entry: expõe o app FastAPI para o runtime Python da Vercel.
Corrige o path da requisição quando a Vercel envia /api/index ou /api/index.py.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import app as _app


async def _fix_path(scope, receive, send):
    """Reescreve path: /api/index ou /api/index.py/upload -> / ou /upload (evita 404 na Vercel)."""
    path = (scope.get("path") or "").strip()
    for prefix in ("/api/index.py", "/api/index"):
        if path == prefix or path.startswith(prefix + "/"):
            rest = path[len(prefix):] if path.startswith(prefix + "/") else ""
            scope = dict(scope)
            scope["path"] = rest if rest else "/"
            if "raw_path" in scope:
                scope["raw_path"] = scope["path"].encode("utf-8")
            break
    await _app(scope, receive, send)


# Vercel procura a variável "app" (ASGI)
app = _fix_path
