"""
Único entrypoint Vercel: encaminha todas as rotas para o FastAPI (main.app).
Corrige o path quando a Vercel envia /api/index ou /api/index/upload.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import app as _app


async def _handler(scope, receive, send):
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


app = _handler
