#!/bin/bash
# Roda o app após git clone + .env configurado.
# Uso: chmod +x run.sh && ./run.sh

cd "$(dirname "$0")"
if [ -d "venv" ]; then
  source venv/bin/activate
fi
if [ ! -f ".env" ]; then
  echo "Crie o .env a partir de .env.example e preencha DATABASE_URL."
  exit 1
fi
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
