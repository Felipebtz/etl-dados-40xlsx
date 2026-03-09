"""
Supabase Bulk XLSX Uploader
Detecta automaticamente o tipo de arquivo (pedidos / produtos / clientes)
e insere na tabela correta via conexão direta PostgreSQL (psycopg2 + COPY).
"""

import asyncio
import io
import os
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path


import pandas as pd
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Supabase Bulk Uploader", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ──────────────────────────────────────────────────────────────
# CONFIGURAÇÃO — edite aqui ou use variáveis de ambiente
# ──────────────────────────────────────────────────────────────
CONFIG = {
    # Supabase → Settings → Database → Connection string (Transaction mode)
    # Nota: @ na senha precisa ser escapado como %40
    "database_url": os.getenv("DATABASE_URL", ""),
    "max_workers": int(os.getenv("MAX_WORKERS", "8")),
    "chunk_size":  int(os.getenv("CHUNK_SIZE",  "1000")),
}

# ──────────────────────────────────────────────────────────────
# SCHEMA DE CADA TABELA
# Colunas canônicas + tipos SQL usados no CREATE TABLE automático
# ──────────────────────────────────────────────────────────────
SCHEMAS = {
    "pedidos": {
        "columns": [
            ("cod_pedido",                     "BIGINT"),
            ("id_cliente",                     "BIGINT"),
            ("single_id",                      "TEXT"),
            ("sku",                            "BIGINT"),
            ("valor_item_pedido",              "NUMERIC"),
            ("valor_item_pedido_frete",        "NUMERIC"),
            ("qtd_itens",                      "INTEGER"),
            ("canal_venda",                    "TEXT"),
            ("bandeira",                       "TEXT"),
            ("data_pedido",                    "DATE"),
            ("cod_tip_frete",                  "INTEGER"),
            ("descricao_tipo_frete",           "TEXT"),
            ("data_atualizacao",               "DATE"),
            ("valor_frete",                    "NUMERIC"),
            ("flag_retira",                    "BOOLEAN"),
            ("data_entrega",                   "TEXT"),
            ("flag_marketplace",               "BOOLEAN"),
            ("flag_lista_casamento",           "BOOLEAN"),
            ("id_lista_casamento",             "BIGINT"),
            ("cod_motivo_cancelamento",        "INTEGER"),
            ("descricao_motivo_cancelamento",  "TEXT"),
            ("_source_file",                   "TEXT"),
            ("_loaded_at",                     "TIMESTAMPTZ"),
        ],
        # colunas originais do xlsx → nome canônico (case-insensitive, strip)
        "rename": {},   # já estão idênticas após sanitize
    },

    "produtos": {
        "columns": [
            ("sku",                                  "BIGINT"),
            ("nome_sku",                             "TEXT"),
            ("nome_agrupamento_diretoria_setor",     "TEXT"),
            ("nome_setor_gerencial",                 "TEXT"),
            ("nome_classe_gerencial",                "TEXT"),
            ("nome_especie_gerencial",               "TEXT"),
            ("nome_categoria_gerencial",             "TEXT"),
            ("nome_categoria_pai_gerencial",         "TEXT"),
            ("nome_departamento_pai_gerencial",      "TEXT"),
            ("nome_setor_origem",                    "TEXT"),
            ("nome_classe_origem",                   "TEXT"),
            ("nome_especie_origem",                  "TEXT"),
            ("nome_sub_especie",                     "TEXT"),
            ("nome_categoria_origem",                "TEXT"),
            ("nome_categoria_pai_origem",            "TEXT"),
            ("nome_departamento_pai_origem",         "TEXT"),
            ("nome_tipo_sku",                        "TEXT"),
            ("nome_marca",                           "TEXT"),
            ("nome_diretoria_setor_alternativa",     "TEXT"),
            ("nome_setor_alternativo",               "TEXT"),
            ("url_produto",                          "TEXT"),
            ("_source_file",                         "TEXT"),
            ("_loaded_at",                           "TIMESTAMPTZ"),
        ],
        "rename": {},
    },

    # Tabela genérica para os outros 20 arquivos de clientes
    # → adapte as colunas conforme seus arquivos reais
    "clientes": {
        "columns": [
            ("id_cliente",     "BIGINT"),
            ("single_id",      "TEXT"),
            ("nome",           "TEXT"),
            ("email",          "TEXT"),
            ("cpf",            "TEXT"),
            ("telefone",       "TEXT"),
            ("data_cadastro",  "DATE"),
            ("_source_file",   "TEXT"),
            ("_loaded_at",     "TIMESTAMPTZ"),
        ],
        "rename": {
            # exemplo: "NOME_CLIENTE" → "nome"
        },
    },
}

# ──────────────────────────────────────────────────────────────
# DETECÇÃO AUTOMÁTICA DO TIPO
# ──────────────────────────────────────────────────────────────
# Fingerprints: conjuntos de colunas que identificam unicamente cada tipo
FINGERPRINTS = {
    "pedidos":  {"cod_pedido", "id_cliente", "sku", "canal_venda", "bandeira"},
    "produtos": {"nome_sku", "nome_setor_gerencial", "nome_marca", "url_produto"},
    "clientes": {"id_cliente", "single_id"},   # fallback — adapte
}

def detect_table(df_columns: list[str]) -> str:
    cols = {sanitize_col(c) for c in df_columns}
    scores = {}
    for table, fingerprint in FINGERPRINTS.items():
        scores[table] = len(fingerprint & cols) / len(fingerprint)
    best = max(scores, key=scores.get)
    if scores[best] < 0.4:
        return "generico"   # nenhum schema conhecido
    return best

# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
def sanitize_col(name: str) -> str:
    name = str(name).strip()
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if name and name[0].isdigit():
        name = "col_" + name
    return name.lower()


def get_conn():
    return psycopg2.connect(CONFIG["database_url"])


def ensure_table(conn, table: str, schema_def: list[tuple]):
    """Cria a tabela se não existir com o schema canônico."""
    col_defs = ",\n  ".join(f'"{col}" {typ}' for col, typ in schema_def)
    sql = f"""
    CREATE TABLE IF NOT EXISTS public."{table}" (
      id BIGSERIAL PRIMARY KEY,
      {col_defs}
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def align_df(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """
    Renomeia, sanitiza e alinha o DataFrame ao schema canônico da tabela.
    Colunas faltantes → NULL. Colunas extras → descartadas.
    """
    schema = SCHEMAS.get(table)
    if not schema:
        return df   # tabela genérica — envia como veio

    # 1. rename mapeado
    rename_map = {k.lower(): v for k, v in schema.get("rename", {}).items()}
    df.columns = [sanitize_col(c) for c in df.columns]
    df = df.rename(columns=rename_map)

    # 2. adicionar colunas de auditoria
    df["_source_file"] = df.get("_source_file", None)
    df["_loaded_at"]   = datetime.utcnow().isoformat()

    # 3. alinhar ao schema canônico
    canonical_cols = [col for col, _ in schema["columns"]]
    for col in canonical_cols:
        if col not in df.columns:
            df[col] = None
    df = df[canonical_cols]

    # 4. converter NaN → None (psycopg2 entende None como NULL)
    df = df.where(pd.notnull(df), None)

    return df


def bulk_insert(conn, table: str, df: pd.DataFrame, chunk_size: int):
    """Insere em chunks usando execute_values (muito mais rápido que insert linha a linha)."""
    cols = list(df.columns)
    col_list = ", ".join(f'"{c}"' for c in cols)
    sql = f'INSERT INTO public."{table}" ({col_list}) VALUES %s'

    with conn.cursor() as cur:
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            rows  = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
            psycopg2.extras.execute_values(cur, sql, rows, page_size=chunk_size)
    conn.commit()

# ──────────────────────────────────────────────────────────────
# PROCESSAMENTO DE UM ARQUIVO
# ──────────────────────────────────────────────────────────────
def process_file(filename: str, content: bytes) -> dict:
    start  = time.time()
    result = {"file": filename, "status": "ok", "rows": 0, "table": "", "elapsed_s": 0, "error": None}

    try:
        xls = pd.ExcelFile(io.BytesIO(content))
        # lê a primeira aba
        df = pd.read_excel(xls, sheet_name=xls.sheet_names[0])
        df = df.dropna(how="all")

        table   = detect_table(list(df.columns))
        df["_source_file"] = filename
        df      = align_df(df, table)
        result["table"] = table
        result["rows"]  = len(df)

        conn = get_conn()
        try:
            schema_def = SCHEMAS.get(table, {}).get("columns", [])
            if schema_def:
                ensure_table(conn, table, schema_def)
            bulk_insert(conn, table, df, CONFIG["chunk_size"])
        finally:
            conn.close()

    except Exception:
        result["status"] = "error"
        result["error"]  = traceback.format_exc()

    result["elapsed_s"] = round(time.time() - start, 2)
    return result

# ──────────────────────────────────────────────────────────────
# ENDPOINTS
# ──────────────────────────────────────────────────────────────
@app.post("/upload")
async def upload_files(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(400, "Nenhum arquivo enviado.")
    if len(files) > 40:
        raise HTTPException(400, "Máximo de 40 arquivos por vez.")

    file_data = [(f.filename, await f.read()) for f in files]

    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as pool:
        tasks   = [loop.run_in_executor(pool, process_file, fn, ct) for fn, ct in file_data]
        results = await asyncio.gather(*tasks)

    ok    = [r for r in results if r["status"] == "ok"]
    error = [r for r in results if r["status"] != "ok"]
    return {"total": len(results), "success": len(ok), "failed": len(error), "results": list(results)}


@app.get("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        db_ok = True
    except Exception as e:
        db_ok = str(e)
    return {"status": "ok", "db_connected": db_ok}


if os.path.isdir("static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")