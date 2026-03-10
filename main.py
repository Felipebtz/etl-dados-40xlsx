"""
Supabase Bulk XLSX Uploader
Detecta automaticamente o tipo de arquivo (pedidos / produtos / clientes)
e insere na tabela correta via conexão direta PostgreSQL (psycopg2 + COPY).
"""

from dotenv import load_dotenv
load_dotenv()

import asyncio
import gzip
import io
import os
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pandas as pd

# Encodings e separadores para CSVs (mesmo padrão, origens diferentes)
CSV_ENCODINGS = ("utf-8", "utf-8-sig", "cp1252", "latin-1", "iso-8859-1")
CSV_SEPARATORS = (",", ";", "\t", "|")
MAX_DB_RETRIES = 3
RETRY_DELAY_SEC = 1
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Supabase Bulk Uploader", version="2.0.0")

# Sem limite de body size (default Starlette é 100MB)
# Para Vercel, o limite real é controlado pelo vercel.json
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
    """Conexão com retry para falhas transitórias (ex.: Vercel/serverless)."""
    last_err = None
    for attempt in range(MAX_DB_RETRIES):
        try:
            return psycopg2.connect(CONFIG["database_url"])
        except Exception as e:
            last_err = e
            if attempt < MAX_DB_RETRIES - 1:
                time.sleep(RETRY_DELAY_SEC * (attempt + 1))
    raise last_err


def read_csv_robust(raw_content: bytes) -> pd.DataFrame:
    """
    ETL robusto para CSV: tenta vários encodings e separadores.
    Ideal para vários CSVs com mesmo padrão vindos de fontes diferentes.
    """
    for encoding in CSV_ENCODINGS:
        for sep in CSV_SEPARATORS:
            try:
                df = pd.read_csv(
                    io.BytesIO(raw_content),
                    sep=sep,
                    encoding=encoding,
                    on_bad_lines="skip",
                    low_memory=False,
                )
                if len(df) == 0 or len(df.columns) < 2:
                    continue
                # Descarta colunas vazias de nome Unnamed
                df = df.loc[:, ~df.columns.str.match(r"^Unnamed:\s*\d+$", na=False)]
                if len(df.columns) < 2:
                    continue
                return df
            except Exception:
                continue
    raise ValueError("Não foi possível ler o CSV com nenhum encoding/separador conhecido.")


def ensure_table(conn, table: str, schema_def: list[tuple] | None = None, columns_from_df: list[str] | None = None):
    """
    Cria a tabela se não existir.
    schema_def: lista (col, tipo) para tabelas conhecidas.
    columns_from_df: para tabela generico — colunas do DataFrame (todas como TEXT).
    """
    if schema_def:
        col_defs = ",\n  ".join(f'"{col}" {typ}' for col, typ in schema_def)
    elif columns_from_df:
        col_defs = ",\n  ".join(f'"{c}" TEXT' for c in columns_from_df)
    else:
        return
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
    Para tabela "generico", sanitiza colunas e adiciona auditoria.
    """
    df.columns = [sanitize_col(c) for c in df.columns]
    df["_source_file"] = df.get("_source_file", None)
    df["_loaded_at"] = datetime.utcnow().isoformat()

    schema = SCHEMAS.get(table)
    if not schema:
        # Tabela genérica: mantém todas as colunas + auditoria
        df = df.where(pd.notnull(df), None)
        return df

    # 1. rename mapeado
    rename_map = {k.lower(): v for k, v in schema.get("rename", {}).items()}
    df = df.rename(columns=rename_map)

    # 2. alinhar ao schema canônico
    canonical_cols = [col for col, _ in schema["columns"]]
    for col in canonical_cols:
        if col not in df.columns:
            df[col] = None
    df = df[canonical_cols]

    # 3. converter NaN → None (psycopg2 entende None como NULL)
    df = df.where(pd.notnull(df), None)

    return df


def bulk_insert(conn, table: str, df: pd.DataFrame, chunk_size: int):
    """Insere em chunks com retry (execute_values)."""
    cols = list(df.columns)
    col_list = ", ".join(f'"{c}"' for c in cols)
    sql = f'INSERT INTO public."{table}" ({col_list}) VALUES %s'
    last_err = None
    for attempt in range(MAX_DB_RETRIES):
        try:
            with conn.cursor() as cur:
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i : i + chunk_size]
                    rows = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
                    psycopg2.extras.execute_values(cur, sql, rows, page_size=chunk_size)
            conn.commit()
            return
        except Exception as e:
            last_err = e
            conn.rollback()
            if attempt < MAX_DB_RETRIES - 1:
                time.sleep(RETRY_DELAY_SEC * (attempt + 1))
    raise last_err

# ──────────────────────────────────────────────────────────────
# PROCESSAMENTO DE UM ARQUIVO
# ──────────────────────────────────────────────────────────────
def process_file(filename: str, content: bytes) -> dict:
    start  = time.time()
    result = {"file": filename, "status": "ok", "rows": 0, "table": "", "elapsed_s": 0, "error": None}

    try:
        # descompactar gzip se vier do browser comprimido
        real_filename = filename
        raw_content   = content
        if filename.lower().endswith(".gz"):
            raw_content   = gzip.decompress(content)
            real_filename = filename[:-3]  # remove o .gz
            result["file"] = real_filename  # mostrar nome original no resultado

        # ETL CSV: vários encodings/separadores (mesmo padrão, fontes diferentes)
        if real_filename.lower().endswith(".csv"):
            df = read_csv_robust(raw_content)
        else:
            xls = pd.ExcelFile(io.BytesIO(raw_content))
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
                ensure_table(conn, table, schema_def=schema_def)
            else:
                # Tabela generico: cria com colunas do CSV (mesmo padrão)
                ensure_table(conn, table, columns_from_df=list(df.columns))
            bulk_insert(conn, table, df, CONFIG["chunk_size"])
        finally:
            conn.close()

    except Exception:
        result["status"] = "error"
        result["error"]  = traceback.format_exc()

    result["elapsed_s"] = round(time.time() - start, 2)
    return result

# ──────────────────────────────────────────────────────────────
# VIEW: BASE ENRIQUECIDA (Pedidos × Produtos × Clientes)
# Cruza: pedidos.sku → produtos, pedidos.single_id → clientes
# ──────────────────────────────────────────────────────────────
VIEW_PEDIDOS_ENRIQUECIDA = """
CREATE OR REPLACE VIEW public.pedidos_enriquecida AS
SELECT
  p.id AS pedido_id,
  p.cod_pedido,
  p.id_cliente,
  p.single_id,
  p.sku,
  p.valor_item_pedido,
  p.valor_item_pedido_frete,
  p.qtd_itens,
  p.canal_venda,
  p.bandeira,
  p.data_pedido,
  p.cod_tip_frete,
  p.descricao_tipo_frete,
  p.data_atualizacao,
  p.valor_frete,
  p.flag_retira,
  p.data_entrega,
  p.flag_marketplace,
  p.flag_lista_casamento,
  p.id_lista_casamento,
  p.cod_motivo_cancelamento,
  p.descricao_motivo_cancelamento,
  p._source_file AS pedido_source_file,
  p._loaded_at AS pedido_loaded_at,
  prod.nome_sku AS produto_nome_sku,
  prod.nome_marca AS produto_marca,
  prod.nome_setor_gerencial AS produto_setor,
  prod.nome_categoria_gerencial AS produto_categoria,
  prod.url_produto AS produto_url,
  c.nome AS cliente_nome,
  c.email AS cliente_email,
  c.cpf AS cliente_cpf,
  c.telefone AS cliente_telefone,
  c.data_cadastro AS cliente_data_cadastro
FROM public.pedidos p
LEFT JOIN public.produtos prod ON p.sku = prod.sku
LEFT JOIN public.clientes c ON p.single_id = c.single_id;
"""


@app.post("/setup-enriquecida")
def setup_enriquecida():
    """
    Cria/atualiza a view pedidos_enriquecida (base final cruzada).
    Pedidos × Produtos (SKU) × Clientes (single_id).
    Rode depois de subir as 3 bases.
    """
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


# ──────────────────────────────────────────────────────────────
# ENDPOINTS
# ──────────────────────────────────────────────────────────────
@app.post("/upload")
async def upload_files(
    files: list[UploadFile] = File(...),
    replace_pedidos: bool = Form(False),
):
    """
    Upload em lote. Opcional: replace_pedidos=true para troca diária
    (apaga pedidos antes de inserir os novos).
    """
    if not files:
        raise HTTPException(400, "Nenhum arquivo enviado.")
    if len(files) > 40:
        raise HTTPException(400, "Máximo de 40 arquivos por vez.")

    if replace_pedidos:
        try:
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute('TRUNCATE TABLE public.pedidos RESTART IDENTITY CASCADE')
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


from fastapi.responses import HTMLResponse

# Interface servida pelo arquivo index.html (evita HTML duplicado no código)
INDEX_HTML_PATH = Path(__file__).resolve().parent / "index.html"


@app.get("/", response_class=HTMLResponse)
def frontend():
    return HTMLResponse(content=INDEX_HTML_PATH.read_text(encoding="utf-8"))