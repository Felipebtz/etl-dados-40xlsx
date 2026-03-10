"""
Supabase Bulk XLSX Uploader
Detecta automaticamente o tipo de arquivo (pedidos / produtos / clientes)
e insere na tabela correta via PostgreSQL ou, se configurado, via GCP Storage + BigQuery.
"""

import asyncio
import gzip
import io
import json
import os
import re
import time
import traceback
import uuid
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Encodings e separadores para CSVs (mesmo padrão, origens diferentes)
CSV_ENCODINGS = ("utf-8", "utf-8-sig", "cp1252", "latin-1", "iso-8859-1")
CSV_SEPARATORS = (",", ";", "\t", "|")
MAX_DB_RETRIES = 3
RETRY_DELAY_SEC = 1
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
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
# GCP (Storage + BigQuery) — quando configurado, upload vai direto para GCS e load no BigQuery
# ──────────────────────────────────────────────────────────────
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "").strip()
GCP_BUCKET = os.getenv("GCP_BUCKET", "").strip()
GCP_DATASET = os.getenv("GCP_DATASET", "etl").strip()
GCP_CREDENTIALS_JSON = os.getenv("GCP_CREDENTIALS_JSON", "").strip()


def _gcp_credentials():
    """Retorna credenciais GCP a partir de env (JSON string ou caminho do arquivo)."""
    if GCP_CREDENTIALS_JSON:
        try:
            info = json.loads(GCP_CREDENTIALS_JSON)
            from google.oauth2 import service_account
            return service_account.Credentials.from_service_account_info(info)
        except Exception:
            return None
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if path and os.path.isfile(path):
        from google.oauth2 import service_account
        return service_account.Credentials.from_service_account_file(path)
    return None


def is_gcp_configured():
    return bool(GCP_PROJECT_ID and GCP_BUCKET and _gcp_credentials())


def get_gcp_storage_client():
    if not is_gcp_configured():
        return None
    from google.cloud import storage
    return storage.Client(project=GCP_PROJECT_ID, credentials=_gcp_credentials())


def get_bigquery_client():
    if not is_gcp_configured():
        return None
    from google.cloud import bigquery
    return bigquery.Client(project=GCP_PROJECT_ID, credentials=_gcp_credentials())

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
    Lê tudo como string para evitar OOM e NumericValueOutOfRange —
    a conversão de tipos fica a cargo do PostgreSQL via cast implícito.
    """
    for encoding in CSV_ENCODINGS:
        for sep in CSV_SEPARATORS:
            try:
                df = pd.read_csv(
                    io.BytesIO(raw_content),
                    sep=sep,
                    encoding=encoding,
                    on_bad_lines="skip",
                    low_memory=True,
                    dtype=str,          # tudo string → sem inferência de tipo
                    keep_default_na=False,
                )
                if len(df) == 0 or len(df.columns) < 2:
                    continue
                # Descarta colunas vazias de nome Unnamed
                df = df.loc[:, ~df.columns.str.match(r"^Unnamed:\s*\d+$", na=False)]
                if len(df.columns) < 2:
                    continue
                # Normaliza células vazias para None
                df = df.replace({"": None, "nan": None, "NaN": None, "None": None, "NULL": None})
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

    # 3. converter NaN/strings vazias → None (psycopg2 entende None como NULL)
    df = df.where(pd.notnull(df), None)
    df = df.replace({"": None, "nan": None, "NaN": None})

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
            df = pd.read_excel(xls, sheet_name=xls.sheet_names[0], dtype=str)
            df = df.replace({"": None, "nan": None, "NaN": None, "None": None, "NULL": None})
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
@app.post("/api/setup_enriquecida")
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
# GCP: modo Storage + BigQuery (upload direto do browser → GCS, load no BQ)
# ──────────────────────────────────────────────────────────────
@app.get("/config")
def get_config():
    """Indica se o backend está em modo GCP (Storage + BigQuery) ou Postgres."""
    return {"mode": "gcp" if is_gcp_configured() else "postgres"}


from pydantic import BaseModel


class GcpUploadUrlRequest(BaseModel):
    filename: str
    table_type: str  # pedidos | produtos | clientes | generico
    content_type: str = "application/octet-stream"


class GcpLoadRequest(BaseModel):
    gcs_uri: str
    table_type: str
    replace_pedidos: bool = False


@app.post("/gcp/upload-url")
def gcp_upload_url(body: GcpUploadUrlRequest):
    """
    Gera URL assinada para o frontend enviar o arquivo direto ao GCS.
    O Railway não recebe o arquivo — evita 502 com arquivos grandes.
    """
    if not is_gcp_configured():
        raise HTTPException(503, "GCP não configurado (GCP_PROJECT_ID, GCP_BUCKET, GCP_CREDENTIALS_JSON).")
    client = get_gcp_storage_client()
    bucket = client.bucket(GCP_BUCKET)
    safe_name = re.sub(r"[^a-zA-Z0-9._-]", "_", body.filename)
    object_path = f"uploads/{datetime.utcnow().strftime('%Y-%m-%d')}/{uuid.uuid4().hex}_{safe_name}"
    blob = bucket.blob(object_path)
    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(hours=2),
        method="PUT",
        content_type=body.content_type or "application/octet-stream",
    )
    gcs_uri = f"gs://{GCP_BUCKET}/{object_path}"
    return {"upload_url": url, "gcs_uri": gcs_uri, "object_name": object_path}


@app.post("/gcp/load")
def gcp_load(body: GcpLoadRequest):
    """
    Dispara job de load no BigQuery a partir do arquivo já no GCS.
    Suporta CSV. Cria o dataset se não existir.
    """
    if not is_gcp_configured():
        raise HTTPException(503, "GCP não configurado.")
    if not body.gcs_uri.startswith("gs://"):
        raise HTTPException(400, "gcs_uri deve ser gs://bucket/path")
    from google.cloud import bigquery
    client = get_bigquery_client()
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, GCP_DATASET)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(dataset_ref)
    table_id = body.table_type if body.table_type in ("pedidos", "produtos", "clientes") else "generico"
    table_ref = f"{GCP_PROJECT_ID}.{GCP_DATASET}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        if (body.table_type == "pedidos" and body.replace_pedidos)
        else bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = client.load_table_from_uri(body.gcs_uri, table_ref, job_config=job_config)
    try:
        load_job.result(timeout=600)
        return {"status": "ok", "rows": load_job.output_rows or 0, "table": table_id}
    except Exception as e:
        return {"status": "error", "message": str(e), "table": table_id}


@app.post("/gcp/setup-enriquecida")
def gcp_setup_enriquecida():
    """
    Cria a view pedidos_enriquecida no BigQuery (join pedidos + produtos + clientes).
    """
    if not is_gcp_configured():
        raise HTTPException(503, "GCP não configurado.")
    client = get_bigquery_client()
    view_sql = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{GCP_DATASET}.pedidos_enriquecida` AS
    SELECT
      p.cod_pedido, p.id_cliente, p.single_id, p.sku,
      p.valor_item_pedido, p.valor_item_pedido_frete, p.qtd_itens,
      p.canal_venda, p.bandeira, p.data_pedido, p.valor_frete, p.data_entrega,
      prod.nome_sku AS produto_nome_sku, prod.nome_marca AS produto_marca,
      prod.nome_setor_gerencial AS produto_setor, prod.nome_categoria_gerencial AS produto_categoria,
      c.nome AS cliente_nome, c.email AS cliente_email, c.cpf AS cliente_cpf, c.telefone AS cliente_telefone
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET}.pedidos` p
    LEFT JOIN `{GCP_PROJECT_ID}.{GCP_DATASET}.produtos` prod ON p.sku = prod.sku
    LEFT JOIN `{GCP_PROJECT_ID}.{GCP_DATASET}.clientes` c ON p.single_id = c.single_id
    """
    try:
        client.query(view_sql).result()
        return {"status": "ok", "message": "View pedidos_enriquecida criada/atualizada no BigQuery."}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ──────────────────────────────────────────────────────────────
# ENDPOINTS
# ──────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────
# ENDPOINT ZIP — recebe um .zip e processa arquivo por arquivo
# sem carregar tudo na RAM de uma vez
# ──────────────────────────────────────────────────────────────
@app.post("/upload-zip")
async def upload_zip(
    file: UploadFile = File(...),
    replace_pedidos: bool = Form(False),
):
    """
    Recebe um único arquivo .zip contendo vários XLSX/CSV.
    Extrai e processa cada arquivo individualmente para economizar RAM.
    Ideal para lotes grandes (ex.: 32 arquivos × 22 MB = 660 MB).
    """
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(400, "Envie um arquivo .zip.")

    raw_zip = await file.read()

    # Verificar se é zip válido
    if not zipfile.is_zipfile(io.BytesIO(raw_zip)):
        raise HTTPException(400, "Arquivo não é um ZIP válido.")

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

    results = []
    with zipfile.ZipFile(io.BytesIO(raw_zip)) as zf:
        names = [
            n for n in zf.namelist()
            if not n.startswith("__MACOSX")        # ignora metadata do Mac
            and not os.path.basename(n).startswith(".")  # ignora arquivos ocultos
            and n.lower().endswith((".xlsx", ".xls", ".csv"))
        ]

        if not names:
            raise HTTPException(400, "Nenhum arquivo XLSX/CSV encontrado dentro do ZIP.")

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as pool:
            tasks = []
            for name in names:
                content_bytes = zf.read(name)
                # Usa apenas o nome do arquivo, sem pastas internas
                filename = os.path.basename(name)
                tasks.append(
                    loop.run_in_executor(pool, process_file, filename, content_bytes)
                )
            results = list(await asyncio.gather(*tasks))

    ok    = [r for r in results if r["status"] == "ok"]
    error = [r for r in results if r["status"] != "ok"]
    return {
        "total":   len(results),
        "success": len(ok),
        "failed":  len(error),
        "results": results,
    }

@app.get("/health")
@app.get("/api/health")
def health():
    if is_gcp_configured():
        return {"status": "ok", "mode": "gcp"}
    try:
        conn = get_conn()
        conn.close()
        db_ok = True
    except Exception as e:
        db_ok = str(e)
    return {"status": "ok", "db_connected": db_ok, "mode": "postgres"}


from fastapi.responses import HTMLResponse

# HTML embutido diretamente — funciona em qualquer ambiente (Vercel, Railway, local)
_HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Supabase Bulk Uploader</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg:     #0d0d12;
      --panel:  #13131a;
      --border: #1e2030;
      --accent: #3ecf8e;
      --red:    #f87171;
      --muted:  #52526e;
      --text:   #e0e0f0;
    }
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: var(--bg); font-family: 'IBM Plex Sans', sans-serif; color: var(--text); min-height: 100vh; }
    .mono { font-family: 'IBM Plex Mono', monospace; }
    body::before {
      content: ''; position: fixed; inset: 0; pointer-events: none; z-index: 0;
      background-image: linear-gradient(rgba(62,207,142,.03) 1px, transparent 1px), linear-gradient(90deg, rgba(62,207,142,.03) 1px, transparent 1px);
      background-size: 40px 40px;
    }
    .z1 { position: relative; z-index: 1; }

    /* DROP ZONE */
    #drop-zone {
      border: 2px dashed var(--border); border-radius: 16px;
      transition: border-color .2s, background .2s; cursor: pointer;
    }
    #drop-zone.over, #gcp-drop-zone.over { border-color: var(--accent); background: rgba(62,207,142,.04); }

    /* PROGRESS */
    .bar-wrap { height: 4px; background: var(--border); border-radius: 99px; overflow: hidden; }
    .bar-fill  { height: 100%; background: var(--accent); border-radius: 99px; transition: width .3s ease; }

    /* RESULT ROW */
    .res-row {
      display: grid; grid-template-columns: 1fr auto auto auto;
      gap: 8px; align-items: center; padding: 8px 12px; border-radius: 8px;
      background: var(--panel); border: 1px solid var(--border); font-size: 13px;
      animation: up .2s ease;
    }
    .badge { padding: 2px 8px; border-radius: 99px; font-size: 11px; font-weight: 600; }
    .b-ok  { background: rgba(62,207,142,.12); color: var(--accent); }
    .b-err { background: rgba(248,113,113,.12); color: var(--red); }
    .type-badge { font-size: 10px; font-weight: 600; padding: 1px 7px; border-radius: 99px; flex-shrink: 0; }
    .type-pedidos  { background: rgba(59,130,246,.15); color: #93c5fd; }
    .type-produtos { background: rgba(251,191,36,.12);  color: #fbbf24; }
    .type-clientes { background: rgba(167,139,250,.12); color: #c4b5fd; }
    .type-generico { background: rgba(107,114,128,.12); color: #9ca3af; }
    .stat { padding: 18px; border-radius: 12px; text-align: center; border: 1px solid var(--border); background: var(--panel); }
    .scroll { max-height: 320px; overflow-y: auto; scrollbar-width: thin; scrollbar-color: var(--border) transparent; }
    .scroll::-webkit-scrollbar { width: 4px; }
    .scroll::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
    @keyframes up { from { opacity:0; transform:translateY(5px); } to { opacity:1; transform:translateY(0); } }
    .btn {
      background: var(--accent); color: #041f12; font-weight: 600; border-radius: 10px;
      padding: 12px 24px; font-size: 14px; border: none; cursor: pointer;
      transition: opacity .15s, transform .1s;
    }
    .btn:hover:not(:disabled) { opacity: .88; transform: translateY(-1px); }
    .btn:disabled { opacity: .35; cursor: not-allowed; transform: none; }
    .pulse { animation: pulse 1.1s infinite; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
  </style>
</head>
<body class="flex flex-col items-center py-14 px-4">

  <!-- HEADER -->
  <div class="z1 w-full max-w-2xl mb-10">
    <div class="flex items-center gap-3 mb-2">
      <svg viewBox="0 0 24 24" class="w-7 h-7" fill="none" stroke="#3ecf8e" stroke-width="1.8">
        <path d="M4 7h16M4 12h10M4 17h7"/>
        <circle cx="19" cy="17" r="3"/>
        <path d="m21 19-1.5-1.5"/>
      </svg>
      <h1 class="text-xl font-semibold tracking-tight">Supabase Bulk Uploader</h1>
      <span class="mono text-xs px-2 py-0.5 rounded border" style="border-color:#1d6648;color:#3ecf8e;background:rgba(62,207,142,.07)">v3.0</span>
    </div>
    <p class="text-sm" style="color:var(--muted)" id="header-desc">
      Carregando…
    </p>
  </div>

  <!-- MODO POSTGRES: ZIP -->
  <div id="ui-postgres" class="z1 w-full max-w-2xl" style="display:none">
  <!-- DROP ZONE ZIP -->
  <div id="drop-zone" class="z1 w-full max-w-2xl py-16 flex flex-col items-center gap-4 mb-6"
       onclick="document.getElementById('zip-input').click()">
    <input type="file" id="zip-input" accept=".zip" class="hidden" onchange="handleZipSelect(this)" />
    <svg class="w-14 h-14" viewBox="0 0 24 24" fill="none" stroke-width="1.2" style="color:var(--muted)" stroke="currentColor">
      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
      <polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/>
    </svg>
    <div class="text-center">
      <p class="font-medium mb-1">Arraste o arquivo <span style="color:var(--accent)">.zip</span> aqui</p>
      <p class="text-sm" style="color:var(--muted)">ou clique para selecionar</p>
    </div>
    <div id="zip-info" class="hidden flex items-center gap-3 px-4 py-2 rounded-lg border" style="border-color:var(--accent);background:rgba(62,207,142,.06)">
      <svg class="w-5 h-5 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="#3ecf8e" stroke-width="2">
        <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/>
        <polyline points="13 2 13 9 20 9"/>
      </svg>
      <span id="zip-name" class="text-sm font-medium" style="color:var(--accent)"></span>
      <span id="zip-size" class="mono text-xs" style="color:var(--muted)"></span>
      <button onclick="clearZip(event)" class="ml-2 text-xs" style="color:var(--muted)" onmouseover="this.style.color='var(--red)'" onmouseout="this.style.color='var(--muted)'">✕</button>
    </div>
  </div>

  <!-- OPÇÃO: TROCA DIÁRIA -->
  <div class="z1 w-full max-w-2xl mb-5 flex items-center gap-2">
    <input type="checkbox" id="replace-pedidos" style="accent-color:var(--accent);width:16px;height:16px" />
    <label for="replace-pedidos" class="text-sm" style="color:var(--text)">
      Substituir pedidos <span style="color:var(--muted)">(troca diária — apaga a tabela antes de inserir)</span>
    </label>
  </div>

  <!-- BOTÃO ENVIAR -->
  <div class="z1 w-full max-w-2xl mb-8">
    <button id="send-btn" class="btn w-full" disabled onclick="startZipUpload()">
      ⚡ Enviar para Supabase
    </button>
  </div>

  <!-- PROGRESSO -->
  <div id="prog-sec" class="z1 w-full max-w-2xl mb-6 hidden">
    <div class="flex justify-between text-xs mb-2" style="color:var(--muted)">
      <span id="prog-label" class="pulse">Enviando…</span>
      <span id="prog-pct" class="mono">0%</span>
    </div>
    <div class="bar-wrap"><div id="prog-fill" class="bar-fill" style="width:0%"></div></div>
  </div>

  <!-- SUMÁRIO -->
  <div id="summary" class="z1 w-full max-w-2xl mb-5 hidden">
    <div class="grid grid-cols-3 gap-3">
      <div class="stat"><p class="mono text-2xl font-semibold" id="s-total">0</p><p class="text-xs mt-1" style="color:var(--muted)">Arquivos</p></div>
      <div class="stat" style="border-color:rgba(62,207,142,.3)"><p class="mono text-2xl font-semibold" id="s-ok" style="color:var(--accent)">0</p><p class="text-xs mt-1" style="color:var(--muted)">Sucesso</p></div>
      <div class="stat" style="border-color:rgba(248,113,113,.3)"><p class="mono text-2xl font-semibold text-red-400" id="s-err">0</p><p class="text-xs mt-1" style="color:var(--muted)">Falhas</p></div>
    </div>
  </div>

  <!-- RESULTADOS -->
  <div id="results-sec" class="z1 w-full max-w-2xl mb-8 hidden">
    <p class="mono text-xs uppercase tracking-widest mb-3" style="color:var(--muted)">Detalhe por arquivo</p>
    <div id="results-list" class="scroll flex flex-col gap-2"></div>
  </div>

  <!-- BASE ENRIQUECIDA -->
  <div class="z1 w-full max-w-2xl p-5 rounded-xl border" style="background:var(--panel);border-color:var(--border)">
    <p class="mono text-xs uppercase tracking-widest mb-1" style="color:var(--muted)">Base enriquecida</p>
    <p class="text-sm mb-4" style="color:var(--text)">
      Após subir as 3 bases, crie a view que cruza
      <span style="color:#93c5fd">Pedidos</span> +
      <span style="color:#fbbf24">Produtos</span> +
      <span style="color:#c4b5fd">Clientes</span>.
    </p>
    <div class="flex items-center gap-3">
      <button onclick="setupEnriquecida()"
        class="px-4 py-2 rounded-lg text-sm font-medium border transition"
        style="border-color:var(--accent);color:var(--accent);background:rgba(62,207,142,.08)"
        onmouseover="this.style.background='rgba(62,207,142,.18)'" onmouseout="this.style.background='rgba(62,207,142,.08)'">
        Criar view pedidos_enriquecida
      </button>
      <span id="enriquecida-msg" class="text-sm"></span>
    </div>
  </div>
  </div>
  <!-- FIM MODO POSTGRES -->

  <!-- MODO GCP: Storage + BigQuery (arquivos direto para GCS, sem passar pelo Railway) -->
  <div id="ui-gcp" class="z1 w-full max-w-2xl" style="display:none">
    <p class="text-sm mb-4" style="color:var(--muted)">
      Arquivos vão <strong style="color:var(--text)">direto para o GCP Storage</strong> e depois são carregados no <strong style="color:var(--text)">BigQuery</strong>. Use <strong>.csv</strong> (BigQuery não carrega XLSX direto). Até 40 arquivos.
    </p>
    <div id="gcp-drop-zone" class="w-full py-12 flex flex-col items-center gap-4 mb-6 rounded-2xl border-2 border-dashed cursor-pointer transition"
         style="border-color:var(--border);" onclick="document.getElementById('gcp-file-input').click()">
      <input type="file" id="gcp-file-input" accept=".csv" multiple class="hidden" />
      <p class="font-medium">Arraste CSVs aqui ou clique</p>
      <p id="gcp-file-count" class="text-sm" style="color:var(--muted)">0 arquivos</p>
    </div>
    <div class="flex items-center gap-2 mb-5">
      <input type="checkbox" id="gcp-replace-pedidos" style="accent-color:var(--accent);width:16px;height:16px" />
      <label for="gcp-replace-pedidos" class="text-sm">Substituir pedidos (troca diária)</label>
    </div>
    <button id="gcp-send-btn" class="btn w-full mb-6" disabled onclick="startGcpUpload()">Enviar para BigQuery</button>
    <div id="gcp-prog-sec" class="mb-6 hidden">
      <div class="flex justify-between text-xs mb-2" style="color:var(--muted)">
        <span id="gcp-prog-label">Enviando…</span>
        <span id="gcp-prog-pct" class="mono">0%</span>
      </div>
      <div class="bar-wrap"><div id="gcp-prog-fill" class="bar-fill" style="width:0%"></div></div>
    </div>
    <div id="gcp-summary" class="mb-5 hidden">
      <div class="grid grid-cols-3 gap-3">
        <div class="stat"><p class="mono text-2xl font-semibold" id="gcp-s-total">0</p><p class="text-xs mt-1" style="color:var(--muted)">Arquivos</p></div>
        <div class="stat" style="border-color:rgba(62,207,142,.3)"><p class="mono text-2xl font-semibold" id="gcp-s-ok" style="color:var(--accent)">0</p><p class="text-xs mt-1" style="color:var(--muted)">Sucesso</p></div>
        <div class="stat" style="border-color:rgba(248,113,113,.3)"><p class="mono text-2xl font-semibold text-red-400" id="gcp-s-err">0</p><p class="text-xs mt-1" style="color:var(--muted)">Falhas</p></div>
      </div>
    </div>
    <div class="z1 w-full max-w-2xl p-5 rounded-xl border mb-6" style="background:var(--panel);border-color:var(--border)">
      <p class="mono text-xs uppercase tracking-widest mb-1" style="color:var(--muted)">Base enriquecida (BigQuery)</p>
      <p class="text-sm mb-4" style="color:var(--text)">Após subir as 3 bases, crie a view.</p>
      <button onclick="setupGcpEnriquecida()" class="px-4 py-2 rounded-lg text-sm font-medium border transition" style="border-color:var(--accent);color:var(--accent);background:rgba(62,207,142,.08)">Criar view pedidos_enriquecida</button>
      <span id="gcp-enriquecida-msg" class="text-sm ml-2"></span>
    </div>
  </div>

<script>
const TABLE_COLORS = { pedidos:'type-pedidos', produtos:'type-produtos', clientes:'type-clientes', generico:'type-generico' };
let selectedZip = null;
let gcpFiles = [];
let APP_MODE = 'postgres';

// ── CONFIG: mostra UI Postgres (ZIP) ou GCP (Storage + BigQuery) ──
fetch(window.location.origin + '/config').then(r=>r.json()).then(c=>{
  APP_MODE = c.mode;
  const desc = document.getElementById('header-desc');
  if (c.mode === 'gcp') {
    desc.innerHTML = 'Modo <strong style="color:var(--text)">GCP Storage + BigQuery</strong>: envie CSVs (até 40). O arquivo vai direto para o Storage, sem passar pelo servidor.';
    document.getElementById('ui-gcp').style.display = 'block';
    initGcpUi();
  } else {
    desc.innerHTML = 'Envie um <strong style="color:var(--text)">.zip</strong> com todos os arquivos XLSX/CSV — detecção automática (pedidos / produtos / clientes) e insert direto no Supabase.';
    document.getElementById('ui-postgres').style.display = 'block';
  }
}).catch(()=>{
  document.getElementById('header-desc').textContent = 'Envie um .zip com XLSX/CSV para Supabase.';
  document.getElementById('ui-postgres').style.display = 'block';
});

function guessType(name) {
  const n = name.toLowerCase();
  if (/pedido|order/i.test(n)) return 'pedidos';
  if (/sku|produto|product/i.test(n)) return 'produtos';
  if (/cliente|customer/i.test(n)) return 'clientes';
  return 'generico';
}

function initGcpUi() {
  const dz = document.getElementById('gcp-drop-zone');
  const input = document.getElementById('gcp-file-input');
  dz.addEventListener('dragover', e=>{ e.preventDefault(); dz.classList.add('over'); });
  dz.addEventListener('dragleave', ()=> dz.classList.remove('over'));
  dz.addEventListener('drop', e=>{
    e.preventDefault(); dz.classList.remove('over');
    addGcpFiles([...e.dataTransfer.files].filter(f=>f.name.toLowerCase().endsWith('.csv')));
  });
  input.addEventListener('change', e=>{ addGcpFiles([...e.target.files]); e.target.value=''; });
}

function addGcpFiles(files) {
  for (const f of files) {
    if (gcpFiles.length >= 40) break;
    if (!gcpFiles.some(x=>x.name===f.name)) gcpFiles.push(f);
  }
  document.getElementById('gcp-file-count').textContent = gcpFiles.length + ' arquivo(s)';
  document.getElementById('gcp-send-btn').disabled = gcpFiles.length === 0;
}

async function startGcpUpload() {
  if (gcpFiles.length === 0) return;
  const btn = document.getElementById('gcp-send-btn');
  const ps = document.getElementById('gcp-prog-sec');
  const label = document.getElementById('gcp-prog-label');
  const fill = document.getElementById('gcp-prog-fill');
  const pct = document.getElementById('gcp-prog-pct');
  btn.disabled = true;
  ps.classList.remove('hidden');
  document.getElementById('gcp-summary').classList.add('hidden');
  const replacePedidos = document.getElementById('gcp-replace-pedidos').checked;
  const results = [];
  const total = gcpFiles.length;
  for (let i = 0; i < gcpFiles.length; i++) {
    const file = gcpFiles[i];
    const tableType = guessType(file.name);
    fill.style.width = (i/total*80) + '%'; pct.textContent = Math.round(i/total*80) + '%';
    label.textContent = 'Enviando ' + file.name + ' para GCS…';
    try {
      const urlRes = await fetch(window.location.origin + '/gcp/upload-url', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename: file.name, table_type: tableType, content_type: file.type || 'text/csv' })
      });
      if (!urlRes.ok) throw new Error(await urlRes.text());
      const { upload_url, gcs_uri } = await urlRes.json();
      const putRes = await fetch(upload_url, { method: 'PUT', body: file, headers: { 'Content-Type': file.type || 'text/csv' } });
      if (!putRes.ok) throw new Error('GCS PUT ' + putRes.status);
      label.textContent = 'Carregando no BigQuery: ' + file.name + '…';
      const loadRes = await fetch(window.location.origin + '/gcp/load', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ gcs_uri, table_type: tableType, replace_pedidos: replacePedidos && tableType === 'pedidos' && i === 0 })
      });
      const loadData = await loadRes.json();
      results.push({ file: file.name, status: loadData.status === 'ok' ? 'ok' : 'error', table: tableType, rows: loadData.rows || 0, error: loadData.message });
    } catch (err) {
      results.push({ file: file.name, status: 'error', table: guessType(file.name), error: err.message });
    }
  }
  fill.style.width = '100%'; pct.textContent = '100%';
  label.textContent = 'Concluído.';
  const ok = results.filter(r=>r.status==='ok').length;
  document.getElementById('gcp-s-total').textContent = results.length;
  document.getElementById('gcp-s-ok').textContent = ok;
  document.getElementById('gcp-s-err').textContent = results.length - ok;
  document.getElementById('gcp-summary').classList.remove('hidden');
  btn.disabled = false;
}

async function setupGcpEnriquecida() {
  const el = document.getElementById('gcp-enriquecida-msg');
  el.textContent = '…'; el.style.color = 'var(--muted)';
  try {
    const res = await fetch(window.location.origin + '/gcp/setup-enriquecida', { method: 'POST' });
    const data = await res.json();
    el.textContent = data.status === 'ok' ? 'View criada.' : data.message;
    el.style.color = data.status === 'ok' ? 'var(--accent)' : 'var(--red)';
  } catch (err) {
    el.textContent = err.message;
    el.style.color = 'var(--red)';
  }
}

// ── DRAG & DROP (Postgres / ZIP) ──────────────────────────────────────────────
const zone = document.getElementById('drop-zone');
zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('over'); });
zone.addEventListener('dragleave', () => zone.classList.remove('over'));
zone.addEventListener('drop', e => {
  e.preventDefault(); zone.classList.remove('over');
  const f = [...e.dataTransfer.files].find(f => f.name.toLowerCase().endsWith('.zip'));
  if (f) setZip(f); else alert('Envie um arquivo .zip');
});

function handleZipSelect(input) {
  if (input.files[0]) setZip(input.files[0]);
  input.value = '';
}

function setZip(f) {
  selectedZip = f;
  document.getElementById('zip-name').textContent = f.name;
  document.getElementById('zip-size').textContent = (f.size / 1024 / 1024).toFixed(1) + ' MB';
  document.getElementById('zip-info').classList.remove('hidden');
  document.getElementById('send-btn').disabled = false;
}

function clearZip(e) {
  e.stopPropagation();
  selectedZip = null;
  document.getElementById('zip-info').classList.add('hidden');
  document.getElementById('send-btn').disabled = true;
  ['summary','results-sec','prog-sec'].forEach(id => document.getElementById(id).classList.add('hidden'));
}

// ── UPLOAD ZIP ──────────────────────────────────────────────
async function startZipUpload() {
  if (!selectedZip) return;

  const btn   = document.getElementById('send-btn');
  const ps    = document.getElementById('prog-sec');
  const label = document.getElementById('prog-label');
  const fill  = document.getElementById('prog-fill');
  const pct   = document.getElementById('prog-pct');

  btn.disabled = true;
  ps.classList.remove('hidden');
  ['summary','results-sec'].forEach(id => document.getElementById(id).classList.add('hidden'));
  label.classList.add('pulse');
  label.style.color = '';
  fill.style.width = '2%'; pct.textContent = '2%';

  const form = new FormData();
  form.append('file', selectedZip);
  form.append('replace_pedidos', document.getElementById('replace-pedidos').checked ? 'true' : 'false');

  try {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', window.location.origin + '/upload-zip');

    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable) {
        const p = Math.round((e.loaded / e.total) * 70);
        fill.style.width = (2 + p) + '%';
        pct.textContent  = (2 + p) + '%';
        label.textContent = 'Enviando… ' + (e.loaded/1024/1024).toFixed(1) + ' / ' + (e.total/1024/1024).toFixed(1) + ' MB';
      }
    };

    const data = await new Promise((resolve, reject) => {
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) resolve(JSON.parse(xhr.responseText));
        else reject(new Error('HTTP ' + xhr.status + ': ' + xhr.responseText));
      };
      xhr.onerror = () => reject(new Error('Erro de rede'));
      xhr.send(form);
    });

    fill.style.width = '100%'; pct.textContent = '100%';
    label.textContent = '✓ Concluído — ' + data.success + ' de ' + data.total + ' arquivos processados';
    label.classList.remove('pulse');
    label.style.color = 'var(--accent)';
    showResults(data);

  } catch (err) {
    fill.style.width = '100%';
    label.textContent = '✗ ' + err.message;
    label.classList.remove('pulse');
    label.style.color = 'var(--red)';
  }

  btn.disabled = false;
}

function showResults(data) {
  document.getElementById('s-total').textContent = data.total;
  document.getElementById('s-ok').textContent    = data.success;
  document.getElementById('s-err').textContent   = data.failed;
  document.getElementById('summary').classList.remove('hidden');

  const list = document.getElementById('results-list');
  list.innerHTML = '';

  for (const r of data.results) {
    const tc    = TABLE_COLORS[r.table] || 'type-generico';
    const badge = r.status === 'ok' ? '<span class="badge b-ok">OK</span>' : '<span class="badge b-err">ERRO</span>';
    const tbl   = r.table ? `<span class="type-badge ${tc}">${r.table}</span>` : '';
    const rows  = r.rows  ? `<span class="mono text-xs" style="color:var(--muted)">${r.rows.toLocaleString('pt-BR')} linhas</span>` : '';
    const t     = `<span class="mono text-xs" style="color:var(--muted)">${r.elapsed_s}s</span>`;

    if (r.error) {
      const wrap = document.createElement('div');
      wrap.innerHTML = `<div class="res-row"><span class="truncate text-sm">${r.file}</span>${tbl}${t}${badge}</div>`;
      const pre = document.createElement('pre');
      pre.style.cssText = 'margin-top:4px;font-size:11px;color:var(--red);white-space:pre-wrap;padding:8px;background:#1a0808;border-radius:6px';
      pre.textContent = r.error;
      wrap.appendChild(pre);
      list.appendChild(wrap);
    } else {
      const row = document.createElement('div');
      row.className = 'res-row';
      row.innerHTML = `<span class="truncate text-sm">${r.file}</span>${tbl}${rows}${t}${badge}`;
      list.appendChild(row);
    }
  }
  document.getElementById('results-sec').classList.remove('hidden');
}

async function setupEnriquecida() {
  const el = document.getElementById('enriquecida-msg');
  el.textContent = '…'; el.style.color = 'var(--muted)';
  try {
    const res  = await fetch(window.location.origin + '/setup-enriquecida', { method: 'POST' });
    const data = await res.json();
    el.textContent = data.status === 'ok' ? '✓ View criada/atualizada' : '✗ ' + data.message;
    el.style.color = data.status === 'ok' ? 'var(--accent)' : 'var(--red)';
  } catch (err) {
    el.textContent = '✗ ' + err.message;
    el.style.color = 'var(--red)';
  }
}
</script>
</body>
</html>"""

@app.get("/", response_class=HTMLResponse)
def frontend():
    return HTMLResponse(content=_HTML)