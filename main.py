"""
GCP Bulk Uploader
Recebe ZIP via Cloud Storage, processa XLSX/CSV em chunks e carrega no BigQuery.
Detecta automaticamente o tipo de arquivo (pedidos / produtos / clientes).
"""

from dotenv import load_dotenv
load_dotenv()

import asyncio
import gzip
import io
import json
import os
import re
import tempfile
import time
import traceback
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

# ── GCP ──────────────────────────────────────────────────────
from google.cloud import bigquery, storage
from google.oauth2 import service_account

app = FastAPI(title="GCP Bulk Uploader", version="5.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ──────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ──────────────────────────────────────────────────────────────
# GCP_SERVICE_ACCOUNT_JSON → conteúdo do JSON da service account (variável no Railway)
# GCP_PROJECT_ID           → ID do projeto GCP
# GCP_BUCKET               → nome do bucket Cloud Storage
# GCP_DATASET              → nome do dataset BigQuery (ex: etl_dados)

def _get_gcp_credentials():
    """Carrega credenciais da variável de ambiente GCP_SERVICE_ACCOUNT_JSON."""
    raw = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "")
    if not raw:
        raise RuntimeError(
            "GCP_SERVICE_ACCOUNT_JSON não configurada! "
            "Railway → Variables → adicione o conteúdo do JSON da service account."
        )
    info = json.loads(raw)
    return service_account.Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/bigquery",
        ],
    )

CONFIG = {
    "project_id": os.getenv("GCP_PROJECT_ID", ""),
    "bucket":     os.getenv("GCP_BUCKET", ""),
    "dataset":    os.getenv("GCP_DATASET", "etl_dados"),
    "chunk_size": int(os.getenv("CHUNK_SIZE", "1000")),
}

ROWS_PER_BATCH = int(os.getenv("ROWS_PER_BATCH", "50000"))

CSV_ENCODINGS = ("utf-8", "utf-8-sig", "cp1252", "latin-1", "iso-8859-1")
CSV_SEPARATORS = (",", ";", "\t", "|")

# ──────────────────────────────────────────────────────────────
# SCHEMAS — mapeamento para tipos BigQuery
# ──────────────────────────────────────────────────────────────
# BigQuery types: STRING, INT64, FLOAT64, NUMERIC, BOOL, DATE, TIMESTAMP
BQ_TYPE_MAP = {
    "TEXT":        "STRING",
    "BIGINT":      "INT64",
    "INTEGER":     "INT64",
    "NUMERIC":     "NUMERIC",
    "BOOLEAN":     "BOOL",
    "DATE":        "DATE",
    "TIMESTAMPTZ": "TIMESTAMP",
}

SCHEMAS = {
    "pedidos": {
        "columns": [
            ("cod_pedido",                    "INT64"),
            ("id_cliente",                    "INT64"),
            ("single_id",                     "STRING"),
            ("sku",                           "INT64"),
            ("valor_item_pedido",             "NUMERIC"),
            ("valor_item_pedido_frete",       "NUMERIC"),
            ("qtd_itens",                     "INT64"),
            ("canal_venda",                   "STRING"),
            ("bandeira",                      "STRING"),
            ("data_pedido",                   "DATE"),
            ("cod_tip_frete",                 "INT64"),
            ("descricao_tipo_frete",          "STRING"),
            ("data_atualizacao",              "DATE"),
            ("valor_frete",                   "NUMERIC"),
            ("flag_retira",                   "BOOL"),
            ("data_entrega",                  "STRING"),
            ("flag_marketplace",              "BOOL"),
            ("flag_lista_casamento",          "BOOL"),
            ("id_lista_casamento",            "INT64"),
            ("cod_motivo_cancelamento",       "INT64"),
            ("descricao_motivo_cancelamento", "STRING"),
            ("_source_file",                  "STRING"),
            ("_loaded_at",                    "TIMESTAMP"),
        ],
        "rename": {},
    },
    "produtos": {
        "columns": [
            ("sku",                                 "INT64"),
            ("nome_sku",                            "STRING"),
            ("nome_agrupamento_diretoria_setor",    "STRING"),
            ("nome_setor_gerencial",                "STRING"),
            ("nome_classe_gerencial",               "STRING"),
            ("nome_especie_gerencial",              "STRING"),
            ("nome_categoria_gerencial",            "STRING"),
            ("nome_categoria_pai_gerencial",        "STRING"),
            ("nome_departamento_pai_gerencial",     "STRING"),
            ("nome_setor_origem",                   "STRING"),
            ("nome_classe_origem",                  "STRING"),
            ("nome_especie_origem",                 "STRING"),
            ("nome_sub_especie",                    "STRING"),
            ("nome_categoria_origem",               "STRING"),
            ("nome_categoria_pai_origem",           "STRING"),
            ("nome_departamento_pai_origem",        "STRING"),
            ("nome_tipo_sku",                       "STRING"),
            ("nome_marca",                          "STRING"),
            ("nome_diretoria_setor_alternativa",    "STRING"),
            ("nome_setor_alternativo",              "STRING"),
            ("url_produto",                         "STRING"),
            ("_source_file",                        "STRING"),
            ("_loaded_at",                          "TIMESTAMP"),
        ],
        "rename": {},
    },
    "clientes": {
        "columns": [
            ("id_cliente",    "INT64"),
            ("single_id",     "STRING"),
            ("nome",          "STRING"),
            ("email",         "STRING"),
            ("cpf",           "STRING"),
            ("telefone",      "STRING"),
            ("data_cadastro", "DATE"),
            ("_source_file",  "STRING"),
            ("_loaded_at",    "TIMESTAMP"),
        ],
        "rename": {},
    },
}

FINGERPRINTS = {
    "pedidos":  {"cod_pedido", "id_cliente", "sku", "canal_venda", "bandeira"},
    "produtos": {"nome_sku", "nome_setor_gerencial", "nome_marca", "url_produto"},
    "clientes": {"id_cliente", "single_id"},
}

# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
def sanitize_col(name: str) -> str:
    name = str(name).strip()
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if name and name[0].isdigit():
        name = "col_" + name
    return name.lower()

def detect_table(df_columns: list[str]) -> str:
    cols = {sanitize_col(c) for c in df_columns}
    scores = {t: len(fp & cols) / len(fp) for t, fp in FINGERPRINTS.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] >= 0.4 else "generico"

def _clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[:, ~df.columns.str.match(r"^Unnamed:\s*\d+$", na=False)]
    return df.replace({"": None, "nan": None, "NaN": None, "None": None, "NULL": None})

def align_df(df: pd.DataFrame, table: str, source_file: str) -> pd.DataFrame:
    df.columns = [sanitize_col(c) for c in df.columns]
    df["_source_file"] = source_file
    df["_loaded_at"]   = datetime.now(timezone.utc).isoformat()

    schema = SCHEMAS.get(table)
    if not schema:
        return df.where(pd.notnull(df), None)

    rename_map = {k.lower(): v for k, v in schema.get("rename", {}).items()}
    df = df.rename(columns=rename_map)

    canonical_cols = [col for col, _ in schema["columns"]]
    for col in canonical_cols:
        if col not in df.columns:
            df[col] = None
    df = df[canonical_cols]
    return df.where(pd.notnull(df), None)

# ──────────────────────────────────────────────────────────────
# BIGQUERY — criar tabela + inserir em lotes
# ──────────────────────────────────────────────────────────────
def get_bq_client():
    creds = _get_gcp_credentials()
    return bigquery.Client(project=CONFIG["project_id"], credentials=creds)

def ensure_bq_table(bq: bigquery.Client, table_id: str, schema_def: list[tuple]):
    """Cria a tabela no BigQuery se não existir."""
    full_id = f"{CONFIG['project_id']}.{CONFIG['dataset']}.{table_id}"
    schema  = [bigquery.SchemaField(col, typ) for col, typ in schema_def]
    table   = bigquery.Table(full_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="_loaded_at")
    bq.create_table(table, exists_ok=True)
    return full_id

def ensure_bq_dataset(bq: bigquery.Client):
    """Cria o dataset se não existir."""
    dataset_id = f"{CONFIG['project_id']}.{CONFIG['dataset']}"
    dataset    = bigquery.Dataset(dataset_id)
    dataset.location = os.getenv("GCP_LOCATION", "US")
    bq.create_dataset(dataset, exists_ok=True)

def bq_insert_batch(bq: bigquery.Client, full_table_id: str, df: pd.DataFrame):
    """Insere um DataFrame no BigQuery via streaming insert (rows_to_insert)."""
    chunk_size = CONFIG["chunk_size"]
    errors_all = []
    for i in range(0, len(df), chunk_size):
        chunk  = df.iloc[i: i + chunk_size]
        rows   = chunk.to_dict(orient="records")
        errors = bq.insert_rows_json(full_table_id, rows)
        if errors:
            errors_all.extend(errors)
    if errors_all:
        raise RuntimeError(f"BigQuery insert errors: {errors_all[:3]}")

# ──────────────────────────────────────────────────────────────
# STREAMING CSV — lê em chunks sem carregar tudo na RAM
# ──────────────────────────────────────────────────────────────
def _detect_csv_params(raw_content: bytes) -> tuple[str, str]:
    sample = raw_content[:65536]
    for enc in CSV_ENCODINGS:
        for sep in CSV_SEPARATORS:
            try:
                df = pd.read_csv(io.BytesIO(sample), sep=sep, encoding=enc,
                                 on_bad_lines="skip", dtype=str, keep_default_na=False, nrows=5)
                if len(df.columns) >= 2:
                    return enc, sep
            except Exception:
                continue
    raise ValueError("Não foi possível detectar encoding/separador do CSV.")

def stream_csv(raw_content: bytes, filename: str, bq_client: bigquery.Client) -> tuple[str, int]:
    enc, sep = _detect_csv_params(raw_content)
    reader   = pd.read_csv(io.BytesIO(raw_content), sep=sep, encoding=enc,
                           on_bad_lines="skip", dtype=str, keep_default_na=False,
                           chunksize=ROWS_PER_BATCH)
    table       = None
    total_rows  = 0
    full_tbl_id = None

    for batch in reader:
        batch = _clean_chunk(batch).dropna(how="all")
        if batch.empty:
            continue
        if table is None:
            table      = detect_table(list(batch.columns))
            schema_def = SCHEMAS.get(table, {}).get("columns") or \
                         [(sanitize_col(c), "STRING") for c in batch.columns]
            full_tbl_id = ensure_bq_table(bq_client, table, schema_def)

        batch = align_df(batch, table, filename)
        bq_insert_batch(bq_client, full_tbl_id, batch)
        total_rows += len(batch)
        del batch

    return table or "generico", total_rows

# ──────────────────────────────────────────────────────────────
# STREAMING XLSX — openpyxl read_only, linha por linha
# ──────────────────────────────────────────────────────────────
def stream_xlsx(raw_content: bytes, filename: str, bq_client: bigquery.Client) -> tuple[str, int]:
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(raw_content), read_only=True, data_only=True)
    ws = wb.active

    table       = None
    total_rows  = 0
    full_tbl_id = None
    headers     = None
    batch_rows  = []

    def flush(rows):
        nonlocal table, full_tbl_id, total_rows
        df = pd.DataFrame(rows, columns=headers, dtype=str)
        df = _clean_chunk(df).dropna(how="all")
        if df.empty:
            return

        if table is None:
            t = detect_table(list(df.columns))
            table       = t
            schema_def  = SCHEMAS.get(table, {}).get("columns") or \
                          [(sanitize_col(c), "STRING") for c in df.columns]
            full_tbl_id = ensure_bq_table(bq_client, table, schema_def)

        df = align_df(df, table, filename)
        bq_insert_batch(bq_client, full_tbl_id, df)
        total_rows += len(df)
        del df

    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            headers = [str(c) if c is not None else f"col_{j}" for j, c in enumerate(row)]
            continue
        batch_rows.append([str(v) if v is not None else None for v in row])
        if len(batch_rows) >= ROWS_PER_BATCH:
            flush(batch_rows)
            batch_rows = []

    if batch_rows:
        flush(batch_rows)

    wb.close()
    return table or "generico", total_rows

# ──────────────────────────────────────────────────────────────
# PROCESSAMENTO DE UM ARQUIVO
# ──────────────────────────────────────────────────────────────
def process_file(filename: str, content: bytes) -> dict:
    start  = time.time()
    result = {"file": filename, "status": "ok", "rows": 0, "table": "", "elapsed_s": 0, "error": None}
    try:
        real_filename = filename
        raw_content   = content
        if filename.lower().endswith(".gz"):
            raw_content   = gzip.decompress(content)
            real_filename = filename[:-3]
            result["file"] = real_filename

        bq = get_bq_client()
        ensure_bq_dataset(bq)

        if real_filename.lower().endswith(".csv"):
            table, rows = stream_csv(raw_content, real_filename, bq)
        else:
            table, rows = stream_xlsx(raw_content, real_filename, bq)

        result["table"] = table
        result["rows"]  = rows
        del raw_content

    except Exception:
        result["status"] = "error"
        result["error"]  = traceback.format_exc()

    result["elapsed_s"] = round(time.time() - start, 2)
    return result

# ──────────────────────────────────────────────────────────────
# CLOUD STORAGE — signed URL para upload direto do browser
# ──────────────────────────────────────────────────────────────
def get_gcs_client():
    creds = _get_gcp_credentials()
    return storage.Client(project=CONFIG["project_id"], credentials=creds)

# ──────────────────────────────────────────────────────────────
# ENDPOINTS
# ──────────────────────────────────────────────────────────────

@app.post("/storage-token")
async def storage_token(body: dict):
    """
    Gera signed URL para o browser fazer upload direto no GCS.
    Se unique=True (ou para arquivos soltos), usa path único: uploads/YYYY-MM-DD/{uuid}_{filename}.
    """
    filename = body.get("filename", "upload.zip")
    unique   = body.get("unique", False)
    safe     = re.sub(r"[^a-zA-Z0-9._-]", "_", filename)
    if unique:
        path = f"uploads/{datetime.utcnow().strftime('%Y-%m-%d')}/{uuid.uuid4().hex}_{safe}"
    else:
        path = f"uploads/{safe}"
    gcs   = get_gcs_client()
    bucket = gcs.bucket(CONFIG["bucket"])
    blob  = bucket.blob(path)
    signed_url = blob.generate_signed_url(
        version      = "v4",
        expiration   = 3600,
        method       = "PUT",
        content_type = body.get("content_type") or "application/octet-stream",
    )
    return {"upload_url": signed_url, "path": path}


@app.post("/process-storage")
async def process_storage(body: dict):
    """
    Baixa o ZIP do GCS em streaming para arquivo temporário,
    extrai e processa cada arquivo individualmente no BigQuery.
    """
    path            = body.get("path", "")
    replace_pedidos = body.get("replace_pedidos", False)
    if not path:
        raise HTTPException(400, "Campo 'path' obrigatório.")

    gcs    = get_gcs_client()
    bucket = gcs.bucket(CONFIG["bucket"])
    blob   = bucket.blob(path)

    # Truncar pedidos antes se solicitado
    if replace_pedidos:
        bq      = get_bq_client()
        full_id = f"{CONFIG['project_id']}.{CONFIG['dataset']}.pedidos"
        try:
            bq.delete_table(full_id)
        except Exception:
            pass  # tabela pode não existir ainda

    tmp_path = None
    results  = []
    try:
        # Download streaming para disco temporário
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = tmp.name
            blob.download_to_file(tmp)

        if not zipfile.is_zipfile(tmp_path):
            raise HTTPException(400, "Arquivo no Storage não é um ZIP válido.")

        with zipfile.ZipFile(tmp_path) as zf:
            names = [
                n for n in zf.namelist()
                if not n.startswith("__MACOSX")
                and not os.path.basename(n).startswith(".")
                and n.lower().endswith((".xlsx", ".xls", ".csv"))
            ]
            if not names:
                raise HTTPException(400, "Nenhum XLSX/CSV encontrado no ZIP.")

            for name in names:
                filename      = os.path.basename(name)
                content_bytes = zf.read(name)
                result        = process_file(filename, content_bytes)
                results.append(result)
                del content_bytes  # libera RAM

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

    ok    = [r for r in results if r["status"] == "ok"]
    error = [r for r in results if r["status"] != "ok"]
    return {"total": len(results), "success": len(ok), "failed": len(error), "results": results}


@app.post("/load-from-gcs")
async def load_from_gcs(body: dict):
    """
    Carrega um arquivo CSV já no GCS direto no BigQuery (load job).
    O Railway NÃO baixa o arquivo — evita timeout e 502 para arquivos grandes.
    """
    path            = body.get("path", "").strip()
    table_type      = (body.get("table_type") or "generico").strip().lower()
    replace_pedidos = body.get("replace_pedidos", False)
    if not path:
        raise HTTPException(400, "Campo 'path' obrigatório.")
    if table_type not in ("pedidos", "produtos", "clientes", "generico"):
        table_type = "generico"

    gcs_uri = f"gs://{CONFIG['bucket']}/{path}"
    bq      = get_bq_client()
    ensure_bq_dataset(bq)
    table_id = f"{CONFIG['project_id']}.{CONFIG['dataset']}.{table_type}"

    job_config = bigquery.LoadJobConfig(
        source_format       = bigquery.SourceFormat.CSV,
        autodetect          = True,
        skip_leading_rows   = 1,
        write_disposition   = bigquery.WriteDisposition.WRITE_TRUNCATE
        if (table_type == "pedidos" and replace_pedidos)
        else bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = bq.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    try:
        load_job.result(timeout=600)
        rows = load_job.output_rows or 0
        return {"status": "ok", "rows": rows, "table": table_type}
    except Exception as e:
        return {"status": "error", "message": str(e), "table": table_type}


@app.post("/setup-enriquecida")
async def setup_enriquecida():
    """Cria/atualiza view pedidos_enriquecida no BigQuery."""
    try:
        bq  = get_bq_client()
        ds  = CONFIG["dataset"]
        pid = CONFIG["project_id"]
        sql = f"""
        CREATE OR REPLACE VIEW `{pid}.{ds}.pedidos_enriquecida` AS
        SELECT
          p.* EXCEPT (_source_file, _loaded_at),
          p._source_file AS pedido_source_file,
          p._loaded_at   AS pedido_loaded_at,
          prod.nome_sku, prod.nome_marca, prod.nome_setor_gerencial,
          prod.nome_categoria_gerencial, prod.url_produto,
          c.nome AS cliente_nome, c.email AS cliente_email,
          c.cpf  AS cliente_cpf,  c.telefone AS cliente_telefone
        FROM `{pid}.{ds}.pedidos` p
        LEFT JOIN `{pid}.{ds}.produtos` prod ON CAST(p.sku AS INT64) = prod.sku
        LEFT JOIN `{pid}.{ds}.clientes` c    ON p.single_id = c.single_id
        """
        bq.query(sql).result()
        return {"status": "ok", "message": f"View {ds}.pedidos_enriquecida criada/atualizada."}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/health")
async def health():
    diag = {"status": "ok", "gcp_project": CONFIG["project_id"],
            "bucket": CONFIG["bucket"], "dataset": CONFIG["dataset"]}
    try:
        bq = get_bq_client()
        list(bq.list_datasets(max_results=1))
        diag["bq_connected"] = True
    except Exception as e:
        diag["bq_connected"] = False
        diag["bq_error"]     = str(e)
        diag["status"]       = "error"
    try:
        gcs = get_gcs_client()
        gcs.get_bucket(CONFIG["bucket"])
        diag["gcs_connected"] = True
    except Exception as e:
        diag["gcs_connected"] = False
        diag["gcs_error"]     = str(e)
        diag["status"]        = "error"
    return diag


_HTML = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>GCP Bulk Uploader</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
  <style>
    :root { --bg:#0d0d12; --panel:#13131a; --border:#1e2030; --accent:#3ecf8e; --red:#f87171; --muted:#52526e; --text:#e0e0f0; }
    *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
    body{background:var(--bg);font-family:'IBM Plex Sans',sans-serif;color:var(--text);min-height:100vh}
    .mono{font-family:'IBM Plex Mono',monospace}
    body::before{content:'';position:fixed;inset:0;pointer-events:none;z-index:0;
      background-image:linear-gradient(rgba(62,207,142,.03) 1px,transparent 1px),linear-gradient(90deg,rgba(62,207,142,.03) 1px,transparent 1px);
      background-size:40px 40px}
    .z1{position:relative;z-index:1}
    #drop-zone,#csv-drop-zone{border:2px dashed var(--border);border-radius:16px;transition:border-color .2s,background .2s;cursor:pointer}
    #drop-zone.over,#csv-drop-zone.over{border-color:var(--accent);background:rgba(62,207,142,.04)}
    .bar-wrap{height:4px;background:var(--border);border-radius:99px;overflow:hidden}
    .bar-fill{height:100%;background:var(--accent);border-radius:99px;transition:width .3s ease}
    .step-badge{display:inline-flex;align-items:center;justify-content:center;width:22px;height:22px;border-radius:50%;font-size:11px;font-weight:700;flex-shrink:0}
    .step-active{background:var(--accent);color:#041f12}
    .step-done{background:rgba(62,207,142,.2);color:var(--accent)}
    .step-idle{background:var(--border);color:var(--muted)}
    .res-row{display:grid;grid-template-columns:1fr auto auto auto;gap:8px;align-items:center;padding:8px 12px;border-radius:8px;background:var(--panel);border:1px solid var(--border);font-size:13px;animation:up .2s ease}
    .badge{padding:2px 8px;border-radius:99px;font-size:11px;font-weight:600}
    .b-ok{background:rgba(62,207,142,.12);color:var(--accent)}
    .b-err{background:rgba(248,113,113,.12);color:var(--red)}
    .type-badge{font-size:10px;font-weight:600;padding:1px 7px;border-radius:99px;flex-shrink:0}
    .type-pedidos{background:rgba(59,130,246,.15);color:#93c5fd}
    .type-produtos{background:rgba(251,191,36,.12);color:#fbbf24}
    .type-clientes{background:rgba(167,139,250,.12);color:#c4b5fd}
    .type-generico{background:rgba(107,114,128,.12);color:#9ca3af}
    .stat{padding:16px;border-radius:12px;text-align:center;border:1px solid var(--border);background:var(--panel)}
    .scroll{max-height:320px;overflow-y:auto;scrollbar-width:thin;scrollbar-color:var(--border) transparent}
    .scroll::-webkit-scrollbar{width:4px}
    .scroll::-webkit-scrollbar-thumb{background:var(--border);border-radius:4px}
    @keyframes up{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:translateY(0)}}
    .btn{background:var(--accent);color:#041f12;font-weight:600;border-radius:10px;padding:12px 24px;font-size:14px;border:none;cursor:pointer;transition:opacity .15s,transform .1s;width:100%}
    .btn:hover:not(:disabled){opacity:.88;transform:translateY(-1px)}
    .btn:disabled{opacity:.35;cursor:not-allowed;transform:none}
    .pulse{animation:pulse 1.1s infinite}
    @keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
  </style>
</head>
<body class="flex flex-col items-center py-14 px-4">

  <!-- HEADER -->
  <div class="z1 w-full max-w-2xl mb-8">
    <div class="flex items-center gap-3 mb-2">
      <svg viewBox="0 0 24 24" class="w-7 h-7" fill="none" stroke="#3ecf8e" stroke-width="1.8">
        <path d="M4 7h16M4 12h10M4 17h7"/>
        <circle cx="19" cy="17" r="3"/><path d="m21 19-1.5-1.5"/>
      </svg>
      <h1 class="text-xl font-semibold tracking-tight">GCP Bulk Uploader</h1>
      <span class="mono text-xs px-2 py-0.5 rounded border" style="border-color:#1d6648;color:#3ecf8e;background:rgba(62,207,142,.07)">v5.0</span>
    </div>
    <p class="text-sm" style="color:var(--muted)">
      Upload direto no GCP Storage e BigQuery — detecção automática
      (<span style="color:#93c5fd">pedidos</span> / <span style="color:#fbbf24">produtos</span> / <span style="color:#c4b5fd">clientes</span>).
    </p>
  </div>

  <!-- RECOMENDADO: CSVs soltos (Railway não baixa — sem timeout) -->
  <div class="z1 w-full max-w-2xl mb-8 p-5 rounded-xl border-2" style="background:rgba(62,207,142,.06);border-color:var(--accent)">
    <p class="text-sm font-semibold mb-1" style="color:var(--accent)">Recomendado para muitos GB</p>
    <p class="text-sm mb-4" style="color:var(--muted)">
      Envie <strong style="color:var(--text)">CSVs soltos</strong>. Cada arquivo vai direto para o Storage e o BigQuery carrega de lá — o Railway não baixa nada, evita congelamento e 502.
    </p>
    <div id="csv-drop-zone" class="w-full py-10 flex flex-col items-center gap-3 mb-4 rounded-xl border-2 border-dashed cursor-pointer transition"
         style="border-color:var(--border)" onclick="document.getElementById('csv-file-input').click()">
      <input type="file" id="csv-file-input" accept=".csv" multiple class="hidden" />
      <p class="font-medium">Arraste CSVs aqui ou clique</p>
      <p id="csv-file-count" class="text-sm" style="color:var(--muted)">0 arquivos</p>
    </div>
    <div class="flex items-center gap-2 mb-4">
      <input type="checkbox" id="csv-replace-pedidos" style="accent-color:var(--accent);width:16px;height:16px" />
      <label for="csv-replace-pedidos" class="text-sm">Substituir pedidos (troca diária)</label>
    </div>
    <button id="csv-send-btn" class="btn mb-4" disabled onclick="startCsvUpload()">Enviar CSVs para BigQuery</button>
    <div id="csv-prog-sec" class="mb-4 hidden">
      <div class="flex justify-between text-xs mb-2" style="color:var(--muted)">
        <span id="csv-prog-label" class="pulse">Enviando…</span>
        <span id="csv-prog-pct" class="mono">0%</span>
      </div>
      <div class="bar-wrap"><div id="csv-prog-fill" class="bar-fill" style="width:0%"></div></div>
    </div>
    <div id="csv-summary" class="mb-4 hidden">
      <div class="grid grid-cols-3 gap-3">
        <div class="stat"><p class="mono text-xl font-semibold" id="csv-s-total">0</p><p class="text-xs" style="color:var(--muted)">Arquivos</p></div>
        <div class="stat" style="border-color:rgba(62,207,142,.3)"><p class="mono text-xl font-semibold" id="csv-s-ok" style="color:var(--accent)">0</p><p class="text-xs" style="color:var(--muted)">Sucesso</p></div>
        <div class="stat" style="border-color:rgba(248,113,113,.3)"><p class="mono text-xl font-semibold text-red-400" id="csv-s-err">0</p><p class="text-xs" style="color:var(--muted)">Falhas</p></div>
      </div>
    </div>
    <div id="csv-results-sec" class="hidden"><p class="mono text-xs uppercase mb-2" style="color:var(--muted)">Detalhe</p><div id="csv-results-list" class="scroll flex flex-col gap-2"></div></div>
  </div>

  <!-- Divisor: ou use ZIP -->
  <div class="z1 w-full max-w-2xl mb-4 flex items-center gap-3">
    <div class="flex-1 h-px" style="background:var(--border)"></div>
    <span class="text-xs" style="color:var(--muted)">Ou use um ZIP (pode dar timeout se &gt; 200 MB)</span>
    <div class="flex-1 h-px" style="background:var(--border)"></div>
  </div>

  <!-- ETAPAS (ZIP) -->
  <div class="z1 w-full max-w-2xl mb-6 flex gap-2 items-center">
    <div class="flex items-center gap-2">
      <span id="step1-badge" class="step-badge step-active">1</span>
      <span class="text-sm font-medium">Selecionar ZIP</span>
    </div>
    <div class="flex-1 h-px" style="background:var(--border)"></div>
    <div class="flex items-center gap-2">
      <span id="step2-badge" class="step-badge step-idle">2</span>
      <span class="text-sm font-medium" style="color:var(--muted)" id="step2-label">Upload para Storage</span>
    </div>
    <div class="flex-1 h-px" style="background:var(--border)"></div>
    <div class="flex items-center gap-2">
      <span id="step3-badge" class="step-badge step-idle">3</span>
      <span class="text-sm font-medium" style="color:var(--muted)" id="step3-label">Processar no banco</span>
    </div>
  </div>

  <!-- DROP ZONE -->
  <div id="drop-zone" class="z1 w-full max-w-2xl py-14 flex flex-col items-center gap-4 mb-5"
       onclick="document.getElementById('zip-input').click()">
    <input type="file" id="zip-input" accept=".zip" class="hidden" onchange="handleZipSelect(this)" />
    <svg class="w-12 h-12" viewBox="0 0 24 24" fill="none" stroke-width="1.2" style="color:var(--muted)" stroke="currentColor">
      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
      <polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/>
    </svg>
    <div class="text-center">
      <p class="font-medium mb-1">Arraste o <span style="color:var(--accent)">.zip</span> aqui</p>
      <p class="text-sm" style="color:var(--muted)">ZIPs pequenos (&lt; 200 MB). Para muitos GB use CSVs soltos acima.</p>
    </div>
    <!-- Info do arquivo selecionado -->
    <div id="zip-info" class="hidden flex items-center gap-3 px-4 py-2 rounded-lg border" style="border-color:var(--accent);background:rgba(62,207,142,.06)">
      <svg class="w-5 h-5 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="#3ecf8e" stroke-width="2">
        <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/>
        <polyline points="13 2 13 9 20 9"/>
      </svg>
      <span id="zip-name" class="text-sm font-medium" style="color:var(--accent)"></span>
      <span id="zip-size" class="mono text-xs" style="color:var(--muted)"></span>
      <button onclick="clearZip(event)" class="ml-2 text-xs" style="color:var(--muted)"
        onmouseover="this.style.color='var(--red)'" onmouseout="this.style.color='var(--muted)'">✕</button>
    </div>
  </div>

  <!-- OPÇÃO TROCA DIÁRIA -->
  <div class="z1 w-full max-w-2xl mb-5 flex items-center gap-2">
    <input type="checkbox" id="replace-pedidos" style="accent-color:var(--accent);width:16px;height:16px" />
    <label for="replace-pedidos" class="text-sm">
      Substituir pedidos <span style="color:var(--muted)">(apaga tabela antes de inserir)</span>
    </label>
  </div>

  <!-- BOTÃO PRINCIPAL -->
  <div class="z1 w-full max-w-2xl mb-6">
    <button id="send-btn" class="btn" disabled onclick="startFullUpload()">
      ⚡ Enviar para Supabase
    </button>
  </div>

  <!-- BARRA ETAPA 1: Upload para Storage -->
  <div id="prog1-sec" class="z1 w-full max-w-2xl mb-3 hidden">
    <div class="flex justify-between text-xs mb-2" style="color:var(--muted)">
      <span id="prog1-label" class="pulse">Enviando para Storage…</span>
      <span id="prog1-pct" class="mono">0%</span>
    </div>
    <div class="bar-wrap"><div id="prog1-fill" class="bar-fill" style="width:0%"></div></div>
  </div>

  <!-- BARRA ETAPA 2: Processamento -->
  <div id="prog2-sec" class="z1 w-full max-w-2xl mb-6 hidden">
    <div class="flex justify-between text-xs mb-2" style="color:var(--muted)">
      <span id="prog2-label" class="pulse">Processando arquivos…</span>
      <span id="prog2-pct" class="mono"></span>
    </div>
    <div class="bar-wrap"><div id="prog2-fill" class="bar-fill" style="width:0%;transition:width 2s ease"></div></div>
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
    <p class="text-sm mb-4" style="color:var(--text)">Após subir as 3 bases, crie a view que cruza
      <span style="color:#93c5fd">Pedidos</span> + <span style="color:#fbbf24">Produtos</span> + <span style="color:#c4b5fd">Clientes</span>.
    </p>
    <div class="flex items-center gap-3">
      <button onclick="setupEnriquecida()" class="px-4 py-2 rounded-lg text-sm font-medium border transition"
        style="border-color:var(--accent);color:var(--accent);background:rgba(62,207,142,.08)"
        onmouseover="this.style.background='rgba(62,207,142,.18)'" onmouseout="this.style.background='rgba(62,207,142,.08)'">
        Criar view pedidos_enriquecida
      </button>
      <span id="enriquecida-msg" class="text-sm"></span>
    </div>
  </div>

<script>
const TABLE_COLORS = {pedidos:'type-pedidos',produtos:'type-produtos',clientes:'type-clientes',generico:'type-generico'};
let selectedZip = null;
let csvFiles = [];

function guessType(name) {
  const n = (name||'').toLowerCase();
  if (/pedido|order/i.test(n)) return 'pedidos';
  if (/sku|produto|product/i.test(n)) return 'produtos';
  if (/cliente|customer/i.test(n)) return 'clientes';
  return 'generico';
}

// ── CSVs soltos ──
function initCsvDropZone() {
  const dz = document.getElementById('csv-drop-zone');
  const input = document.getElementById('csv-file-input');
  dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('over'); });
  dz.addEventListener('dragleave', () => dz.classList.remove('over'));
  dz.addEventListener('drop', e => {
    e.preventDefault(); dz.classList.remove('over');
    const files = [...e.dataTransfer.files].filter(f => f.name.toLowerCase().endsWith('.csv'));
    addCsvFiles(files);
  });
  input.addEventListener('change', e => { addCsvFiles([...e.target.files]); e.target.value = ''; });
}
function addCsvFiles(files) {
  for (const f of files) {
    if (csvFiles.length >= 100) break;
    if (!csvFiles.some(x => x.name === f.name)) csvFiles.push(f);
  }
  document.getElementById('csv-file-count').textContent = csvFiles.length + ' arquivo(s)';
  document.getElementById('csv-send-btn').disabled = csvFiles.length === 0;
}
async function startCsvUpload() {
  if (csvFiles.length === 0) return;
  const btn = document.getElementById('csv-send-btn');
  const replacePedidos = document.getElementById('csv-replace-pedidos').checked;
  btn.disabled = true;
  document.getElementById('csv-prog-sec').classList.remove('hidden');
  document.getElementById('csv-summary').classList.add('hidden');
  document.getElementById('csv-results-sec').classList.add('hidden');
  const results = [];
  const total = csvFiles.length;
  let firstPedidosDone = false;
  for (let i = 0; i < csvFiles.length; i++) {
    const file = csvFiles[i];
    const tableType = guessType(file.name);
    const pct = Math.round((i / total) * 100);
    document.getElementById('csv-prog-fill').style.width = pct + '%';
    document.getElementById('csv-prog-pct').textContent = pct + '%';
    document.getElementById('csv-prog-label').textContent = 'Enviando ' + (i+1) + '/' + total + ': ' + file.name + ' para GCS…';
    document.getElementById('csv-prog-label').classList.add('pulse');
    try {
      const tokenRes = await fetch(window.location.origin + '/storage-token', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ filename: file.name, unique: true, content_type: 'text/csv' })
      });
      if (!tokenRes.ok) throw new Error('URL: ' + await tokenRes.text());
      const { upload_url, path } = await tokenRes.json();
      const putRes = await fetch(upload_url, { method: 'PUT', body: file, headers: { 'Content-Type': 'text/csv' } });
      if (!putRes.ok) throw new Error('GCS PUT ' + putRes.status);
      document.getElementById('csv-prog-label').textContent = 'Carregando no BigQuery: ' + file.name + '…';
      const loadRes = await fetch(window.location.origin + '/load-from-gcs', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          path,
          table_type: tableType,
          replace_pedidos: replacePedidos && tableType === 'pedidos' && !firstPedidosDone
        })
      });
      const loadData = await loadRes.json();
      if (loadData.status === 'ok' && tableType === 'pedidos') firstPedidosDone = true;
      results.push({ file: file.name, status: loadData.status === 'ok' ? 'ok' : 'error', table: tableType, rows: loadData.rows || 0, error: loadData.message });
    } catch (err) {
      results.push({ file: file.name, status: 'error', table: guessType(file.name), error: err.message });
    }
  }
  document.getElementById('csv-prog-fill').style.width = '100%';
  document.getElementById('csv-prog-pct').textContent = '100%';
  document.getElementById('csv-prog-label').textContent = 'Concluído.';
  document.getElementById('csv-prog-label').classList.remove('pulse');
  document.getElementById('csv-s-total').textContent = results.length;
  document.getElementById('csv-s-ok').textContent = results.filter(r => r.status === 'ok').length;
  document.getElementById('csv-s-err').textContent = results.filter(r => r.status !== 'ok').length;
  document.getElementById('csv-summary').classList.remove('hidden');
  const list = document.getElementById('csv-results-list');
  list.innerHTML = '';
  results.forEach(r => {
    const tc = TABLE_COLORS[r.table] || 'type-generico';
    const badge = r.status === 'ok' ? '<span class="badge b-ok">OK</span>' : '<span class="badge b-err">ERRO</span>';
    const row = document.createElement('div');
    row.className = 'res-row';
    row.innerHTML = `<span class="truncate text-sm">${r.file}</span><span class="type-badge ${tc}">${r.table}</span>${r.rows ? '<span class="mono text-xs" style="color:var(--muted)">' + r.rows.toLocaleString('pt-BR') + ' linhas</span>' : ''}${badge}`;
    if (r.error) { const pre = document.createElement('pre'); pre.style.cssText = 'font-size:11px;color:var(--red);margin-top:4px'; pre.textContent = r.error; row.appendChild(pre); }
    list.appendChild(row);
  });
  document.getElementById('csv-results-sec').classList.remove('hidden');
  btn.disabled = false;
}

// ── DRAG & DROP (ZIP) ──
const zone = document.getElementById('drop-zone');
zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('over'); });
zone.addEventListener('dragleave', () => zone.classList.remove('over'));
zone.addEventListener('drop', e => {
  e.preventDefault(); zone.classList.remove('over');
  const f = [...e.dataTransfer.files].find(f => f.name.toLowerCase().endsWith('.zip'));
  if (f) setZip(f); else alert('Envie um arquivo .zip');
});
function handleZipSelect(input) { if (input.files[0]) setZip(input.files[0]); input.value=''; }

function setZip(f) {
  selectedZip = f;
  document.getElementById('zip-name').textContent = f.name;
  document.getElementById('zip-size').textContent = (f.size/1024/1024).toFixed(1) + ' MB';
  document.getElementById('zip-info').classList.remove('hidden');
  document.getElementById('send-btn').disabled = false;
  setStep(1);
}

function clearZip(e) {
  e.stopPropagation();
  selectedZip = null;
  document.getElementById('zip-info').classList.add('hidden');
  document.getElementById('send-btn').disabled = true;
  ['prog1-sec','prog2-sec','summary','results-sec'].forEach(id => document.getElementById(id).classList.add('hidden'));
  setStep(1);
}

function setStep(n) {
  ['step1-badge','step2-badge','step3-badge'].forEach((id, i) => {
    const el = document.getElementById(id);
    el.className = 'step-badge ' + (i+1 < n ? 'step-done' : i+1 === n ? 'step-active' : 'step-idle');
  });
  document.getElementById('step2-label').style.color = n >= 2 ? 'var(--text)' : 'var(--muted)';
  document.getElementById('step3-label').style.color = n >= 3 ? 'var(--text)' : 'var(--muted)';
}

// ── FLUXO COMPLETO ──
async function startFullUpload() {
  if (!selectedZip) return;
  const btn = document.getElementById('send-btn');
  btn.disabled = true;
  ['summary','results-sec'].forEach(id => document.getElementById(id).classList.add('hidden'));

  try {
    // ── ETAPA 1: Pegar signed URL do backend ──
    setStep(2);
    const tokenRes = await fetch(window.location.origin + '/storage-token', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({filename: selectedZip.name})
    });
    if (!tokenRes.ok) throw new Error('Erro ao gerar URL de upload: ' + await tokenRes.text());
    const {upload_url, path} = await tokenRes.json();

    // ── ETAPA 2: Upload direto para Supabase Storage via XHR (progresso real) ──
    const p1sec   = document.getElementById('prog1-sec');
    const p1label = document.getElementById('prog1-label');
    const p1fill  = document.getElementById('prog1-fill');
    const p1pct   = document.getElementById('prog1-pct');
    p1sec.classList.remove('hidden');
    p1label.classList.add('pulse');

    await new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open('PUT', upload_url);
      xhr.setRequestHeader('Content-Type', 'application/octet-stream');
      xhr.upload.onprogress = (e) => {
        if (e.lengthComputable) {
          const p = Math.round((e.loaded / e.total) * 100);
          p1fill.style.width = p + '%';
          p1pct.textContent  = p + '%';
          p1label.textContent = 'Enviando para Storage… ' + (e.loaded/1024/1024).toFixed(0) + ' / ' + (e.total/1024/1024).toFixed(0) + ' MB';
        }
      };
      xhr.onload = () => {
        if (xhr.status >= 200 && xhr.status < 300) resolve();
        else reject(new Error('Upload falhou: HTTP ' + xhr.status + ' — ' + xhr.responseText));
      };
      xhr.onerror = () => reject(new Error('Erro de rede no upload'));
      xhr.send(selectedZip);
    });

    p1fill.style.width = '100%'; p1pct.textContent = '100%';
    p1label.textContent = '✓ ZIP no Storage';
    p1label.classList.remove('pulse');
    p1label.style.color = 'var(--accent)';

    // ── ETAPA 3: Railway processa o ZIP do Storage ──
    setStep(3);
    const p2sec   = document.getElementById('prog2-sec');
    const p2label = document.getElementById('prog2-label');
    const p2fill  = document.getElementById('prog2-fill');
    const p2pct   = document.getElementById('prog2-pct');
    p2sec.classList.remove('hidden');
    p2label.classList.add('pulse');
    p2fill.style.width = '5%';

    // Animação de progresso indeterminada enquanto o backend processa
    let fakeProgress = 5;
    const ticker = setInterval(() => {
      if (fakeProgress < 90) {
        fakeProgress += Math.random() * 2;
        p2fill.style.width = fakeProgress.toFixed(0) + '%';
        p2pct.textContent  = fakeProgress.toFixed(0) + '%';
      }
    }, 3000);

    let data;
    try {
      const processRes = await fetch(window.location.origin + '/process-storage', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          path: path,
          replace_pedidos: document.getElementById('replace-pedidos').checked
        })
      });
      if (!processRes.ok) throw new Error('Processamento falhou: ' + await processRes.text());
      data = await processRes.json();
    } finally {
      clearInterval(ticker);
    }

    p2fill.style.width = '100%'; p2pct.textContent = '100%';
    p2label.textContent = '✓ Processamento concluído — ' + data.success + ' de ' + data.total + ' arquivos';
    p2label.classList.remove('pulse');
    p2label.style.color = 'var(--accent)';

    showResults(data);

  } catch (err) {
    // Mostrar erro na barra ativa
    ['prog1-label','prog2-label'].forEach(id => {
      const el = document.getElementById(id);
      if (el && el.classList.contains('pulse')) {
        el.textContent = '✗ ' + err.message;
        el.classList.remove('pulse');
        el.style.color = 'var(--red)';
      }
    });
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
    const badge = r.status==='ok' ? '<span class="badge b-ok">OK</span>' : '<span class="badge b-err">ERRO</span>';
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
    el.textContent = data.status==='ok' ? '✓ View criada/atualizada' : '✗ ' + data.message;
    el.style.color = data.status==='ok' ? 'var(--accent)' : 'var(--red)';
  } catch (err) {
    el.textContent = '✗ ' + err.message; el.style.color = 'var(--red)';
  }
}
document.addEventListener('DOMContentLoaded', initCsvDropZone);
</script>
</body>
</html>"""

@app.get("/", response_class=HTMLResponse)
def frontend():
    return HTMLResponse(content=_HTML)