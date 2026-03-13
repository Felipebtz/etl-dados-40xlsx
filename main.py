"""
GCP Bulk Uploader
Principal: carregamento de CSV do GCS (stream + load job Parquet) — rápido, suporta milhões de linhas.
Também: ZIP com XLSX/CSV. Detecção automática (pedidos / produtos / clientes).
Objetivo final: view unificada (enriquecimento) para uso em uma única plataforma.
"""

from dotenv import load_dotenv
load_dotenv()

import asyncio
import csv
import gzip
import io
import json
import logging
import os
import re
import tempfile
import time
import traceback
import uuid
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

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
    """
    Carrega credenciais GCP de uma destas formas:
    1) GOOGLE_APPLICATION_CREDENTIALS = caminho do arquivo .json (ex.: springboot-demo-484902-6d1ccdd455a6.json)
    2) GCP_SERVICE_ACCOUNT_JSON = conteúdo do JSON colado (para Railway)
    """
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if path:
        # Caminho relativo à pasta do projeto
        if not os.path.isabs(path):
            path = str(Path(__file__).resolve().parent / path)
        if os.path.isfile(path):
            return service_account.Credentials.from_service_account_file(
                path,
                scopes=[
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/bigquery",
                ],
            )
    raw = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "")
    if not raw:
        raise RuntimeError(
            "Credenciais GCP não encontradas. Crie um arquivo .env na pasta do projeto com:\n"
            "  GCP_PROJECT_ID=springboot-demo-484902\n"
            "  GCP_BUCKET=el-lucas\n"
            "  GCP_DATASET=dataset_lucas\n"
            "  GOOGLE_APPLICATION_CREDENTIALS=springboot-demo-484902-6d1ccdd455a6.json\n"
            "E coloque o arquivo .json da service account na mesma pasta do main.py."
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

# Tamanho do lote para CSV (principal fluxo). 100k–200k = menos load jobs, carga mais rápida. Ajuste via ROWS_PER_BATCH.
ROWS_PER_BATCH = int(os.getenv("ROWS_PER_BATCH", "100000"))
# Número de arquivos processados em paralelo no ZIP (1 = sequencial; 2–4 para muitos arquivos)
PARALLEL_WORKERS = max(1, min(int(os.getenv("PARALLEL_WORKERS", "2")), 8))

CSV_ENCODINGS = ("utf-8", "utf-8-sig", "cp1252", "latin-1", "iso-8859-1")
CSV_SEPARATORS = (",", ";", "\t", "|")

# ──────────────────────────────────────────────────────────────
# SCHEMAS — mapeamento para tipos BigQuery
# ──────────────────────────────────────────────────────────────
# BigQuery types: STRING, INT64, FLOAT64, NUMERIC, BOOL, DATE, TIMESTAMP
#
# Erros de schema no load job (Parquet → BQ) e como evitamos:
# - _loaded_at TIMESTAMP vs STRING: align_df usa pd.Timestamp.now(tz="UTC"), não .isoformat().
# - cod_pedido / INT64 vs STRING: _coerce_col_for_parquet(..., "INT64") converte para Int64.
# - Colunas DATE: _coerce_col_for_parquet usa .dt.date para Parquet gravar date32 (BQ DATE).
# - Colunas BOOL: _coerce_col_for_parquet normaliza para boolean (evita STRING no Parquet).
# - INT64/FLOAT64/NUMERIC: _coerce_col_for_parquet converte para tipo numérico.
# - df.where(pd.notnull(df), None) só em colunas object; senão Int64/boolean viram object e Parquet grava STRING.
# - Tabela "generico": ensure_bq_table garante _source_file e _loaded_at no schema.
# - Particionamento: tabela exige _loaded_at no schema; ensure_bq_table adiciona se faltar.
# - NUMERIC no Parquet: pandas/pyarrow grava como float64; BQ rejeita "changed type from NUMERIC to FLOAT".
#   Por isso usamos FLOAT64 no schema para valores monetários; tabelas já criadas com NUMERIC precisam
#   ALTER COLUMN ... SET DATA TYPE FLOAT64 ou DROP + recriar.
# - Colunas STRING: align_df aplica .apply(str) + .astype("string") para o Parquet gravar string; sem isso,
#   valores só numéricos (ex.: código "1234") seriam inferidos como INT64 → schema mismatch no load job.
# - Escrita no BQ: único caminho é bq_insert_batch (Parquet + load_table_from_file); insert_rows_json não é usado.
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
            ("valor_item_pedido",             "FLOAT64"),
            ("valor_item_pedido_frete",       "FLOAT64"),
            ("qtd_itens",                     "INT64"),
            ("canal_venda",                   "STRING"),
            ("bandeira",                      "STRING"),
            ("data_pedido",                   "DATE"),
            ("cod_tip_frete",                 "INT64"),
            ("descricao_tipo_frete",          "STRING"),
            ("data_atualizacao",              "DATE"),
            ("valor_frete",                   "FLOAT64"),
            ("flag_retira",                   "BOOL"),
            ("data_entrega",                  "TIMESTAMP"),
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
    """Remove colunas Unnamed (linhas com 'too many values') e normaliza nulos."""
    unnamed = df.columns.str.match(r"^Unnamed:\s*\d+$", na=False)
    if unnamed.any():
        df = df.loc[:, ~unnamed]
    return df.replace({"": None, "nan": None, "NaN": None, "None": None, "NULL": None})

def _coerce_col_for_parquet(series: pd.Series, bq_type: str, col_name: str = None) -> pd.Series:
    """Converte coluna para tipo compatível com Parquet/BigQuery e evita schema mismatch no load job.
    Loga quando valores viram NaN após coerção (ex.: vírgula decimal, texto inválido)."""
    col_label = col_name or "?"
    if bq_type == "TIMESTAMP":
        out = pd.to_datetime(series, utc=True, errors="coerce")
        _log_coerce_nans(series, out, col_label, "TIMESTAMP")
        return out
    if bq_type == "DATE":
        out = pd.to_datetime(series, errors="coerce").dt.date
        _log_coerce_nans(series, out, col_label, "DATE")
        return out
    if bq_type == "BOOL":
        if series.dtype == bool or series.dtype.name == "boolean":
            return series
        s = series.astype(str).str.lower().str.strip()
        out = s.isin(("1", "true", "sim", "yes", "s")).where(series.notna(), pd.NA).astype("boolean")
        return out
    if bq_type == "INT64":
        out = pd.to_numeric(series, errors="coerce").astype("Int64")
        _log_coerce_nans(series, out, col_label, "INT64")
        return out
    if bq_type in ("FLOAT64", "NUMERIC"):
        out = pd.to_numeric(series, errors="coerce")
        _log_coerce_nans(series, out, col_label, bq_type)
        return out
    return series


def _log_coerce_nans(before: pd.Series, after: pd.Series, col_name: str, bq_type: str) -> None:
    """Se a coerção introduziu novos nulls (ex.: '1.831,23' → NaN), loga aviso."""
    before_ok = before.notna() & (before.astype(str).str.strip() != "")
    after_ok = after.notna()
    new_nans = before_ok & ~after_ok
    n = new_nans.sum()
    if n > 0:
        log.warning("Coerção %s (%s): %s valor(es) viraram null (ex.: vírgula decimal ou inválido)", col_name, bq_type, n)


def align_df(df: pd.DataFrame, table: str, source_file: str) -> pd.DataFrame:
    df.columns = [sanitize_col(c) for c in df.columns]
    df["_source_file"] = source_file
    # TIMESTAMP para Parquet/BQ: tipo nativo evita erro "Field _loaded_at has changed type from TIMESTAMP to STRING"
    df["_loaded_at"] = pd.Timestamp.now(tz="UTC")

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

    # Coerção de tipos para evitar schema mismatch no load job (Parquet → BigQuery)
    for col, bq_type in schema["columns"]:
        if col not in df.columns:
            continue
        if bq_type in ("TIMESTAMP", "DATE", "BOOL", "INT64", "FLOAT64", "NUMERIC"):
            df[col] = _coerce_col_for_parquet(df[col], bq_type, col_name=col)
        elif bq_type == "STRING":
            # Garante STRING no Parquet (evita "Field X has changed type from STRING to INTEGER" quando valores são numéricos)
            s = df[col].apply(lambda x: None if pd.isna(x) or x is None else str(x))
            df[col] = s.astype("string")  # StringDtype → Parquet grava como string, não infere int

    # Só substituir null por None em colunas object (vetorial); evita Int64/boolean virarem object.
    obj_cols = [c for c in df.columns if df[c].dtype == object]
    if obj_cols:
        df[obj_cols] = df[obj_cols].where(pd.notnull(df[obj_cols]), None)
    return df

# ──────────────────────────────────────────────────────────────
# BIGQUERY — criar tabela + inserir em lotes
# ──────────────────────────────────────────────────────────────
def get_bq_client():
    creds = _get_gcp_credentials()
    return bigquery.Client(project=CONFIG["project_id"], credentials=creds)

def ensure_bq_table(bq: bigquery.Client, table_id: str, schema_def: list[tuple]):
    """Cria a tabela no BigQuery se não existir. Garante _source_file e _loaded_at no schema (exigido pelo particionamento).
    Se a tabela já existir com colunas NUMERIC (ex.: valor_item_pedido), altere para FLOAT64 no BQ ou apague a tabela e deixe recriar."""
    full_id = f"{CONFIG['project_id']}.{CONFIG['dataset']}.{table_id}"
    schema_def = list(schema_def)
    cols = {col for col, _ in schema_def}
    if "_source_file" not in cols:
        schema_def.append(("_source_file", "STRING"))
    if "_loaded_at" not in cols:
        schema_def.append(("_loaded_at", "TIMESTAMP"))
    schema  = [bigquery.SchemaField(col, typ) for col, typ in schema_def]
    table   = bigquery.Table(full_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="_loaded_at")
    bq.create_table(table, exists_ok=True)
    log.info("BigQuery: tabela criada/verificada %s (%s colunas)", full_id, len(schema))
    return full_id

def ensure_bq_dataset(bq: bigquery.Client):
    """Cria o dataset se não existir."""
    dataset_id = f"{CONFIG['project_id']}.{CONFIG['dataset']}"
    dataset    = bigquery.Dataset(dataset_id)
    dataset.location = os.getenv("GCP_LOCATION", "US")
    bq.create_dataset(dataset, exists_ok=True)

def bq_insert_batch(bq: bigquery.Client, full_table_id: str, df: pd.DataFrame):
    """
    Insere um DataFrame no BigQuery via load job (Parquet).
    Evita pico de RAM de insert_rows_json e escala para milhões de linhas.
    Fluxo: DataFrame → arquivo Parquet temporário → load_table_from_file → WRITE_APPEND.
    """
    if df.empty:
        return
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        # Garante tipos compatíveis: colunas object com apenas date() viram date32 no Parquet
        df.to_parquet(tmp_path, index=False)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        with open(tmp_path, "rb") as f:
            load_job = bq.load_table_from_file(f, full_table_id, job_config=job_config)
        load_job.result()
        log.info(
            "BigQuery: inseridas %s linhas em %s via load job (Parquet)",
            len(df),
            full_table_id.split(".")[-1],
        )
    except Exception as e:
        log.exception(
            "BigQuery load job falhou para %s (%s linhas). Dtypes: %s",
            full_table_id.split(".")[-1],
            len(df),
            df.dtypes.to_dict(),
        )
        raise
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

# ──────────────────────────────────────────────────────────────
# PIPELINE CSV (PRINCIPAL) — stream, limpeza, load job Parquet, enriquecimento no final
# Fluxo: CSV no GCS → stream (sem carregar tudo na RAM) → lotes → Parquet → BigQuery.
# Objetivo: carregamento rápido e estável de milhões de linhas; enriquecimento via view unificada.
# ──────────────────────────────────────────────────────────────
def _read_lines_from_stream(stream, chunk_size: int = 2**20) -> iter:
    """Lê stream binário em chunks, remove \\x00, entrega linha a linha — SEM acumular o arquivo na RAM."""
    buffer = b""
    for chunk in iter(lambda: stream.read(chunk_size), b""):
        buffer += chunk.replace(b"\x00", b"")
        parts = buffer.split(b"\n")
        buffer = parts.pop()
        for line in parts:
            line = line.rstrip(b"\r")
            if line:
                yield line
    if buffer.strip():
        yield buffer.strip()

def _detect_sep_from_header(line: bytes, encodings: tuple = CSV_ENCODINGS) -> tuple[str, str]:
    """Detecta encoding e separador usando csv.reader; escolhe o sep que produz mais colunas (evita vírgula dentro de campo)."""
    best_enc, best_sep = "utf-8", ","
    best_cols = 0
    for enc in encodings:
        try:
            text = line.decode(enc)
            for sep in CSV_SEPARATORS:
                row = next(csv.reader(io.StringIO(text), delimiter=sep))
                if len(row) >= 2 and len(row) > best_cols:
                    best_enc, best_sep, best_cols = enc, sep, len(row)
        except Exception:
            continue
    return (best_enc, best_sep) if best_cols >= 2 else ("utf-8", ",")

def stream_clean_csv(stream, batch_size: int = None) -> iter:
    """
    Lê CSV do stream linha a linha: remove ASCII 0, valida número de colunas,
    descarta linhas irrecuperáveis. Entrega (header, list_of_rows) em lotes.
    Usa csv.reader para respeitar campos entre aspas. Conta e loga linhas descartadas.
    """
    batch_size = batch_size or ROWS_PER_BATCH
    lines = _read_lines_from_stream(stream)
    header_line = next(lines, None)
    if not header_line:
        return
    enc, sep = _detect_sep_from_header(header_line)
    text = header_line.decode(enc, errors="replace")
    header = next(csv.reader(io.StringIO(text), delimiter=sep))
    header = [sanitize_col(h) for h in header]
    n_expected = len(header)
    batch = []
    bad_lines = 0
    for line in lines:
        try:
            text = line.decode(enc, errors="replace")
            row = next(csv.reader(io.StringIO(text), delimiter=sep))
            if len(row) != n_expected:
                bad_lines += 1
                continue
            batch.append(row)
            if len(batch) >= batch_size:
                if bad_lines > 0:
                    log.warning("CSV: %s linhas descartadas (colunas != %s) neste lote", bad_lines, n_expected)
                bad_lines = 0
                yield header, batch
                batch = []
        except Exception:
            bad_lines += 1
            continue
    if bad_lines > 0:
        log.warning("CSV: %s linhas descartadas (colunas != %s) no lote final", bad_lines, n_expected)
    if batch:
        yield header, batch


def _stream_csv_to_bq(
    stream,
    filename: str,
    bq: bigquery.Client,
    replace_pedidos: bool,
    table_type_hint: str = "generico",
) -> tuple[int, str]:
    """
    Pipeline CSV: stream → stream_clean_csv → lotes → load job Parquet no BigQuery.
    Usado por load-from-gcs (principal) e por CSV dentro do ZIP. Suporta milhões de linhas.
    Retorna (total_rows, table_name).
    """
    log.info("CSV stream: iniciando leitura e carga para %s", filename)
    ensure_bq_dataset(bq)
    full_table_id = None
    table = table_type_hint if table_type_hint in ("pedidos", "produtos", "clientes") else "generico"
    total_rows = 0
    batch_num = 0
    if replace_pedidos and table == "pedidos":
        full_id = f"{CONFIG['project_id']}.{CONFIG['dataset']}.pedidos"
        try:
            bq.delete_table(full_id)
            log.info("CSV stream: tabela pedidos truncada (replace_pedidos)")
        except Exception:
            pass

    for header, rows in stream_clean_csv(stream):
        if not rows:
            continue
        df = pd.DataFrame(rows, columns=header, dtype=str)
        df = _clean_chunk(df).dropna(how="all")
        if df.empty:
            continue
        if full_table_id is None:
            table = detect_table(list(df.columns))
            schema_def = SCHEMAS.get(table, {}).get("columns") or [
                (sanitize_col(c), "STRING") for c in df.columns
            ]
            full_table_id = ensure_bq_table(bq, table, schema_def)
            log.info("CSV stream: tipo detectado=%s, tabela=%s", table, full_table_id.split(".")[-1])
        df = align_df(df, table, filename)
        bq_insert_batch(bq, full_table_id, df)
        total_rows += len(df)
        batch_num += 1
        log.info("CSV stream: %s — lote %s: +%s linhas (total %s)", filename, batch_num, len(df), total_rows)
    log.info("CSV stream: concluído %s — tabela=%s, total=%s linhas", filename, table or "generico", total_rows)
    log.info("CSV: próximo passo → POST /setup-enriquecida para criar a view unificada (pedidos + produtos + clientes)")
    return total_rows, table or "generico"


def stream_from_gcs_clean_and_load(
    bucket_name: str,
    path: str,
    filename: str,
    table_type: str,
    bq: bigquery.Client,
    replace_pedidos: bool,
) -> tuple[int, str]:
    """
    CSV no GCS: stream (sem carregar arquivo inteiro) → limpeza → lotes Parquet → load job BQ.
    Fluxo principal para carregamento rápido de CSVs grandes (milhões de linhas).
    Retorna (total_rows, table_name) onde table_name é o tipo detectado (pedidos/produtos/clientes/generico).
    """
    log.info("GCS CSV: abrindo stream %s/%s para %s", bucket_name, path, filename)
    gcs    = get_gcs_client()
    bucket = gcs.bucket(bucket_name)
    blob   = bucket.blob(path)
    with blob.open("rb") as stream:
        total_rows, table = _stream_csv_to_bq(stream, filename, bq, replace_pedidos, table_type)
    log.info("GCS CSV: carga finalizada %s — %s linhas (use /setup-enriquecida para view unificada)", filename, total_rows)
    return total_rows, table

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
    size_mb = len(raw_content) / 1024 / 1024
    log.info("CSV (memória): iniciando %s (%.1f MB), lote=%s", filename, size_mb, ROWS_PER_BATCH)
    raw_clean = raw_content.replace(b"\x00", b"")
    enc, sep  = _detect_csv_params(raw_clean)
    log.info("CSV (memória): encoding=%s, sep=%r", enc, sep)
    reader    = pd.read_csv(io.BytesIO(raw_clean), sep=sep, encoding=enc,
                            on_bad_lines="skip", dtype=str, keep_default_na=False,
                            chunksize=ROWS_PER_BATCH)
    table       = None
    total_rows  = 0
    full_tbl_id = None
    batch_num   = 0

    for batch in reader:
        batch = _clean_chunk(batch).dropna(how="all")
        if batch.empty:
            continue
        if table is None:
            table      = detect_table(list(batch.columns))
            schema_def = SCHEMAS.get(table, {}).get("columns") or \
                         [(sanitize_col(c), "STRING") for c in batch.columns]
            full_tbl_id = ensure_bq_table(bq_client, table, schema_def)
            log.info("CSV (memória): tipo detectado=%s", table)

        batch = align_df(batch, table, filename)
        bq_insert_batch(bq_client, full_tbl_id, batch)
        total_rows += len(batch)
        batch_num += 1
        log.info("CSV (memória): %s — lote %s: +%s linhas (total %s)", filename, batch_num, len(batch), total_rows)
        del batch

    log.info("CSV (memória): concluído %s — tabela=%s, total=%s linhas", filename, table or "generico", total_rows)
    return table or "generico", total_rows

# ──────────────────────────────────────────────────────────────
# STREAMING XLSX — openpyxl read_only, linha por linha
# ──────────────────────────────────────────────────────────────
def stream_xlsx(raw_content: bytes, filename: str, bq_client: bigquery.Client) -> tuple[str, int]:
    import openpyxl
    size_mb = len(raw_content) / 1024 / 1024
    log.info("XLSX: abrindo workbook %s (%.1f MB)...", filename, size_mb)
    wb = openpyxl.load_workbook(io.BytesIO(raw_content), read_only=True, data_only=True)
    ws = wb.active
    log.info("XLSX: workbook aberto, planilha ativa=%s. Iniciando leitura linha a linha (lote=%s)...", ws.title, ROWS_PER_BATCH)

    table       = None
    total_rows  = 0
    full_tbl_id = None
    headers     = None
    batch_rows  = []
    batch_num   = 0

    def flush(rows):
        nonlocal table, full_tbl_id, total_rows, batch_num
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
            log.info("XLSX: tipo detectado=%s, tabela=%s", table, full_tbl_id.split(".")[-1])

        df = align_df(df, table, filename)
        bq_insert_batch(bq_client, full_tbl_id, df)
        total_rows += len(df)
        batch_num += 1
        log.info("XLSX: %s — lote %s inserido: +%s linhas (total %s)", filename, batch_num, len(df), total_rows)
        del df

    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            headers = [str(c) if c is not None else f"col_{j}" for j, c in enumerate(row)]
            log.info("XLSX: %s — cabeçalho com %s colunas", filename, len(headers))
            continue
        batch_rows.append([str(v) if v is not None else None for v in row])
        if len(batch_rows) >= ROWS_PER_BATCH:
            flush(batch_rows)
            batch_rows = []

    if batch_rows:
        flush(batch_rows)

    wb.close()
    log.info("XLSX: concluído %s — tabela=%s, total=%s linhas", filename, table or "generico", total_rows)
    return table or "generico", total_rows

# ──────────────────────────────────────────────────────────────
# PROCESSAMENTO DE UM ARQUIVO
# ──────────────────────────────────────────────────────────────
def process_file(filename: str, content: bytes) -> dict:
    start  = time.time()
    size_mb = len(content) / 1024 / 1024
    log.info("process_file: iniciando %s (%.1f MB)", filename, size_mb)
    result = {"file": filename, "status": "ok", "rows": 0, "table": "", "elapsed_s": 0, "error": None}
    try:
        real_filename = filename
        raw_content   = content
        if filename.lower().endswith(".gz"):
            log.info("process_file: descomprimindo .gz...")
            raw_content   = gzip.decompress(content)
            real_filename = filename[:-3]
            result["file"] = real_filename

        bq = get_bq_client()
        ensure_bq_dataset(bq)

        if real_filename.lower().endswith((".csv", ".txt")):
            table, rows = stream_csv(raw_content, real_filename, bq)
        else:
            table, rows = stream_xlsx(raw_content, real_filename, bq)

        result["table"] = table
        result["rows"]  = rows
        del raw_content
        result["elapsed_s"] = round(time.time() - start, 2)
        log.info("process_file: concluído %s — tabela=%s, %s linhas em %.1fs", filename, result["table"], result["rows"], result["elapsed_s"])

    except Exception:
        result["status"] = "error"
        result["error"]  = traceback.format_exc()
        result["elapsed_s"] = round(time.time() - start, 2)
        log.exception("process_file: ERRO em %s após %.1fs", filename, result["elapsed_s"])

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
    try:
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
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


def _zip_aceita(n: str) -> bool:
    """Se o nome do entry do ZIP deve ser processado (evita __MACOSX, etc.)."""
    n = n.replace("\\", "/")
    base = os.path.basename(n)
    if n.startswith("__MACOSX") or base.startswith("."):
        return False
    low = base.lower()
    if low.endswith((".xlsx", ".xls", ".csv", ".txt", ".csv.gz", ".txt.gz")):
        return True
    if ".csv" in low or ".xlsx" in low or ".xls" in low:
        return True
    return False


def _zip_is_streamable_csv(name: str) -> bool:
    """Se o entry é CSV/txt (incl. .gz) e pode ser lido em stream."""
    low = os.path.basename(name).lower()
    if low.endswith(".gz"):
        low = low[:-3]
    return low.endswith(".csv") or low.endswith(".txt")


def _process_one_zip_entry(args: tuple) -> tuple[int, dict]:
    """
    Processa um único arquivo dentro do ZIP. Usado por ProcessPoolExecutor.
    Retorna (índice_original, result_dict).
    """
    tmp_path, idx, name = args
    start = time.time()
    filename = os.path.basename(name)
    try:
        with zipfile.ZipFile(tmp_path, "r") as zf:
            if _zip_is_streamable_csv(name):
                display_name = filename[:-3] if filename.lower().endswith(".gz") else filename
                bq = get_bq_client()
                # replace_pedidos já foi aplicado no início de process_storage (truncate pedidos); workers só fazem append.
                if filename.lower().endswith(".gz"):
                    with zf.open(name) as raw:
                        with gzip.GzipFile(fileobj=raw) as stream:
                            total_rows, table = _stream_csv_to_bq(stream, display_name, bq, False)
                else:
                    with zf.open(name) as stream:
                        total_rows, table = _stream_csv_to_bq(stream, display_name, bq, False)
                elapsed = round(time.time() - start, 2)
                return (idx, {
                    "file": display_name,
                    "status": "ok",
                    "rows": total_rows,
                    "table": table,
                    "elapsed_s": elapsed,
                    "error": None,
                })
            else:
                content_bytes = zf.read(name)
                result = process_file(filename, content_bytes)
                result["elapsed_s"] = round(time.time() - start, 2)
                return (idx, result)
    except Exception:
        elapsed = round(time.time() - start, 2)
        return (idx, {
            "file": filename,
            "status": "error",
            "rows": 0,
            "table": "",
            "elapsed_s": elapsed,
            "error": traceback.format_exc(),
        })


@app.post("/process-storage")
async def process_storage(body: dict):
    """
    Baixa o ZIP do GCS em streaming para arquivo temporário,
    extrai e processa cada arquivo no BigQuery (em paralelo quando PARALLEL_WORKERS > 1).
    """
    path            = body.get("path", "")
    replace_pedidos = body.get("replace_pedidos", False)
    if not path:
        raise HTTPException(400, "Campo 'path' obrigatório.")

    log.info("process-storage: iniciando path=%s, replace_pedidos=%s", path, replace_pedidos)
    gcs    = get_gcs_client()
    bucket = gcs.bucket(CONFIG["bucket"])
    blob   = bucket.blob(path)

    if replace_pedidos:
        bq      = get_bq_client()
        full_id = f"{CONFIG['project_id']}.{CONFIG['dataset']}.pedidos"
        try:
            bq.delete_table(full_id)
        except Exception:
            pass

    tmp_path = None
    results  = []
    try:
        log.info("process-storage: baixando ZIP do GCS para arquivo temporário...")
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = tmp.name
            blob.download_to_file(tmp)
        log.info("process-storage: ZIP baixado em %s", tmp_path)

        if not zipfile.is_zipfile(tmp_path):
            raise HTTPException(400, "Arquivo no Storage não é um ZIP válido.")

        with zipfile.ZipFile(tmp_path) as zf:
            names = [n.replace("\\", "/") for n in zf.namelist() if _zip_aceita(n)]

        if not names:
            raise HTTPException(
                400,
                "Nenhum arquivo .csv, .xlsx, .xls ou .txt encontrado no ZIP. "
                "Verifique: extensão correta, arquivos na raiz ou em subpastas (todos são considerados)."
            )

        total_files = len(names)
        workers = PARALLEL_WORKERS if total_files > 1 else 1
        log.info("ZIP: %s arquivos a processar (workers=%s): %s", total_files, workers, [os.path.basename(n) for n in names])

        if workers <= 1:
            # Sequencial
            bq = get_bq_client()
            for idx, name in enumerate(names):
                filename = os.path.basename(name)
                start_t = time.time()
                log.info("ZIP: [%s/%s] Processando %s", idx + 1, total_files, filename)
                try:
                    if _zip_is_streamable_csv(name):
                        display_name = filename[:-3] if filename.lower().endswith(".gz") else filename
                        # replace_pedidos já aplicado no início (truncate pedidos); aqui só append.
                        if filename.lower().endswith(".gz"):
                            with zipfile.ZipFile(tmp_path) as zf:
                                with zf.open(name) as raw:
                                    with gzip.GzipFile(fileobj=raw) as stream:
                                        total_rows, table = _stream_csv_to_bq(stream, display_name, bq, False)
                        else:
                            with zipfile.ZipFile(tmp_path) as zf:
                                with zf.open(name) as stream:
                                    total_rows, table = _stream_csv_to_bq(stream, display_name, bq, False)
                        elapsed = round(time.time() - start_t, 2)
                        results.append({"file": display_name, "status": "ok", "rows": total_rows, "table": table, "elapsed_s": elapsed, "error": None})
                    else:
                        with zipfile.ZipFile(tmp_path) as zf:
                            content_bytes = zf.read(name)
                        result = process_file(filename, content_bytes)
                        result["elapsed_s"] = round(time.time() - start_t, 2)
                        results.append(result)
                        del content_bytes
                except Exception:
                    elapsed = round(time.time() - start_t, 2)
                    log.exception("ZIP: [%s/%s] ERRO %s", idx + 1, total_files, filename)
                    results.append({"file": filename, "status": "error", "rows": 0, "table": "", "elapsed_s": elapsed, "error": traceback.format_exc()})
        else:
            # Paralelo: cada worker processa um arquivo (abre o ZIP sozinho)
            task_args = [(tmp_path, idx, name) for idx, name in enumerate(names)]
            results_by_idx = {}
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_process_one_zip_entry, a): a[1] for a in task_args}
                for future in as_completed(futures):
                    idx, result = future.result()
                    results_by_idx[idx] = result
                    log.info("ZIP: concluído [%s/%s] %s", idx + 1, total_files, result.get("file", ""))
            results = [results_by_idx[i] for i in range(len(names))]

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

    ok    = [r for r in results if r["status"] == "ok"]
    error = [r for r in results if r["status"] != "ok"]
    log.info("process-storage: concluído path=%s — total=%s, sucesso=%s, falhas=%s", path, len(results), len(ok), len(error))
    return {"total": len(results), "success": len(ok), "failed": len(error), "results": results}


@app.post("/load-from-gcs")
async def load_from_gcs(body: dict):
    """
    Carrega CSV do GCS (fluxo principal para grandes volumes).
    Stream → remove ASCII 0 → valida colunas → lotes em Parquet → load job no BigQuery.
    Rápido e com suporte a milhões de linhas. Ao final, use POST /setup-enriquecida para a view unificada.
    """
    path            = body.get("path", "").strip()
    table_type      = (body.get("table_type") or "generico").strip().lower()
    replace_pedidos = body.get("replace_pedidos", False)
    if not path:
        raise HTTPException(400, "Campo 'path' obrigatório.")
    if table_type not in ("pedidos", "produtos", "clientes", "generico"):
        table_type = "generico"
    filename = path.split("/")[-1] if "/" in path else path
    if "_" in filename and filename.count("_") >= 2:
        filename = filename.split("_", 1)[1]

    log.info("load-from-gcs (CSV): path=%s, filename=%s, table_type=%s", path, filename, table_type)
    try:
        bq = get_bq_client()
        total_rows, table_detected = stream_from_gcs_clean_and_load(
            CONFIG["bucket"],
            path,
            filename,
            table_type,
            bq,
            replace_pedidos,
        )
        table_res = table_detected or table_type
        log.info("load-from-gcs: concluído path=%s — %s linhas, tabela=%s", path, total_rows, table_res)
        return {
            "status": "ok",
            "rows": total_rows,
            "table": table_res,
            "enrichment_hint": "Após carregar pedidos, produtos e clientes, chame POST /setup-enriquecida para a view unificada (enriquecimento).",
        }
    except Exception as e:
        log.exception("load-from-gcs: ERRO path=%s — %s", path, e)
        traceback.print_exc()
        return {"status": "error", "message": str(e), "table": table_type}


@app.post("/setup-enriquecida")
async def setup_enriquecida():
    """
    Cria/atualiza a view unificada pedidos_enriquecida (enriquecimento).
    Junta pedidos + produtos + clientes em uma única tabela para uso na plataforma.
    Chamar após o carregamento dos CSVs de pedidos, produtos e clientes.
    """
    try:
        bq  = get_bq_client()
        ds  = CONFIG["dataset"]
        pid = CONFIG["project_id"]
        for tbl in ("pedidos", "produtos", "clientes"):
            try:
                bq.get_table(f"{pid}.{ds}.{tbl}")
            except Exception:
                return {
                    "status": "error",
                    "message": f"Tabela '{tbl}' não existe. Suba os CSVs de pedidos, produtos e clientes (load-from-gcs) antes de criar a view."
                }
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
        log.info("Enriquecimento: view %s.pedidos_enriquecida criada/atualizada (pedidos + produtos + clientes)", ds)
        return {"status": "ok", "message": f"View {ds}.pedidos_enriquecida criada/atualizada. Use esta tabela unificada na plataforma."}
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
      ⚡ Enviar para BigQuery
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

    // ── ETAPA 2: Upload direto para GCS via XHR (progresso real) ──
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