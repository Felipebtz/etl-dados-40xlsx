"""
Supabase Bulk XLSX Uploader
Detecta automaticamente o tipo de arquivo (pedidos / produtos / clientes)
e insere na tabela correta via conexão direta PostgreSQL (psycopg2 + COPY).
"""

import asyncio
import gzip
import io
import os
import re
import tempfile
import time
import traceback
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import httpx
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
# SUPABASE STORAGE — para upload direto do browser e download pelo backend
# Variáveis de ambiente obrigatórias para o fluxo Storage:
#   SUPABASE_URL    = https://<projeto>.supabase.co
#   SUPABASE_KEY    = service_role key (Settings → API)
#   STORAGE_BUCKET  = nome do bucket (ex: "etl-uploads")
# ──────────────────────────────────────────────────────────────
STORAGE = {
    "url":    os.getenv("SUPABASE_URL", ""),
    "key":    os.getenv("SUPABASE_KEY", ""),
    "bucket": os.getenv("STORAGE_BUCKET", "etl-uploads"),
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


# Tamanho do lote de linhas processado por vez (RAM controlada)
ROWS_PER_BATCH = int(os.getenv("ROWS_PER_BATCH", "50000"))  # ~50k linhas por vez


def _clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Remove colunas Unnamed, normaliza vazios para None."""
    df = df.loc[:, ~df.columns.str.match(r"^Unnamed:\s*\d+$", na=False)]
    return df.replace({"": None, "nan": None, "NaN": None, "None": None, "NULL": None})


def _detect_csv_params(raw_content: bytes) -> tuple[str, str]:
    """Detecta encoding e separador lendo só as primeiras linhas do CSV."""
    sample = raw_content[:65536]  # primeiros 64 KB
    for encoding in CSV_ENCODINGS:
        for sep in CSV_SEPARATORS:
            try:
                df = pd.read_csv(
                    io.BytesIO(sample),
                    sep=sep,
                    encoding=encoding,
                    on_bad_lines="skip",
                    dtype=str,
                    keep_default_na=False,
                    nrows=5,
                )
                if len(df.columns) >= 2:
                    return encoding, sep
            except Exception:
                continue
    raise ValueError("Não foi possível detectar encoding/separador do CSV.")


def stream_csv(raw_content: bytes, filename: str, conn, table_hint: str | None = None):
    """
    Lê CSV em lotes de ROWS_PER_BATCH linhas — nunca carrega tudo na RAM.
    Retorna (table, total_rows).
    """
    encoding, sep = _detect_csv_params(raw_content)
    reader = pd.read_csv(
        io.BytesIO(raw_content),
        sep=sep,
        encoding=encoding,
        on_bad_lines="skip",
        dtype=str,
        keep_default_na=False,
        chunksize=ROWS_PER_BATCH,  # ← streaming: 1 lote de cada vez
    )

    table      = None
    total_rows = 0
    table_created = False

    for batch in reader:
        batch = _clean_chunk(batch)
        batch = batch.dropna(how="all")
        if batch.empty:
            continue

        # Detecta tabela no primeiro lote (colunas são sempre as mesmas)
        if table is None:
            table = table_hint or detect_table(list(batch.columns))

        batch["_source_file"] = filename
        batch = align_df(batch, table)

        # Cria tabela só uma vez
        if not table_created:
            schema_def = SCHEMAS.get(table, {}).get("columns", [])
            if schema_def:
                ensure_table(conn, table, schema_def=schema_def)
            else:
                ensure_table(conn, table, columns_from_df=list(batch.columns))
            table_created = True

        bulk_insert(conn, table, batch, CONFIG["chunk_size"])
        total_rows += len(batch)
        del batch  # libera RAM do lote imediatamente

    return table or "generico", total_rows


def stream_xlsx(raw_content: bytes, filename: str, conn, table_hint: str | None = None):
    """
    Lê XLSX linha por linha com openpyxl (read_only=True) em lotes de ROWS_PER_BATCH.
    Nunca carrega a planilha inteira na RAM.
    Retorna (table, total_rows).
    """
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(raw_content), read_only=True, data_only=True)
    ws = wb.active

    table      = None
    total_rows = 0
    table_created = False
    headers    = None
    batch_rows = []

    def flush_batch(batch_rows, headers):
        nonlocal table, table_created, total_rows
        df = pd.DataFrame(batch_rows, columns=headers, dtype=str)
        df = _clean_chunk(df)
        df = df.dropna(how="all")
        if df.empty:
            return

        if table is None:
            table = table_hint or detect_table(list(df.columns))

        df["_source_file"] = filename
        df = align_df(df, table)

        if not table_created:
            schema_def = SCHEMAS.get(table, {}).get("columns", [])
            if schema_def:
                ensure_table(conn, table, schema_def=schema_def)
            else:
                ensure_table(conn, table, columns_from_df=list(df.columns))
            table_created = True

        bulk_insert(conn, table, df, CONFIG["chunk_size"])
        total_rows += len(df)
        del df

    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            # Primeira linha = cabeçalho
            headers = [str(c) if c is not None else f"col_{j}" for j, c in enumerate(row)]
            continue

        batch_rows.append([str(v) if v is not None else None for v in row])

        if len(batch_rows) >= ROWS_PER_BATCH:
            flush_batch(batch_rows, headers)
            batch_rows = []  # libera RAM

    # Último lote
    if batch_rows:
        flush_batch(batch_rows, headers)

    wb.close()
    return table or "generico", total_rows


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
# PROCESSAMENTO DE UM ARQUIVO — streaming por lotes
# Nunca carrega o arquivo inteiro na RAM
# ──────────────────────────────────────────────────────────────
def process_file(filename: str, content: bytes) -> dict:
    start  = time.time()
    result = {"file": filename, "status": "ok", "rows": 0, "table": "", "elapsed_s": 0, "error": None}

    try:
        # Descompactar gzip se necessário
        real_filename = filename
        raw_content   = content
        if filename.lower().endswith(".gz"):
            raw_content   = gzip.decompress(content)
            real_filename = filename[:-3]
            result["file"] = real_filename

        conn = get_conn()
        try:
            if real_filename.lower().endswith(".csv"):
                # CSV: streaming com pd.read_csv(chunksize=N)
                table, total_rows = stream_csv(raw_content, real_filename, conn)
            else:
                # XLSX/XLS: streaming linha por linha com openpyxl read_only
                table, total_rows = stream_xlsx(raw_content, real_filename, conn)

            result["table"] = table
            result["rows"]  = total_rows
        finally:
            conn.close()

        del raw_content  # libera RAM

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


# ──────────────────────────────────────────────────────────────
# STORAGE: gera URL assinada para upload direto do browser
# O browser faz PUT direto no Supabase Storage (sem passar pelo Railway)
# ──────────────────────────────────────────────────────────────
@app.post("/storage-token")
async def storage_token(body: dict):
    """
    Retorna uma signed URL para o browser fazer PUT direto no Supabase Storage.
    Body: { "filename": "dados.zip" }
    """
    filename = body.get("filename", "upload.zip")
    path     = f"uploads/{filename}"
    url      = f"{STORAGE['url']}/storage/v1/object/{STORAGE['bucket']}/{path}"
    headers  = {
        "Authorization": f"Bearer {STORAGE['key']}",
        "Content-Type":  "application/json",
    }
    # Cria signed URL de upload (válida por 1 hora)
    sign_url = f"{STORAGE['url']}/storage/v1/object/sign/{STORAGE['bucket']}/{path}"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(sign_url, headers=headers, json={"expiresIn": 3600})
    if r.status_code not in (200, 201):
        raise HTTPException(500, f"Erro ao gerar signed URL: {r.text}")
    data = r.json()
    signed_url = data.get("signedURL") or data.get("signedUrl") or data.get("url")
    if not signed_url:
        raise HTTPException(500, f"Resposta inesperada do Storage: {data}")
    # Prefixar com a URL base se vier relativo
    if signed_url.startswith("/"):
        signed_url = STORAGE["url"] + signed_url
    return {"upload_url": signed_url, "path": path}


# ──────────────────────────────────────────────────────────────
# PROCESS-STORAGE: Railway baixa o ZIP do Storage e processa
# Chamado pelo browser APÓS o upload para o Storage concluir
# ──────────────────────────────────────────────────────────────
@app.post("/process-storage")
async def process_storage(body: dict):
    """
    Baixa o ZIP do Supabase Storage e processa arquivo por arquivo.
    Body: { "path": "uploads/dados.zip", "replace_pedidos": false }
    """
    path            = body.get("path", "")
    replace_pedidos = body.get("replace_pedidos", False)

    if not path:
        raise HTTPException(400, "Campo 'path' obrigatório.")

    # ── Baixar o ZIP do Storage em streaming para arquivo temporário ──
    download_url = f"{STORAGE['url']}/storage/v1/object/{STORAGE['bucket']}/{path}"
    headers      = {"Authorization": f"Bearer {STORAGE['key']}"}

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp_path = tmp.name
            async with httpx.AsyncClient(timeout=600) as client:
                async with client.stream("GET", download_url, headers=headers) as resp:
                    if resp.status_code != 200:
                        raise HTTPException(500, f"Erro ao baixar do Storage: {resp.status_code}")
                    async for chunk in resp.aiter_bytes(chunk_size=8 * 1024 * 1024):  # 8 MB chunks
                        tmp.write(chunk)

        # ── Verificar ZIP ──
        if not zipfile.is_zipfile(tmp_path):
            raise HTTPException(400, "Arquivo no Storage não é um ZIP válido.")

        # ── Truncar pedidos se solicitado ──
        if replace_pedidos:
            conn = get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE public.pedidos RESTART IDENTITY CASCADE")
                conn.commit()
            finally:
                conn.close()

        # ── Processar arquivo por arquivo dentro do ZIP ──
        results = []
        with zipfile.ZipFile(tmp_path) as zf:
            names = [
                n for n in zf.namelist()
                if not n.startswith("__MACOSX")
                and not os.path.basename(n).startswith(".")
                and n.lower().endswith((".xlsx", ".xls", ".csv"))
            ]
            if not names:
                raise HTTPException(400, "Nenhum XLSX/CSV encontrado no ZIP.")

            # Processar sequencialmente para economizar RAM (14 GB = cuidado)
            for name in names:
                filename      = os.path.basename(name)
                content_bytes = zf.read(name)
                result        = process_file(filename, content_bytes)
                results.append(result)
                del content_bytes  # liberar RAM imediatamente

    finally:
        # Remover arquivo temporário sempre
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

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
    try:
        conn = get_conn()
        conn.close()
        db_ok = True
    except Exception as e:
        db_ok = str(e)
    return {"status": "ok", "db_connected": db_ok}


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
    :root { --bg:#0d0d12; --panel:#13131a; --border:#1e2030; --accent:#3ecf8e; --red:#f87171; --muted:#52526e; --text:#e0e0f0; }
    *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
    body{background:var(--bg);font-family:'IBM Plex Sans',sans-serif;color:var(--text);min-height:100vh}
    .mono{font-family:'IBM Plex Mono',monospace}
    body::before{content:'';position:fixed;inset:0;pointer-events:none;z-index:0;
      background-image:linear-gradient(rgba(62,207,142,.03) 1px,transparent 1px),linear-gradient(90deg,rgba(62,207,142,.03) 1px,transparent 1px);
      background-size:40px 40px}
    .z1{position:relative;z-index:1}
    #drop-zone{border:2px dashed var(--border);border-radius:16px;transition:border-color .2s,background .2s;cursor:pointer}
    #drop-zone.over{border-color:var(--accent);background:rgba(62,207,142,.04)}
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
      <h1 class="text-xl font-semibold tracking-tight">Supabase Bulk Uploader</h1>
      <span class="mono text-xs px-2 py-0.5 rounded border" style="border-color:#1d6648;color:#3ecf8e;background:rgba(62,207,142,.07)">v4.0</span>
    </div>
    <p class="text-sm" style="color:var(--muted)">
      Upload de <strong style="color:var(--text)">ZIPs grandes</strong> direto no Supabase Storage —
      detecção automática (<span style="color:#93c5fd">pedidos</span> /
      <span style="color:#fbbf24">produtos</span> /
      <span style="color:#c4b5fd">clientes</span>).
    </p>
  </div>

  <!-- ETAPAS -->
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
      <p class="text-sm" style="color:var(--muted)">Qualquer tamanho — sobe direto no Supabase Storage</p>
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

// ── DRAG & DROP ──
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
</script>
</body>
</html>"""

@app.get("/", response_class=HTMLResponse)
def frontend():
    return HTMLResponse(content=_HTML)