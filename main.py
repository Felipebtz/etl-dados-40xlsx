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
@app.post("/upload")
@app.post("/api/upload")  # mesma rota para Vercel (api/upload.py) e local
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
    :root {
      --bg:     #0d0d12;
      --panel:  #13131a;
      --border: #1e2030;
      --accent: #3ecf8e;   /* verde Supabase */
      --accent2:#1d6648;
      --red:    #f87171;
      --muted:  #52526e;
      --text:   #e0e0f0;
    }
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: var(--bg); font-family: 'IBM Plex Sans', sans-serif; color: var(--text); min-height: 100vh; }
    .mono { font-family: 'IBM Plex Mono', monospace; }

    /* ─── GRID BG ─── */
    body::before {
      content: '';
      position: fixed; inset: 0; pointer-events: none; z-index: 0;
      background-image:
        linear-gradient(rgba(62,207,142,.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(62,207,142,.03) 1px, transparent 1px);
      background-size: 40px 40px;
    }
    .z1 { position: relative; z-index: 1; }

    /* ─── DROP ZONE ─── */
    #drop-zone {
      border: 1.5px dashed var(--border);
      border-radius: 14px;
      transition: border-color .2s, background .2s;
      cursor: pointer;
    }
    #drop-zone.over {
      border-color: var(--accent);
      background: rgba(62,207,142,.04);
    }
    #file-input { display: none; }

    /* ─── CHIP ─── */
    .chip {
      display: flex; align-items: center; gap: 8px;
      background: var(--panel); border: 1px solid var(--border);
      border-radius: 8px; padding: 6px 10px; font-size: 12px;
      animation: up .15s ease;
    }
    .chip .x { margin-left: auto; cursor: pointer; color: var(--muted); transition: color .15s; flex-shrink:0; }
    .chip .x:hover { color: var(--red); }

    /* ─── TABLE TYPE BADGE ─── */
    .type-badge {
      font-size: 10px; font-weight: 600; padding: 1px 7px;
      border-radius: 99px; flex-shrink: 0;
    }
    .type-pedidos  { background: rgba(59,130,246,.15); color: #93c5fd; }
    .type-produtos { background: rgba(251,191,36,.12);  color: #fbbf24; }
    .type-clientes { background: rgba(167,139,250,.12); color: #c4b5fd; }
    .type-generico { background: rgba(107,114,128,.12); color: #9ca3af; }

    /* ─── PROGRESS ─── */
    .bar-wrap { height: 3px; background: var(--border); border-radius: 99px; overflow: hidden; }
    .bar-fill  { height: 100%; background: var(--accent); border-radius: 99px; transition: width .4s ease; }

    /* ─── RESULT ROW ─── */
    .res-row {
      display: grid; grid-template-columns: 1fr auto auto auto;
      gap: 8px; align-items: center;
      padding: 8px 12px; border-radius: 8px;
      background: var(--panel); border: 1px solid var(--border);
      font-size: 13px; animation: up .2s ease;
    }
    .badge { padding: 2px 8px; border-radius: 99px; font-size: 11px; font-weight: 600; }
    .b-ok  { background: rgba(62,207,142,.12); color: var(--accent); }
    .b-err { background: rgba(248,113,113,.12); color: var(--red); }

    /* ─── STAT CARD ─── */
    .stat { padding: 18px; border-radius: 12px; text-align: center; border: 1px solid var(--border); background: var(--panel); }

    .scroll { max-height: 260px; overflow-y: auto; scrollbar-width: thin; scrollbar-color: var(--border) transparent; }
    .scroll::-webkit-scrollbar { width: 4px; }
    .scroll::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }

    @keyframes up { from { opacity:0; transform:translateY(5px); } to { opacity:1; transform:translateY(0); } }

    .btn-primary {
      background: var(--accent); color: #041f12; font-weight: 600;
      border-radius: 10px; padding: 12px 0; width: 100%; font-size: 14px;
      transition: opacity .15s, transform .1s; cursor: pointer; border: none;
    }
    .btn-primary:hover:not(:disabled) { opacity: .9; transform: translateY(-1px); }
    .btn-primary:disabled { opacity: .35; cursor: not-allowed; transform: none; }
    .pulse { animation: pulse 1.1s infinite; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
  </style>
</head>
<body class="flex flex-col items-center py-14 px-4">

  <!-- HEADER -->
  <div class="z1 w-full max-w-3xl mb-8">
    <div class="flex items-center gap-3 mb-2">
      <svg viewBox="0 0 24 24" class="w-7 h-7" fill="none" stroke="#3ecf8e" stroke-width="1.8">
        <path d="M4 7h16M4 12h10M4 17h7"/>
        <circle cx="19" cy="17" r="3"/>
        <path d="m21 19-1.5-1.5"/>
      </svg>
      <h1 class="text-xl font-semibold tracking-tight">Supabase Bulk Uploader</h1>
      <span class="mono text-xs px-2 py-0.5 rounded border" style="border-color:#1d6648;color:#3ecf8e;background:rgba(62,207,142,.07)">v2.0</span>
    </div>
    <p class="text-sm" style="color:var(--muted)">
      Envie até <strong style="color:var(--text)">40 arquivos XLSX ou CSV</strong> —
      detecção automática do tipo (<span style="color:#93c5fd">pedidos</span> /
      <span style="color:#fbbf24">produtos</span> /
      <span style="color:#c4b5fd">clientes</span>) e insert direto no Supabase.
    </p>
  </div>

  <!-- CONFIG (hidden) -->
  <div class="z1 w-full max-w-3xl mb-5 p-4 rounded-xl border" style="background:var(--panel);border-color:var(--border)">
    <p class="mono text-xs uppercase tracking-widest mb-3" style="color:var(--muted)">Conexão</p>
    <div class="grid grid-cols-1 gap-3">
      <div>
        <label class="block text-xs mb-1" style="color:var(--muted)">API URL do backend</label>
        <input id="api-url" type="text" placeholder="deixe vazio para usar este servidor"
          class="mono w-full text-sm rounded-lg px-3 py-2 border outline-none transition"
          style="background:#0d0d12;border-color:var(--border);color:var(--text)"
          onfocus="this.style.borderColor='#3ecf8e'" onblur="this.style.borderColor='var(--border)'" />
      </div>
    </div>
  </div>

  <!-- DROP ZONE -->
  <div id="drop-zone" class="z1 w-full max-w-3xl py-14 flex flex-col items-center gap-3 mb-4"
       onclick="document.getElementById('file-input').click()">
    <input type="file" id="file-input" multiple accept=".xlsx,.xls,.csv" />
    <svg class="w-10 h-10" viewBox="0 0 24 24" fill="none" stroke-width="1.4" style="color:var(--muted)" stroke="currentColor">
      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
      <polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/>
    </svg>
    <p class="text-sm font-medium">Arraste os XLSXs aqui</p>
    <p class="text-xs" style="color:var(--muted)">clique para selecionar · .xlsx .xls .csv · máx 40 arquivos</p>
  </div>

  <!-- CONTADOR + LIMPAR -->
  <div class="z1 w-full max-w-3xl flex justify-between items-center mb-2 px-1">
    <span id="file-count" class="mono text-xs" style="color:var(--muted)">0 arquivos</span>
    <button onclick="clearAll()" class="text-xs px-3 py-1 rounded-lg border transition"
      style="border-color:var(--border);color:var(--muted)"
      onmouseover="this.style.color='var(--red)';this.style.borderColor='rgba(248,113,113,.4)'"
      onmouseout="this.style.color='var(--muted)';this.style.borderColor='var(--border)'">Limpar</button>
  </div>

  <!-- LISTA DE ARQUIVOS -->
  <div id="file-list" class="z1 scroll w-full max-w-3xl flex flex-col gap-2 mb-5"></div>

  <!-- OPÇÃO: TROCA DIÁRIA DE PEDIDOS -->
  <div class="z1 w-full max-w-3xl mb-3 flex items-center gap-2">
    <input type="checkbox" id="replace-pedidos" class="rounded border" style="border-color:var(--border);accent-color:var(--accent)" />
    <label for="replace-pedidos" class="text-sm" style="color:var(--text)">Substituir pedidos (troca diária — apaga a tabela antes de inserir)</label>
  </div>

  <!-- BOTÃO -->
  <div class="z1 w-full max-w-3xl mb-6">
    <button id="upload-btn" class="btn-primary" disabled onclick="startUpload()">
      ⚡ Enviar para Supabase
    </button>
  </div>

  <!-- BASE ENRIQUECIDA (após subir as bases) -->
  <div class="z1 w-full max-w-3xl mb-6 p-4 rounded-xl border" style="background:var(--panel);border-color:var(--border)">
    <p class="mono text-xs uppercase tracking-widest mb-2" style="color:var(--muted)">Base enriquecida</p>
    <p class="text-sm mb-3" style="color:var(--text)">Depois de subir Clientes, Pedidos e Produtos: crie a view que cruza as 3 tabelas (SKU → produtos, single_id → clientes).</p>
    <button type="button" onclick="setupEnriquecida()" class="px-4 py-2 rounded-lg text-sm font-medium border transition" style="border-color:var(--accent);color:var(--accent);background:rgba(62,207,142,.08)" onmouseover="this.style.background='rgba(62,207,142,.15)'" onmouseout="this.style.background='rgba(62,207,142,.08)'">Criar/atualizar view pedidos_enriquecida</button>
    <span id="enriquecida-msg" class="ml-2 text-sm"></span>
  </div>

  <!-- PROGRESSO -->
  <div id="prog-sec" class="z1 w-full max-w-3xl mb-5 hidden">
    <div class="flex justify-between text-xs mb-2" style="color:var(--muted)">
      <span id="prog-label" class="pulse">Enviando…</span>
      <span id="prog-pct" class="mono">0%</span>
    </div>
    <div class="bar-wrap"><div id="prog-fill" class="bar-fill" style="width:0%"></div></div>
  </div>

  <!-- SUMÁRIO -->
  <div id="summary" class="z1 w-full max-w-3xl mb-4 hidden">
    <div class="grid grid-cols-3 gap-3 mb-5">
      <div class="stat"><p class="mono text-2xl font-semibold" id="s-total">0</p><p class="text-xs mt-1" style="color:var(--muted)">Total</p></div>
      <div class="stat" style="border-color:rgba(62,207,142,.3)"><p class="mono text-2xl font-semibold" id="s-ok" style="color:var(--accent)">0</p><p class="text-xs mt-1" style="color:var(--muted)">Sucesso</p></div>
      <div class="stat" style="border-color:rgba(248,113,113,.3)"><p class="mono text-2xl font-semibold text-red-400" id="s-err">0</p><p class="text-xs mt-1" style="color:var(--muted)">Falhas</p></div>
    </div>
  </div>

  <!-- RESULTADOS -->
  <div id="results-sec" class="z1 w-full max-w-3xl hidden">
    <p class="mono text-xs uppercase tracking-widest mb-3" style="color:var(--muted)">Detalhe por arquivo</p>
    <div id="results-list" class="scroll flex flex-col gap-2"></div>
  </div>

<script>
let selectedFiles = [];
const TABLE_COLORS = { pedidos:'type-pedidos', produtos:'type-produtos', clientes:'type-clientes', generico:'type-generico' };

// ── DRAG & DROP ──────────────────────────────────────────────
const zone = document.getElementById('drop-zone');
zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('over'); });
zone.addEventListener('dragleave', () => zone.classList.remove('over'));
zone.addEventListener('drop', e => { e.preventDefault(); zone.classList.remove('over'); addFiles([...e.dataTransfer.files]); });
document.getElementById('file-input').addEventListener('change', e => { addFiles([...e.target.files]); e.target.value=''; });

function addFiles(files) {
  const xlsx = files.filter(f => f.name.match(/\\.(xlsx|xls|csv)$/i));
  for (const f of xlsx) {
    if (selectedFiles.length >= 40) { alert('Máximo de 40 arquivos!'); break; }
    if (!selectedFiles.find(x => x.name === f.name)) selectedFiles.push(f);
  }
  render();
}
function removeFile(name) { selectedFiles = selectedFiles.filter(f => f.name !== name); render(); }
function clearAll() {
  selectedFiles = [];
  render();
  ['summary','results-sec','prog-sec'].forEach(id => document.getElementById(id).classList.add('hidden'));
}

// Detecta tipo do arquivo pelo nome (rápido, sem ler o xlsx)
function guessType(name) {
  // CSV herda o mesmo tipo pelo nome
  const n = name.toLowerCase();
  if (/pedido|order/i.test(n))   return 'pedidos';
  if (/sku|produto|product/i.test(n)) return 'produtos';
  if (/cliente|customer/i.test(n)) return 'clientes';
  return '?';
}

function render() {
  const list = document.getElementById('file-list');
  list.innerHTML = '';
  for (const f of selectedFiles) {
    const tipo = guessType(f.name);
    const tc   = TABLE_COLORS[tipo] || 'type-generico';
    const size = (f.size/1024).toFixed(0) + ' KB';
    const chip = document.createElement('div');
    chip.className = 'chip';
    chip.innerHTML = `
      <svg class="w-4 h-4 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="#3ecf8e" stroke-width="2">
        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
        <polyline points="14 2 14 8 20 8"/>
      </svg>
      <span class="truncate">${f.name}</span>
      <span class="type-badge ${tc}">${tipo}</span>
      <span class="mono flex-shrink-0" style="color:var(--muted)">${size}</span>
      <span class="x" onclick="removeFile('${f.name}')">✕</span>
    `;
    list.appendChild(chip);
  }
  document.getElementById('file-count').textContent = `${selectedFiles.length} arquivo(s)`;
  document.getElementById('upload-btn').disabled = selectedFiles.length === 0;
}

// ── COMPRESSÃO GZIP (browser nativo via CompressionStream) ──
async function compressFile(file) {
  const buffer = await file.arrayBuffer();
  const stream = new Blob([buffer]).stream();
  const compressed = stream.pipeThrough(new CompressionStream('gzip'));
  const chunks = [];
  const reader = compressed.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  const compressedBlob = new Blob(chunks, { type: 'application/gzip' });
  // Retorna novo File com .gz no nome para o backend identificar
  return new File([compressedBlob], file.name + '.gz', { type: 'application/gzip' });
}

// ── UPLOAD COM COMPRESSÃO ────────────────────────────────────
async function startUpload() {
  const btn = document.getElementById('upload-btn');
  btn.disabled = true; btn.textContent = '⏳ Comprimindo…';

  const ps = document.getElementById('prog-sec');
  ps.classList.remove('hidden');
  setProgress(5);

  // 1. Comprimir todos os arquivos em paralelo
  const label = document.getElementById('prog-label');
  label.textContent = 'Comprimindo arquivos…';
  label.classList.add('pulse');

  let compressed;
  try {
    compressed = await Promise.all(selectedFiles.map(compressFile));
  } catch (err) {
    label.textContent = '✗ Erro ao comprimir: ' + err.message;
    label.classList.remove('pulse');
    label.style.color = 'var(--red)';
    btn.disabled = false; btn.textContent = '⚡ Enviar para Supabase';
    return;
  }

  // Mostrar redução de tamanho
  const originalSize  = selectedFiles.reduce((s, f) => s + f.size, 0);
  const compressedSize = compressed.reduce((s, f) => s + f.size, 0);
  const reduction = Math.round((1 - compressedSize / originalSize) * 100);
  label.textContent = `Enviando… (comprimido ${reduction}% menor)`;
  setProgress(30);

  // 2. Enviar em lotes de 10 arquivos para não estourar o limite da Vercel
  const BATCH = 10;
  const allResults = [];
  const url = window.location.origin + '/upload';

  try {
    const replacePedidos = document.getElementById('replace-pedidos').checked;
    for (let i = 0; i < compressed.length; i += BATCH) {
      const batch = compressed.slice(i, i + BATCH);
      const form  = new FormData();
      for (const f of batch) form.append('files', f);
      form.append('replace_pedidos', (i === 0 && replacePedidos) ? 'true' : 'false');

      label.textContent = `Enviando lote ${Math.floor(i/BATCH)+1} de ${Math.ceil(compressed.length/BATCH)}… (comprimido ${reduction}% menor)`;
      setProgress(30 + Math.round((i / compressed.length) * 60));

      const res = await fetch(url, { method: 'POST', body: form });
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
      const data = await res.json();
      allResults.push(...data.results);
    }

    setProgress(100);
    label.textContent = '✓ Concluído';
    label.classList.remove('pulse');
    label.style.color = 'var(--accent)';

    const ok  = allResults.filter(r => r.status === 'ok').length;
    const err = allResults.filter(r => r.status !== 'ok').length;
    showResults({ total: allResults.length, success: ok, failed: err, results: allResults });

  } catch (err) {
    label.textContent = '✗ ' + err.message;
    label.classList.remove('pulse');
    label.style.color = 'var(--red)';
  }

  btn.disabled = false; btn.textContent = '⚡ Enviar para Supabase';
}

function setProgress(p) {
  document.getElementById('prog-fill').style.width = p + '%';
  document.getElementById('prog-pct').textContent  = p + '%';
}

async function setupEnriquecida() {
  const el = document.getElementById('enriquecida-msg');
  el.textContent = '…';
  el.style.color = 'var(--muted)';
  try {
    const res = await fetch(window.location.origin + '/setup-enriquecida', { method: 'POST' });
    const data = await res.json();
    if (data.status === 'ok') {
      el.textContent = '✓ ' + (data.message || 'View criada.');
      el.style.color = 'var(--accent)';
    } else {
      el.textContent = '✗ ' + (data.message || 'Erro');
      el.style.color = 'var(--red)';
    }
  } catch (err) {
    el.textContent = '✗ ' + err.message;
    el.style.color = 'var(--red)';
  }
}

function showResults(data) {
  document.getElementById('s-total').textContent = data.total;
  document.getElementById('s-ok').textContent    = data.success;
  document.getElementById('s-err').textContent   = data.failed;
  document.getElementById('summary').classList.remove('hidden');

  const list = document.getElementById('results-list');
  list.innerHTML = '';
  for (const r of data.results) {
    const row = document.createElement('div');
    row.className = 'res-row';
    const tc = TABLE_COLORS[r.table] || 'type-generico';
    const badge = r.status === 'ok'
      ? '<span class="badge b-ok">OK</span>'
      : '<span class="badge b-err">ERRO</span>';
    const tbl  = r.table ? `<span class="type-badge ${tc}">${r.table}</span>` : '';
    const rows = r.rows  ? `<span class="mono text-xs" style="color:var(--muted)">${r.rows} linhas</span>` : '';
    const time = `<span class="mono text-xs" style="color:var(--muted)">${r.elapsed_s}s</span>`;

    row.innerHTML = `<span class="truncate text-sm">${r.file}</span>${tbl}${rows}${time}${badge}`;

    if (r.error) {
      const wrap = document.createElement('div');
      wrap.style.cssText = 'display:block';
      wrap.innerHTML = `<div style="display:grid;grid-template-columns:1fr auto auto auto;gap:8px;align-items:center">
        <span class="truncate text-sm">${r.file}</span>${tbl}${rows}${time}${badge}</div>`;
      const detail = document.createElement('pre');
      detail.style.cssText = 'margin-top:6px;font-size:11px;color:var(--red);white-space:pre-wrap;overflow-x:auto;padding:8px;background:#1a0808;border-radius:6px';
      detail.textContent = r.error;
      wrap.appendChild(detail);
      list.appendChild(wrap);
      continue;
    }
    list.appendChild(row);
  }
  document.getElementById('results-sec').classList.remove('hidden');
}
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def frontend():
    return HTMLResponse(content=_HTML)