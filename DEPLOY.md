# Deploy com .env (cPanel / Linux / nuvem)

Use este guia depois do `git clone` para subir o projeto com variáveis de ambiente em um servidor (cPanel, VPS, Railway, Render, etc.).

---

## 1. Clonar e entrar na pasta

```bash
git clone https://github.com/SEU_USUARIO/etl-python.git
cd etl-python
```

---

## 2. Criar e preencher o .env

```bash
cp .env.example .env
nano .env   # ou vim, ou edite pelo cPanel
```

Preencha pelo menos:

- **DATABASE_URL** — connection string do PostgreSQL (Supabase). Use o **Pooler** (porta 6543). Se a senha tiver `@`, troque por `%40`.
- Opcional: **MAX_WORKERS**, **CHUNK_SIZE**, **SUPABASE_URL**, **SUPABASE_KEY**.

Exemplo (ficcional):

```env
DATABASE_URL=postgresql://postgres.abc123:MinhaS3nha%40@aws-0-sa-east-1.pooler.supabase.com:6543/postgres?sslmode=require
MAX_WORKERS=8
CHUNK_SIZE=1000
```

Salve e feche. **Não commite o .env** (ele já está no .gitignore).

---

## 3. Ambiente Python e dependências

### Opção A — Terminal (SSH / Linux)

```bash
python3 -m venv venv
source venv/bin/activate   # Linux/Mac
# No Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Opção B — cPanel “Setup Python App”

1. Em cPanel, abra **Setup Python App** (ou **Application Manager**).
2. **Create Application**.
3. Python version: **3.10** ou **3.12**.
4. Application root: pasta do projeto (ex.: `etl-python` ou `public_html/etl-python`).
5. Application startup file: deixe em branco ou use o comando abaixo no “Startup command”.
6. Em **Configuration files** ou **Environment variables**, adicione as variáveis do `.env` (uma a uma: `DATABASE_URL`, etc.), ou aponte para o `.env` se o painel permitir.

---

## 4. Rodar o app

O app é FastAPI e usa `main:app`. Comando:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

- **0.0.0.0** — aceita acesso externo.
- **8000** — troque se o cPanel/servidor usar outra porta (ex.: 5000, 8080).

### No cPanel “Setup Python App”

- Em **Startup command** (ou equivalente), use:
  ```bash
  uvicorn main:app --host 0.0.0.0 --port $PORT
  ```
  Se o cPanel passar a porta por variável (ex.: `PORT=5000`), use `$PORT`. Caso contrário, use a porta que o painel indicar (ex.: `--port 5000`).

### Deixar rodando em segundo plano (Linux sem cPanel)

```bash
nohup uvicorn main:app --host 0.0.0.0 --port 8000 &
```

Ou com **systemd**: crie um serviço que rode esse comando na pasta do projeto e com o mesmo usuário que tem o `.env`.

---

## 5. Acessar

- **Local:** http://localhost:8000
- **Servidor:** http://SEU_IP:8000 ou o domínio que o cPanel tiver configurado (ex.: https://seudominio.com se tiver proxy reverso para a porta do app).

A primeira rota é a interface de upload (GET /). O upload vai em POST /upload e usa o `DATABASE_URL` do `.env`.

---

## 6. Checklist rápido

| Passo              | Comando / ação |
|--------------------|----------------|
| Clone              | `git clone ...` e `cd etl-python` |
| .env               | `cp .env.example .env` e preencher `DATABASE_URL` (e outras se quiser) |
| Venv               | `python3 -m venv venv` e `source venv/bin/activate` |
| Dependências       | `pip install -r requirements.txt` |
| Rodar              | `uvicorn main:app --host 0.0.0.0 --port 8000` |
| cPanel              | Setup Python App → variáveis de ambiente = conteúdo do .env → startup: comando uvicorn acima |

---

## 7. Limite de upload

Neste servidor **não** vale o limite de 4,5 MB da Vercel. Você pode aumentar o limite do body no FastAPI se precisar (por exemplo para arquivos de dezenas de MB). O front já envia em lotes de 1 arquivo; aqui o tamanho máximo por arquivo depende só do servidor e da configuração do FastAPI.
