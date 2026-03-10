# Deploy no Railway — passo a passo

Use este guia para colocar o projeto **etl-python** no Railway (plano pago) e conectar ao Postgres do Railway ou ao Supabase.

---

## 1. Criar o projeto no Railway

1. Acesse [railway.com](https://railway.com) e faça login.
2. Clique em **New Project**.
3. Escolha **Deploy from GitHub repo**.
4. Conecte o GitHub (se ainda não estiver) e selecione o repositório **etl-python** (ou o nome do seu repo).
5. Railway vai criar um **service** para o app e começar o build.

---

## 2. Postgres no Railway (se for usar o banco do Railway)

Se você já adicionou um **PostgreSQL** no mesmo projeto:

1. No projeto, clique no **service do Postgres**.
2. Aba **Variables** — o Railway já expõe `DATABASE_URL` (e outras).
3. No **service do app** (etl-python), você precisa ter a variável **DATABASE_URL** apontando para esse Postgres.

**Como ligar o app ao Postgres:**

1. Clique no **service do app** (etl-python).
2. Aba **Variables** → **Add variable** ou **Reference**.
3. Se existir **“Add reference”** / **“Variable reference”**, escolha as variáveis do Postgres (por exemplo `DATABASE_URL`). Assim o app usa o mesmo banco.
4. Se não tiver referência, crie uma variável manual:
   - Nome: `DATABASE_URL`
   - Valor: cole a connection string que o Railway mostra no service do Postgres (em Variables, algo como `postgresql://postgres:SENHA@host:5432/railway`).  
   Use o **Private URL** (interno) que o Railway mostrar, não o público, para o app dentro do mesmo projeto.

Se você **não** usa Postgres do Railway e quer continuar com **Supabase**:

- Em **Variables** do service do app, crie `DATABASE_URL` com a connection string do **Supabase** (Pooler, porta 6543).

---

## 3. Variáveis de ambiente no service do app

No **service da aplicação** (não no Postgres):

1. Aba **Variables**.
2. Garanta que exista:
   - **DATABASE_URL** — connection string do banco (Railway Postgres ou Supabase). Se usou “reference” ao Postgres, já pode estar preenchida.
   - Opcional: **MAX_WORKERS** = `8`, **CHUNK_SIZE** = `1000`.

Não é necessário criar `.env` no repo; tudo é configurado nas Variables do Railway.

---

## 4. Build e start (já configurados no repo)

O projeto já tem:

- **railway.json** — comando de start: `uvicorn main:app --host 0.0.0.0 --port $PORT`
- **Procfile** — alternativa: `web: uvicorn main:app ...`
- **requirements.txt** — dependências

O Railway detecta Python, instala com `pip install -r requirements.txt` e usa o comando acima. Não precisa configurar build/start manualmente, a menos que queira mudar.

---

## 5. Domínio público (HTTPS)

1. No **service do app**, vá em **Settings**.
2. Em **Networking**, clique em **Generate Domain** (ou **Settings** → **Networking**).
3. O Railway gera uma URL tipo `https://seu-app.up.railway.app`.
4. Acesse essa URL: a tela do **Supabase Bulk Uploader** deve abrir (GET /).

---

## 6. Resumo do fluxo

| Passo | Onde | O que fazer |
|-------|------|-------------|
| 1 | Railway | New Project → Deploy from GitHub repo → escolher **etl-python** |
| 2 | Service do app | Variables → ter **DATABASE_URL** (referência ao Postgres do Railway ou string do Supabase) |
| 3 | Service do app | Settings → Networking → **Generate Domain** |
| 4 | Navegador | Abrir a URL gerada e testar o upload |

---

## 7. Usar o Postgres que você criou no Railway

Você colou variáveis do Postgres do Railway. Para o **app** usar esse banco:

- No **service do app** (etl-python), em **Variables**, adicione uma variável **DATABASE_URL**.
- Valor: a connection string **privada** do Postgres (no mesmo projeto). No service do Postgres, em Variables, o Railway costuma mostrar algo como:
  - `DATABASE_URL=postgresql://postgres:SENHA@RAILWAY_PRIVATE_DOMAIN:5432/railway`
- Substitua `SENHA`, `RAILWAY_PRIVATE_DOMAIN` pelos valores reais que o Railway mostra (ou use “Variable reference” se estiver disponível).

Assim o upload (POST /upload) grava nas tabelas **pedidos**, **produtos**, **clientes** no Postgres do Railway. Se quiser usar **Supabase** em vez do Postgres do Railway, use como **DATABASE_URL** a connection string do Supabase (Pooler).

---

## 8. Segurança

- **Não** commite senhas no Git. No Railway tudo fica em **Variables**.
- Se você expôs a senha do Postgres em algum lugar, altere no Railway: service Postgres → Variables → altere **POSTGRES_PASSWORD** e reinicie o Postgres (e o app, se necessário).

---

## 9. Limite de upload e HTTP 502 (timeout)

No plano pago o limite de request body é bem maior que na Vercel. Arquivos grandes (dezenas de MB) costumam funcionar; no código já está configurado até 2 GB por arquivo (`MAX_UPLOAD_PART_MB`).

**Se aparecer HTTP 502 ("Application failed to respond"):** o proxy do Railway encerrou a requisição por tempo. Isso é comum com arquivos de centenas de MB ou GB.

- Envie **um arquivo por vez** (o front já usa lotes de 1; evite muitos na fila).
- Use **"Enviar sem comprimir"** para arquivos muito grandes (evita timeout na compressão no navegador).
- No Railway, em **Settings** do service, veja se há opção de **request timeout** e aumente se existir.
- Para arquivos de 2 GB+, o processamento pode levar vários minutos; 502 pode ser limite do gateway — nesse caso reduzir tamanho por arquivo (ex.: particionar em partes menores) ou processar em outro ambiente com timeout maior.
