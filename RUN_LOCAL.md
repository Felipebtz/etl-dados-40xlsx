# Rodar o projeto localmente — passo a passo

## 1. Pré-requisitos

- **Python 3.10+** instalado ([python.org](https://www.python.org/downloads/))
- **Conta GCP** com projeto, bucket no Cloud Storage e service account (arquivo `.json`)

---

## 2. Abrir o projeto no terminal

```powershell
cd C:\Users\felip\OneDrive\meus_sistemas\etl-python
```

---

## 3. Criar o ambiente virtual (venv)

```powershell
python -m venv venv
```

Se der erro, tente `py -m venv venv`.

---

## 4. Ativar o venv

**No PowerShell (Windows):**

```powershell
.\venv\Scripts\Activate.ps1
```

Se aparecer erro de política de execução, rode antes:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**No CMD:**

```cmd
venv\Scripts\activate.bat
```

Quando o venv estiver ativo, o prompt deve mostrar `(venv)` no início.

---

## 5. Instalar as dependências

```powershell
pip install -r requirements.txt
```

---

## 6. Configurar o arquivo .env

1. Copie o exemplo para `.env`:

   ```powershell
   copy .env.example .env
   ```

2. Abra o `.env` no editor e preencha:

   - **GCP_PROJECT_ID** — ID do projeto GCP (ex.: `springboot-demo-484902`)
   - **GCP_BUCKET** — Nome do bucket no Cloud Storage (ex.: `etl-uploads-meu-projeto`)
   - **GCP_DATASET** — Nome do dataset no BigQuery (ex.: `dataset_lucas`)
   - **GCP_SERVICE_ACCOUNT_JSON** — Conteúdo **inteiro** do arquivo JSON da service account

   Para o JSON você pode:

   - Abrir o arquivo `springboot-demo-484902-6d1ccdd455a6.json` (ou o que você usa)
   - Copiar todo o conteúdo (Ctrl+A, Ctrl+C)
   - Colar no `.env` na mesma linha, assim:

     ```
     GCP_SERVICE_ACCOUNT_JSON={"type":"service_account","project_id":"springboot-demo-484902",...}
     ```

   Ou colar com várias linhas (o `load_dotenv` aceita). **Não** apague aspas nem vírgulas.

3. Salve o `.env`.

---

## 7. Subir o servidor

```powershell
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

- `--reload` — reinicia o app quando você alterar arquivos (útil para desenvolvimento)
- A API e a interface ficam em: **http://127.0.0.1:8000**

---

## 8. Testar no navegador

1. Abra **http://127.0.0.1:8000**
2. Você deve ver a tela do **GCP Bulk Uploader** com:
   - Bloco **“Recomendado para muitos GB”** (CSVs soltos)
   - Opção de **ZIP** mais abaixo
3. Para testar: escolha um ou mais CSVs na área “Recomendado para muitos GB” e clique em **Enviar CSVs para BigQuery**.

---

## 9. Parar o servidor

No terminal onde o uvicorn está rodando, pressione **Ctrl+C**.

---

## Resumo dos comandos (em ordem)

```powershell
cd C:\Users\felip\OneDrive\meus_sistemas\etl-python
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
# Editar .env com GCP_PROJECT_ID, GCP_BUCKET, GCP_DATASET e GCP_SERVICE_ACCOUNT_JSON
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Depois acesse **http://127.0.0.1:8000**.

---

## Se der erro "GCP_SERVICE_ACCOUNT_JSON não configurada" ou 500 no /storage-token

Significa que o app não encontrou as credenciais GCP. Faça o seguinte:

1. **Atualize o código**  
   Se você baixou um ZIP do GitHub, baixe de novo (ou faça `git pull`) para ter o `main.py` que aceita o arquivo `.json` por caminho.

2. **Crie (ou edite) o arquivo `.env`** na pasta do projeto (mesma pasta do `main.py`) com exatamente:

   ```env
   GCP_PROJECT_ID=springboot-demo-484902
   GCP_BUCKET=el-lucas
   GCP_DATASET=dataset_lucas
   GOOGLE_APPLICATION_CREDENTIALS=springboot-demo-484902-6d1ccdd455a6.json
   ```

3. **Coloque o arquivo da service account na pasta do projeto**  
   O arquivo deve se chamar `springboot-demo-484902-6d1ccdd455a6.json` e ficar na **mesma pasta** que o `main.py` (não dentro de `venv`). Quem configurou o GCP (Felipe) precisa te enviar esse arquivo por um canal seguro (não mandar por chat público).

4. **Reinicie o servidor**  
   Pare o uvicorn (Ctrl+C) e suba de novo: `uvicorn main:app --reload --host 0.0.0.0 --port 8000`.
