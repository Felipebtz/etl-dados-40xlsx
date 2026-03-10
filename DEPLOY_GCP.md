# Deploy com GCP Storage + BigQuery

Quando você configura as variáveis GCP, o app **não recebe os arquivos no Railway**: o navegador envia direto para o **Google Cloud Storage** e o backend só dispara o **load no BigQuery**. Assim evita 502 e timeout com arquivos de muitos GB.

---

## 1. No Google Cloud (Console)

1. **Projeto**  
   Use o projeto do arquivo de chave (ex.: `springboot-demo-484902`).

2. **Bucket**  
   - Storage → Criar bucket (ex.: `etl-uploads-seu-projeto`).  
   - Região: preferência mesma do BigQuery.  
   - Permissões: a **service account** do JSON precisa de **Storage Object Creator** (ou equivalente) nesse bucket.

3. **BigQuery**  
   - O dataset pode ser criado automaticamente pelo app na primeira carga.  
   - A service account precisa de **BigQuery Data Editor** (ou **Job User** + permissão para criar tabelas no dataset).

4. **Service account (arquivo .json)**  
   - IAM → Service accounts → a que está no JSON.  
   - Permissões necessárias:  
     - **Storage Object Creator** (ou Storage Admin) no bucket.  
     - **BigQuery Job User** + **BigQuery Data Editor** (ou BigQuery Admin) no projeto/dataset.

---

## 2. Variáveis no Railway (ou .env local)

No **service da aplicação** (não no Postgres), em **Variables**:

| Variável | Exemplo | Obrigatório |
|----------|--------|-------------|
| `GCP_PROJECT_ID` | `springboot-demo-484902` | Sim |
| `GCP_BUCKET` | `etl-uploads-seu-projeto` | Sim |
| `GCP_DATASET` | `etl` | Não (default: `etl`) |
| `GCP_CREDENTIALS_JSON` | `{"type":"service_account",...}` | Sim |

**GCP_CREDENTIALS_JSON:**  
Cole o **conteúdo inteiro** do arquivo JSON da service account (o que você baixou do GCP). Pode ser em uma linha; o app faz `json.loads()` desse valor.

- **Não commite** o arquivo `.json` no Git (já está no `.gitignore` com `*-*.json`).  
- No Railway use só a variável; localmente pode usar `GOOGLE_APPLICATION_CREDENTIALS` apontando para o caminho do arquivo.

Quando **GCP_PROJECT_ID**, **GCP_BUCKET** e **GCP_CREDENTIALS_JSON** estão definidos, a UI passa a mostrar o **modo GCP**: envio de CSVs direto para o Storage e carga no BigQuery.

---

## 3. Uso na interface

1. Abra o app (Railway ou local).  
2. Se o backend estiver em modo GCP, a tela mostra **“Modo GCP Storage + BigQuery”**.  
3. Selecione até 40 arquivos **CSV** (BigQuery não carrega XLSX direto do GCS; use CSV).  
4. Opção **“Substituir pedidos”**: na primeira carga de tipo “pedidos”, a tabela `pedidos` no BigQuery é truncada antes de inserir.  
5. Clique em **Enviar para BigQuery**.  
   - Cada arquivo é enviado **direto para o GCS** (URL assinada).  
   - Em seguida o backend chama o BigQuery para carregar desse arquivo no GCS.  
6. Depois de subir pedidos, produtos e clientes, use **“Criar view pedidos_enriquecida”** para criar a view no BigQuery.

---

## 4. Resumo do fluxo

- **Browser** → pede URL assinada ao **Railway** → **Railway** devolve URL do GCS.  
- **Browser** → envia o arquivo **direto para o GCS** (não passa pelo Railway).  
- **Browser** → avisa o **Railway** que o arquivo está em `gs://bucket/path`.  
- **Railway** → dispara **job de load no BigQuery** a partir desse `gs://` e devolve o resultado.

Assim o Railway não trafega os arquivos grandes, evitando 502 e limite de tamanho no servidor.
