# Manual do usuário — Supabase Bulk Uploader

## O que é

Uma tela única para enviar arquivos **XLSX, XLS ou CSV** para o banco Supabase (PostgreSQL). O sistema identifica sozinho se cada arquivo é de **pedidos**, **produtos** ou **clientes** e grava na tabela certa.

---

## Como usar

### 1. Abrir a plataforma

- **Na web (Vercel):** abra o link que o administrador passou (ex.: `https://seu-projeto.vercel.app`).
- **No computador (local):** quem rodar o servidor vai acessar algo como `http://localhost:8000`.

A página que abre é a do **Supabase Bulk Uploader** (título no topo).

---

### 2. Enviar arquivos

1. **Arraste** os arquivos para a área tracejada **“Arraste os XLSXs aqui”**  
   **ou** clique nessa área e escolha os arquivos no explorador.

2. Formatos aceitos: **.xlsx**, **.xls**, **.csv**.  
   Máximo: **40 arquivos** por vez.

3. Cada arquivo aparece na lista com um rótulo:
   - **pedidos** (azul)
   - **produtos** (amarelo)
   - **clientes** (roxo)

4. Para tirar um arquivo da lista, clique no **✕** ao lado dele.  
   Para limpar tudo, use o botão **Limpar**.

5. **Substituir pedidos (troca diária):**  
   Se você marcar essa opção, antes de inserir a tabela de **pedidos** será apagada. Use quando for enviar **só o arquivo novo do dia** e quiser que ele substitua o que estava antes.

6. Clique em **“⚡ Enviar para Supabase”**.

7. A tela mostra:
   - “Comprimindo…” e depois “Enviando…”
   - Barra de progresso e, no final, **Total**, **Sucesso** e **Falhas**, com o detalhe por arquivo (nome, tipo, linhas, tempo, OK ou ERRO).

---

### 3. Base enriquecida (Pedidos + Produtos + Clientes)

Depois de **já ter enviado** arquivos de **clientes**, **pedidos** e **produtos**:

1. Role até o bloco **“Base enriquecida”**.
2. Clique em **“Criar/atualizar view pedidos_enriquecida”**.
3. Se der certo, aparece uma mensagem em verde (ex.: “View criada.”).  
   Essa view no Supabase junta pedidos com dados de produto (por SKU) e de cliente (por single_id).

---

## Resumo rápido

| Ação              | O que fazer |
|-------------------|-------------|
| Enviar arquivos   | Arrastar ou clicar na área, depois “Enviar para Supabase”. |
| Troca diária de pedidos | Marcar “Substituir pedidos” e enviar só o arquivo do dia. |
| Criar base cruzada | Depois de subir as 3 bases, clicar em “Criar/atualizar view pedidos_enriquecida”. |
| Limpar a lista    | Botão “Limpar” ou ✕ em cada arquivo. |

---

## Erros comuns

- **“HTTP 404” ao enviar:** a URL da aplicação está errada ou o backend não está no ar. Confirme com o administrador o link correto e se o deploy está ativo.
- **“Nenhum arquivo enviado”:** nenhum arquivo foi selecionado; escolha pelo menos um .xlsx, .xls ou .csv.
- **Falha em um arquivo:** na lista de resultados aparece “ERRO” e detalhes em vermelho; o restante dos arquivos pode ter sido enviado com sucesso. Ajuste o arquivo com problema e reenvie se precisar.

---

## Campo “API URL do backend”

Na seção **Conexão**, o campo **“API URL do backend”** deve ficar **vazio** na maioria dos casos (a própria página usa o mesmo servidor). Só preencha se o administrador disser que você deve usar outra URL.
