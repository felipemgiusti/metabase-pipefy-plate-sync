# 🚗 Metabase → Pipefy Plate Sync

Automação em Python que cria automaticamente cards no Pipefy com base em dados extraídos do Metabase.

---

## 📌 Objetivo

Eliminar processos manuais criando cards automaticamente no Pipefy a partir de registros identificados no Metabase.

---

## ⚙️ Como funciona

1. Consulta dados no Metabase via API
2. Filtra registros do dia atual
3. Transforma os dados (placa, cidade, data)
4. Cria cards no Pipefy via GraphQL API
5. Evita duplicidade (execução local)
6. Executa automaticamente via GitHub Actions

---

## 🧰 Tecnologias utilizadas

- Python 3.11
- Requests
- GraphQL (Pipefy API)
- Metabase API
- GitHub Actions (automação)
- dotenv

---

## 🔐 Variáveis de ambiente

Crie um arquivo `.env` local com:

```env
METABASE_API_KEY=seu_token_metabase
PIPEFY_TOKEN=seu_token_pipefy
