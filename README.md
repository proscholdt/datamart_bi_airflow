# Data mart Voomp — Airflow / Astronomer

Pipeline ETL em arquitetura **medallion** (bronze → silver → gold) sobre **Azure Blob
Storage**, usando **Polars**. A orquestração antiga por `subprocess` foi substituída por
um **DAG do Airflow** rodando na **Astronomer**, com as tasks executando como **pods no
cluster** (`KubernetesExecutor`).

## Estrutura

```
.
├── dags/
│   └── voomp_datamart.py        # DAG único (sensores + bronze→silver→gold + arquivamento)
├── include/
│   └── voomp/
│       ├── voomp_config.py      # conexão Azure + nomes de container + auto-criação
│       ├── 1_bronze/            # scripts bronze→silver
│       └── 2_silver/            # scripts silver→gold e arquivamento
├── Dockerfile                   # Astro Runtime (Airflow 2.10)
├── requirements.txt             # polars, azure-storage-blob, fastexcel, provider azure...
├── .env                         # segredos LOCAIS (gitignored — NÃO commitar)
├── .env.example                 # template sem segredos
└── .github/workflows/deploy.yml # CI: deploy automático no push para main
```

> A pasta `voomp/` na raiz é o **código original** (backup). Pode ser removida após validar
> que tudo roda a partir de `include/voomp/`.

## O DAG `voomp_datamart`

- **Disparo event-driven:** `WasbPrefixSensor` aguarda arquivos chegarem no bronze
  (`source_voomp/voomp/` e `source_voomp/projetadas_voomp/`).
- **Paralelismo:** após o `bronze→silver`, as 4 dimensões + `f_vendas` rodam em paralelo;
  o ramo de projeções roda em paralelo ao de vendas.
- **Arquivamento (`*_carregados`)** só ocorre depois que as leituras terminam.
- Cada task executa o script original via `runpy` (código de transformação inalterado).

## Containers (novos) + auto-criação

Definidos em [`include/voomp/voomp_config.py`](include/voomp/voomp_config.py), com override por env:

| Camada | Container         | Env var de override        |
|--------|-------------------|----------------------------|
| bronze | `datamartbibronze`| `VOOMP_CONTAINER_BRONZE`   |
| silver | `datamartbisilver`| `VOOMP_CONTAINER_SILVER`   |
| gold   | `datamartbigold`  | `VOOMP_CONTAINER_GOLD`     |

`get_container_client(camada)` **cria o container se ele não existir**.

## Segredos (.env)

⚠️ O `.env` contém **chaves de produção** e está no `.gitignore`. **Nunca** faça commit dele.

Variáveis usadas pelo data mart:
- `STORAGE_ACCOUNT_NAME` / `STORAGE_ACCOUNT_KEY` — conta Azure (hoje `saactivecampaign`).
- `AIRFLOW_CONN_WASB_DEFAULT` — conexão usada pelo `WasbPrefixSensor`.
- `VOOMP_CONTAINER_*` (opcional) — sobrescreve os nomes de container.

## Rodar localmente

Pré-requisitos: **Docker Desktop** + **Astro CLI** (`winget install -e --id Astronomer.Astro`).

```powershell
astro dev start          # sobe o Airflow local em http://localhost:8080 (admin/admin)
```

Na UI: despause `voomp_datamart` e clique em **Trigger DAG**. (Local usa LocalExecutor; o
`executor_config` de pods é ignorado sem efeito.)

## Deploy no cluster Astronomer (`datamartbi01`)

1. Crie um **Deployment** no Workspace, no cluster `datamartbi01`, com **executor = Kubernetes**
   (assim as tasks rodam como pods no cluster, sem workers Celery).
2. No Deployment, configure as **Environment Variables** (marque as chaves como *Secret*):
   `STORAGE_ACCOUNT_NAME`, `STORAGE_ACCOUNT_KEY`, `AIRFLOW_CONN_WASB_DEFAULT`.
3. Deploy:
   - Manual: `astro deploy <DEPLOYMENT_ID>`
   - CI: push para `main` → GitHub Actions ([deploy.yml](.github/workflows/deploy.yml)).
     Configure no repositório: secret `ASTRONOMER_API_TOKEN` e variable `ASTRO_DEPLOYMENT_ID`.

## Pontos a revisar

- **Tags/versões:** confirme a tag do `astro-runtime` no [Dockerfile](Dockerfile) e os pins em
  [requirements.txt](requirements.txt) (ajuste se o `pip` não resolver).
- **Sensores `deferrable=True`:** requerem provider Azure recente + triggerer ativo (padrão no
  Astro). Se necessário, troque por `mode="reschedule"`.
- **Memória dos pods:** `MEM_DEFAULT`/`MEM_HEAVY` no DAG são estimativas — ajuste ao volume real.
