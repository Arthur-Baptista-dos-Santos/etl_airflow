# `Pipeline ETL com Airflow + DuckDB`

> Pipeline de dados orquestrado pelo Apache Airflow rodando em Docker, com transformacao via pandas e armazenamento analítico em DuckDB.

---

## `Tecnologias`

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.0-darkgreen)
![DuckDB](https://img.shields.io/badge/DuckDB-1.5-yellow)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![pandas](https://img.shields.io/badge/pandas-2.x-purple)

---

## `O que faz`

Extrai dados brutos de vendas de um CSV, detecta e remove registros invalidos, calcula metricas de negocio (receita, lucro, margem) e classifica performance por vendedor. O resultado e armazenado em DuckDB e um relatorio textual e gerado automaticamente. Todo o fluxo e orquestrado pelo Airflow com dependencias declaradas entre tarefas.

---

## `Pipeline`

```
CSV bruto
    extrair        - le o arquivo, loga nulos encontrados
        transformar    - remove nulos, calcula metricas, classifica performance
            carregar       - persiste dados limpos no DuckDB
                gerar_relatorio - executa SQL analitico e gera relatorio por vendedor
```

---

## `Arquitetura`

```
etl_airflow/
├── dags/
│   └── pipeline_vendas.py   # DAG com 4 PythonOperators encadeados
├── dados/
│   └── vendas_brutas.csv    # fonte de dados (25 registros, 2 nulos propositais)
├── saida/                   # gerado pelo pipeline (gitignored)
│   ├── vendas.db            # banco DuckDB com dados transformados
│   └── relatorio.txt        # resumo por vendedor
├── logs/                    # logs do Airflow (gitignored)
├── plugins/
└── docker-compose.yml       # Airflow (webserver + scheduler) + PostgreSQL
```

---

## `Resultados`

| Vendedor | Vendas | Receita | Lucro | Margem |
|---|---|---|---|---|
| Joao | 6 | R$ 23.650 | R$ 9.810 | 44,42% |
| Carlos | 6 | R$ 13.870 | R$ 5.750 | 44,42% |
| Ana | 5 | R$ 8.120 | R$ 3.620 | 46,97% |
| Maria | 6 | R$ 6.630 | R$ 3.170 | 47,76% |

Registros invalidos removidos: 2 (vendedor e regiao ausentes)
Duracao total do pipeline: ~5 segundos

---

## `Pre-requisitos`

- Docker Desktop instalado e rodando

---

## `Como rodar`

```bash
# Clonar o repositorio
git clone https://github.com/Arthur-Baptista-dos-Santos/etl_airflow.git
cd etl_airflow

# Inicializar o banco e criar usuario admin
docker compose up airflow-init

# Subir o Airflow em background
docker compose up webserver scheduler -d
```

Acesse `http://localhost:8080` com login `admin` / senha `admin`.

Ative a DAG `pipeline_vendas` e clique em **Trigger** para executar.

```bash
# Parar os containers
docker compose down
```

---

## `Estrutura da DAG`

```python
tarefa_extrair >> tarefa_transformar >> tarefa_carregar >> tarefa_relatorio
```

O operador `>>` define dependencia sequencial. O Airflow garante que cada tarefa so executa apos a anterior ter concluido com sucesso.

---

## `Conceitos aplicados`

- **`ETL`**: padrao Extract-Transform-Load para pipelines de dados estruturados
- **`Apache Airflow`**: orquestrador de workflows com interface visual, logs por tarefa e reexecucao seletiva
- **`DAG`**: grafo aciclico dirigido que representa o pipeline e suas dependencias
- **`PythonOperator`**: executa funcoes Python como tarefas dentro do Airflow
- **`Docker Compose`**: orquestra 3 containers (webserver, scheduler, postgres) com dependencias e healthcheck
- **`DuckDB`**: banco analítico embutido, sem servidor, ideal para pipelines locais e de dados
- **`pandas`**: transformacao e limpeza de dados com deteccao de nulos e feature engineering
- **`catchup=False`**: evita execucoes retroativas ao subir uma DAG com data de inicio no passado
