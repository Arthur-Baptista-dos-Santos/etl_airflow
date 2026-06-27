from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


import pandas as pd
import duckdb


def extrair (**kwargs):
    df = pd.read_csv("/opt/airflow/dados/vendas_brutas.csv")
    print(f"Extraídos {len(df)} registros")
    print(f"Colunas: {df.columns.tolist()}")
    print(f"Nulos encontrados: \n{df.isnull().sum}")
    return len (df)


def transformar(**kwargs):
    df = pd.read_csv("/opt/airflow/dados/vendas_brutas.csv")

    total_antes = len(df)
    df = df.dropna(subset=["vendedor", "regiao"])
    total_depois = len(df)
    print(f"Removidos {total_antes - total_depois} registros com nulos")

    df["receita"] = df["quantidade"] * df["preco_unitario"]
    df["custo"] = df["quantidade"] * df["custo_unitario"]
    df["lucro"] = df["receita"] - df["custo"]
    df["margem_pct"] = (df["lucro"] / df["receita"] * 100).round(2)

    df["performance"] = df["margem_pct"].apply(
        lambda x: "alta" if x >= 45 else "media" if x >= 35 else "baixa"
    )

    df["data"] = pd.to_datetime(df["data"])

    df.to_csv("/opt/airflow/dados/vendas_transformadas.csv", index=False)
    print(f"Transformação concluída: {len(df)} registros salvos")


def carregar(**kwargs):
    df = pd.read_csv("/opt/airflow/dados/vendas_transformadas.csv")

    con = duckdb.connect("/opt/airflow/saida/vendas.db")
    con.execute("DROP TABLE IF EXISTS vendas")
    con.execute("CREATE TABLE vendas AS SELECT * FROM df")

    total = con.execute("SELECT COUNT(*) FROM vendas").fetchone()[0]
    print(f"Carregados {total} registros no DuckDB")
    con.close()


def gerar_relatorio(**kwargs):
    con = duckdb.connect("/opt/airflow/saida/vendas.db")

    resumo = con.execute("""
        SELECT
            vendedor,
            COUNT(*) as total_vendas,
            ROUND(SUM(receita), 2) as receita_total,
            ROUND(SUM(lucro), 2) as lucro_total,
            ROUND(AVG(margem_pct), 2) as margem_media
        FROM vendas
        GROUP BY vendedor
        ORDER BY lucro_total DESC
    """).fetchdf()

    con.close()

    relatorio = "/opt/airflow/saida/relatorio.txt"
    with open(relatorio, "w", encoding="utf-8") as f:
        f.write("RELATORIO DE VENDAS\n")
        f.write("=" * 40 + "\n\n")
        f.write(resumo.to_string(index=False))

    print("Relatório gerado em saida/relatorio.txt")
    print(resumo.to_string(index=False))


with DAG(
    dag_id="pipeline_vendas",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "vendas"],
) as dag:

    tarefa_extrair = PythonOperator(
        task_id="extrair",
        python_callable=extrair,
    )

    tarefa_transformar = PythonOperator(
        task_id="transformar",
        python_callable=transformar,
    )

    tarefa_carregar = PythonOperator(
        task_id="carregar",
        python_callable=carregar,
    )

    tarefa_relatorio = PythonOperator(
        task_id="gerar_relatorio",
        python_callable=gerar_relatorio,
    )

    tarefa_extrair >> tarefa_transformar >> tarefa_carregar >> tarefa_relatorio
