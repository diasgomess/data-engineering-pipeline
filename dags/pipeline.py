from datetime import datetime, timedelta

from airflow import DAG
try:
    # Airflow 3.x
    from airflow.providers.standard.operators.python import PythonOperator
except ModuleNotFoundError:
    # Airflow 2.x
    from airflow.operators.python import PythonOperator

from src.ingestion.api_clima import run as bronze_clima
from src.ingestion.api_livros import run as bronze_livros
from src.ingestion.csv_ingestion import run as bronze_kaggle

from src.transformation_silver.silver_clima import run as silver_clima
from src.transformation_silver.silver_kaggle import run as silver_kaggle
from src.transformation_silver.silver_livros import run as silver_livros

from src.transformation_gold.gold_clima import run as gold_clima
from src.transformation_gold.gold_kaggle import run as gold_kaggle
from src.transformation_gold.gold_livros import run as gold_livros


default_args = {
    "owner":            "data-engineering",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="pipeline_completo",
    description="Pipeline Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",  # todo dia às 06:00
    catchup=False,
    tags=["bronze", "silver", "gold"],
) as dag:

    # ── Bronze ────────────────────────────────────────────────
    t_bronze_clima   = PythonOperator(task_id="bronze_clima",   python_callable=bronze_clima)
    t_bronze_livros  = PythonOperator(task_id="bronze_livros",  python_callable=bronze_livros)
    t_bronze_kaggle  = PythonOperator(task_id="bronze_kaggle",  python_callable=bronze_kaggle)

    # ── Silver ────────────────────────────────────────────────
    t_silver_clima   = PythonOperator(task_id="silver_clima",   python_callable=silver_clima)
    t_silver_livros  = PythonOperator(task_id="silver_livros",  python_callable=silver_livros)
    t_silver_kaggle  = PythonOperator(task_id="silver_kaggle",  python_callable=silver_kaggle)

    # ── Gold ──────────────────────────────────────────────────
    t_gold_clima     = PythonOperator(task_id="gold_clima",     python_callable=gold_clima)
    t_gold_livros    = PythonOperator(task_id="gold_livros",    python_callable=gold_livros)
    t_gold_kaggle    = PythonOperator(task_id="gold_kaggle",    python_callable=gold_kaggle)

    # ── Dependências (Bronze → Silver → Gold por tabela) ──────
    t_bronze_clima   >> t_silver_clima   >> t_gold_clima
    t_bronze_livros  >> t_silver_livros  >> t_gold_livros
    t_bronze_kaggle  >> t_silver_kaggle  >> t_gold_kaggle