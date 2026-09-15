from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

def return_hello():
    return "Hello, Airflow!"

def return_goodbye():
    return "Goodbye, Airflow!"

with DAG(
    dag_id="sample_dag",
    default_args={
        "start_date": datetime(2025, 1, 1)
    },
    schedule="0 0 * * *",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    hello = PythonOperator(
        task_id="hello_task",
        python_callable=return_hello
    )

    goodbye = PythonOperator(
        task_id="goodbye_task",
        python_callable=return_goodbye
    )

    end = EmptyOperator(task_id="end")

    start >> hello >> goodbye >> end
