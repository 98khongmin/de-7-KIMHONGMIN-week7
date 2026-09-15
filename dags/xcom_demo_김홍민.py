from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

# 이름 설정
ultimate_number = "42"
MY_NAME = "김홍민"

default_args = {
    "ultimate_number": ultimate_number,
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

def return_data(**context):
    ti = context["ti"]
    # 첫 시도(try_number == 1) 시 강제 실패 유발하여 retry 기록 생성
    if ti.try_number <= 1:
        logging.info(f"[시도 {ti.try_number}] 첫 번째 시도 실패 시뮬레이션")
        raise ValueError("첫 실패")
    
    # 2회 이상 시도 시 정상 수행 및 XCom return
    result_value = f"{ultimate_number}"
    return result_value

def pull_data(**context):
    ti = context["ti"]
    # 앞 작업의 return_value를 xcom_pull로 수신
    pulled_value = ti.xcom_pull(task_ids="task_return_data")
    logging.info("=" * 40)
    logging.info(f"xcom_pull 값: {pulled_value}")
    logging.info("=" * 40)

with DAG(
    dag_id=f"xcom_demo_{MY_NAME}",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    task_return_data = PythonOperator(
        task_id="task_return_data",
        python_callable=return_data,
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    task_pull_data = PythonOperator(
        task_id="task_pull_data",
        python_callable=pull_data,
    )

    task_return_data >> task_pull_data
