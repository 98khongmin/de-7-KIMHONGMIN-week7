from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='backfill_demo_김홍민',
    default_args=default_args,
    description='Q7 backfill demo',
    schedule='@daily',
    start_date=datetime(2026, 9, 9),  # 오늘(9/16) 기준 7일 전 고정 리터럴
    catchup=True,
) as dag:

    # logical date 파일 생성 태스크
    write_daily_file = BashOperator(
        task_id='write_daily_file',
        bash_command=(
            'mkdir -p /tmp/backfill_output && '
            'echo "Run date: {{ ds }}" > /tmp/backfill_output/daily_{{ ds }}.txt'
        ),
    )

    # 파일 정상 생성 검증 태스크
    verify_file = BashOperator(
        task_id='verify_file',
        bash_command='ls -l /tmp/backfill_output/daily_{{ ds }}.txt',
    )

    write_daily_file >> verify_file
