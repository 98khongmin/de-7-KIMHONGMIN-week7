from datetime import datetime
import os
import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

LOCAL_CSV_PATH = "/tmp/data/netflix_titles.csv"
LOCAL_OUTPUT_DIR = "/tmp/output/silver"

def get_bucket_name(context):
    # Airflow 에서 인자 json 형태로 넘겨줌
    dag_run_conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    return (
        dag_run_conf.get("s3_bucket_name")
        or Variable.get("s3_bucket_name", default_var=None)
        or os.environ.get("S3_BUCKET_NAME")
    )

def download_from_s3(**context):
    bucket_name = get_bucket_name(context)
    if not bucket_name:
        raise ValueError("버킷 이름이 설정되지 않았습니다. 트리거 구성 JSON, Airflow Variables 또는 S3_BUCKET_NAME 환경 변수를 확인하세요.")
    os.makedirs(os.path.dirname(LOCAL_CSV_PATH), exist_ok=True)
    s3 = boto3.client("s3")
    s3.download_file(bucket_name, "bronze/netflix_titles.csv", LOCAL_CSV_PATH)
    print(f"Downloaded netflix_titles.csv to {LOCAL_CSV_PATH}")

def upload_to_s3(**context):
    bucket_name = get_bucket_name(context)
    if not bucket_name:
        raise ValueError("버킷 이름이 설정되지 않았습니다. 트리거 구성 JSON, Airflow Variables 또는 S3_BUCKET_NAME 환경 변수를 확인하세요.")
    today = datetime.now().strftime("%Y-%m-%d")
    s3_prefix = f"silver/{today}/"
    s3 = boto3.client("s3")
    
    uploaded_files = []
    for root, _, files in os.walk(LOCAL_OUTPUT_DIR):
        for file in files:
            if file.endswith(".parquet"):
                local_file = os.path.join(root, file)
                s3_key = f"{s3_prefix}{file}"
                s3.upload_file(local_file, bucket_name, s3_key)
                uploaded_files.append(s3_key)
    
    print(f"Uploaded count: {len(uploaded_files)}")
    for key in uploaded_files:
        print(f"Uploaded S3 Key: s3://{bucket_name}/{key}")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="weekly_pipeline_kimhongmin",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["q9", "week7"],
) as dag:

    download_csv = PythonOperator(
        task_id="download_csv",
        python_callable=download_from_s3,
    )

    transform = BashOperator(
        task_id="transform",
         bash_command=(
            "spark-submit /opt/airflow/dags/jobs/transform.py "
            "{{ dag_run.conf.get('release_year', 2015) if dag_run else 2015 }} "
            f"{LOCAL_CSV_PATH} {LOCAL_OUTPUT_DIR}"
        ),
    )

    upload_silver = PythonOperator(
        task_id="upload_silver",
        python_callable=upload_to_s3,
    )

    download_csv >> transform >> upload_silver
