import json
from base64 import b64encode
import airflow
from airflow import DAG
from airflow.models import Variable

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator

from datetime import datetime, timedelta

default_args = {
    "start_date": datetime(2022, 5, 9), #airflow.utils.days_ago(0),
    "retries": 1,
    "retry_delays": timedelta(minutes = 5),
    "owner": "Airflow",
    "depends_on_past": False,
}

dag = DAG(
    "SEB-Automation",
    schedule_interval=None, #timedelta(hours=1) or "0 10 * * *"
    default_args=default_args,
    catchup=False,
    max_active_runs=1
)

# For the demonstration of Airflow Variables usage
config = {}
config["project_id"] = Variable.get("project_id")
config["bucket"] = Variable.get("gcs_bucket")
config["dataset_id"] = "sample_data"

start = DummyOperator(
    dag=dag,
    task_id="Start"
)

create_dataset = BigQueryCreateEmptyDatasetOperator(
    dag=dag,
    task_id="Create_Empty_Dataset_if_not_exists",
    project_id=config['project_id'],
    dataset_id=config['dataset_id'],
    gcp_conn_id='bigquery_default',
    exists_ok=True,
    location="EU"
)

export_data_gcs_to_bigquery = GCSToBigQueryOperator(
    dag=dag,
    task_id="Export_To_BigQuery_From_GCS",
    bucket=config['bucket'],
    source_objects="aggregatedData.csv",
    source_format="CSV",
    destination_project_dataset_table=f"{config['project_id']}.{config['dataset_id']}.aggregated_data",
    write_disposition="WRITE_TRUNCATE", # Overwrites the table
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    bigquery_conn_id="bigquery_default",
    skip_leading_rows=1,
    field_delimiter=","
)

def read_data_from_gcs(**kwargs):
    
    import pandas as pd
    import io
    from google.cloud import storage
    
    client = storage.Client(project=config['project_id'])
    bucket = client.bucket(config['bucket'])
    blob = bucket.blob("aggregatedData.csv")
    data = blob.download_as_string()
    
    df = pd.read_csv(io.BytesIO(data))
    
    return df.to_dict('records')

read_data = PythonOperator(
    dag=dag,
    task_id="Read_From_GCS",
    python_callable=read_data_from_gcs,
)

publish_task = PubSubPublishMessageOperator(
    dag=dag,
    task_id="publish_data_to_pubsub",
    project_id = config['project_id'],
    topic= "SEB-data-engineering-task",
    messages=[{"data": b64encode(str.encode("{{ ti.xcom_pull(task_ids='Read_From_GCS') }}"))}],
)

end = DummyOperator(
    dag=dag,
    task_id="End"
)

start >> create_dataset >> export_data_gcs_to_bigquery >> end
start >> read_data >> publish_task >> end


