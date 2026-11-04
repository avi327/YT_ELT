from airflow import DAG
from dataquality.soda import yt_elt_data_quality
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlistId, get_video_ids, extract_video_data, save_to_json
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

local_tz = pendulum.timezone("Europe/Malta")

from datawarehouse.dwh import staging_table, core_table

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1, tzinfo=local_tz),
    #'end_date': datetime(2023, 12, 31, tzinfo=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['airflow@example.com'],
    'max_active_runs': 1,
    'diagram_timeout': timedelta(minutes=60),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='A DAG to fetch YouTube video statistics and save to JSON',
    schedule_interval='0 14 * * *',  # Daily at 2 PM
    catchup=False,
) as dag_produce:
    
    playlist_id = get_playlistId()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_json=save_to_json(extracted_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    playlist_id >> video_ids >> extracted_data >> save_to_json_json >> trigger_update_db

with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='DAG to proccess json file and insert data into both staging and core schemas',
    schedule=None, 
    catchup=False,
) as dag_update:
    
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
            task_id="trigger_data_quality",
            trigger_dag_id="data_quality",
        )

    update_staging >> update_core >> trigger_data_quality

with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='DAG to check the data quality on both layers in the db',
    schedule=None, 
    catchup=False,
) as dag_quality:
    
    soda_validation_staging = yt_elt_data_quality("staging")
    soda_validation_core = yt_elt_data_quality("core")

    soda_validation_staging >> soda_validation_core