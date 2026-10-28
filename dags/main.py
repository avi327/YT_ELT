from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlistId, get_video_ids, extract_video_data, save_to_json

local_tz = pendulum.timezone("Europe/Malta")

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
) as dag:
    
    playlist_id = get_playlistId()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_json=save_to_json(extracted_data)

    playlist_id >> video_ids >> extracted_data >> save_to_json_json