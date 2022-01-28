from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,CreateTableOperator)
from helpers import SqlQueries as q
#import sql_queries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False,
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table='staging_events',
    s3_path='s3://udacity-dend/log_data',
    json_paths='s3://udacity-dend/log_json_path.json',
    query = q.staging_template
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table='staging_songs',
    s3_path='s3://udacity-dend/song_data',
    json_paths="auto",
    query = q.staging_template
)

create_staging_songs_table = CreateTableOperator(
    task_id='Create_staging_songs_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = q.staging_songs_table_create 
)
create_staging_events_table = CreateTableOperator(
    task_id='Create_staging_events_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = q.staging_events_table_create 
)
create_songplays_table = CreateTableOperator(
    task_id='Create_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = q.songplay_table_create
)
create_user_table = CreateTableOperator(
    task_id='Create_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = q.user_table_create
)

create_song_table = CreateTableOperator(
    task_id='Create_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = q.song_table_create
)

create_artist_table = CreateTableOperator(
    task_id='Create_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = q.artist_table_create
)

create_time_table = CreateTableOperator(
    task_id='Create_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = q.time_table_create
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = q.songplay_table_insert,
    table = 'public.songplays'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = q.user_table_insert,
    table = 'public.users'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = q.song_table_insert,
    table = 'public.songs'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = q.artist_table_insert,
    table = 'public.artists'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    query = q.time_table_insert,
    table = 'public.time'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift"
)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >>create_staging_songs_table
start_operator >>create_staging_events_table
create_staging_songs_table >> stage_songs_to_redshift
create_staging_events_table >> stage_events_to_redshift
stage_songs_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_artist_dimension_table
stage_songs_to_redshift >> load_song_dimension_table
stage_events_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_time_dimension_table
stage_events_to_redshift >> load_user_dimension_table
create_songplays_table >> load_songplays_table
create_time_table >> load_time_dimension_table
create_artist_table >> load_artist_dimension_table 
create_song_table >> load_song_dimension_table
create_user_table >> load_user_dimension_table
load_songplays_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
run_quality_checks >> end_operator