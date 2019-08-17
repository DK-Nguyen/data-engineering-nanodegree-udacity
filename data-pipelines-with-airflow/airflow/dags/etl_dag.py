"""
This module extracts the data from JSON files stored in S3 and put them
in the staging tables in RedShift. After that, it will transform the data
into fact and dimension tables. 
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'DK',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

etl_dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="@hourly"
          )

# start the data pipeline
start_operator = DummyOperator(task_id='Begin_execution',  dag=etl_dag)

# load the data from S3 to staging tables in RedShift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=etl_dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data/',
    clear_table=False,
    extra_info_sql="format as json 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=etl_dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    clear_table=False,
    extra_info_sql="json 'auto' compupdate off region 'us-west-2'"
)

# load the data from staging tables (staging_events & staging_songs) into the fact table (songplays)
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=etl_dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql_command=SqlQueries.songplay_table_insert,
    clear_table=False
)

# load the data from staging tables (staging_events & staging_songs) 
# into the dimension tables (user, songs, artists, time)
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=etl_dag,
    redshift_conn_id='redshift',
    table='users',
    sql_command=SqlQueries.user_table_insert,
    clear_table=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=etl_dag,
    redshift_conn_id='redshift',
    table='songs',
    sql_command=SqlQueries.song_table_insert,
    clear_table=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=etl_dag,
    redshift_conn_id='redshift',
    table='artists',
    sql_command=SqlQueries.artist_table_insert,
    clear_table=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=etl_dag,
    redshift_conn_id='redshift',
    table='time',
    sql_command=SqlQueries.artist_table_insert,
    clear_table=True
)

# run data quality check
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=etl_dag,
    redshift_conn_id='redshift',
    table='time',
    retries=3,
    dq_checks=None
)

# data pipeline done!
end_operator = DummyOperator(task_id='Stop_execution',  dag=etl_dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
