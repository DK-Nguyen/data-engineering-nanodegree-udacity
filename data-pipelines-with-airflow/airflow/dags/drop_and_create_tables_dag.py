"""
This module is used to drop and create tables in the redShift data warehouse.
Run before doing the ETL to 
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers.create_tables_queries import CreateTablesQueries

default_args = {
    'owner': 'DK',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

drop_create_dag = DAG('create_drop_tables_dag',
                      default_args=default_args,
                      description='Create and Drop Tables in Redshift',
                      schedule_interval=None)

start_operator = DummyOperator(task_id='Begin_execution',  dag=drop_create_dag)

start_operator = DummyOperator(task_id='Begin_execution',  dag=drop_create_dag)

drop_table_artist_operator = PostgresOperator(task_id='Drop_table_artist', 
                                              dag=drop_create_dag,
                                              postgres_conn_id='redshift',
                                              sql=CreateTablesQueries.drop_table_artist)

create_table_artist_operator = PostgresOperator(task_id='Create_table_artist', 
                                                dag=drop_create_dag,
                                                postgres_conn_id='redshift',
                                                sql=CreateTablesQueries.create_table_artists)

drop_table_songplays_operator = PostgresOperator(task_id='Drop_table_songplays', 
                                                 dag=drop_create_dag,
                                                 postgres_conn_id='redshift',
                                                 sql=CreateTablesQueries.drop_table_songplays)

create_table_songplays_operator = PostgresOperator(task_id='Create_table_songplays', 
                                                   dag=drop_create_dag,
                                                   postgres_conn_id='redshift',
                                                   sql=CreateTablesQueries.create_table_songplays)

drop_table_songs_operator = PostgresOperator(task_id='Drop_table_songs', 
                                             dag=drop_create_dag,
                                             postgres_conn_id='redshift',
                                             sql=CreateTablesQueries.drop_table_songs)

create_table_songs_operator = PostgresOperator(task_id='Create_table_songs', 
                                               dag=drop_create_dag,
                                               postgres_conn_id='redshift',
                                               sql=CreateTablesQueries.create_table_songs)

drop_table_time_operator = PostgresOperator(task_id='Drop_table_time', 
                                            dag=drop_create_dag,
                                            postgres_conn_id='redshift',
                                            sql=CreateTablesQueries.drop_table_time)

create_table_time_operator = PostgresOperator(task_id='Create_table_time', 
                                              dag=drop_create_dag,
                                              postgres_conn_id='redshift',
                                              sql=CreateTablesQueries.create_table_time)

drop_table_user_operator = PostgresOperator(task_id='Drop_table_user', 
                                            dag=drop_create_dag,
                                            postgres_conn_id='redshift',
                                            sql=CreateTablesQueries.drop_table_users)

create_table_user_operator = PostgresOperator(task_id='Create_table_user', 
                                              dag=drop_create_dag,
                                              postgres_conn_id='redshift',
                                              sql=CreateTablesQueries.create_table_users)

drop_table_staging_events_operator = PostgresOperator(task_id='Drop_table_staging_events', 
                                                      dag=drop_create_dag,
                                                      postgres_conn_id='redshift',
                                                      sql=CreateTablesQueries.drop_table_staging_events)

create_table_staging_events_operator = PostgresOperator(task_id='Create_table_staging_events', 
                                                        dag=drop_create_dag,
                                                        postgres_conn_id='redshift',
                                                        sql=CreateTablesQueries.create_table_staging_events)

drop_table_staging_songs_operator = PostgresOperator(task_id='Drop_table_staging_songs', 
                                                     dag=drop_create_dag,
                                                     postgres_conn_id='redshift',
                                                     sql=CreateTablesQueries.drop_table_staging_songs)

create_table_staging_songs_operator = PostgresOperator(task_id='Create_table_staging_songs', 
                                                       dag=drop_create_dag,
                                                       postgres_conn_id='redshift',
                                                       sql=CreateTablesQueries.create_table_staging_songs)

end_operator = DummyOperator(task_id='Stop_execution',  dag=drop_create_dag)

start_operator >> drop_table_artist_operator
drop_table_artist_operator >> create_table_artist_operator
start_operator >> drop_table_songplays_operator
drop_table_songplays_operator >> create_table_songplays_operator
start_operator >> drop_table_songs_operator
drop_table_songs_operator >> create_table_songs_operator
start_operator >> drop_table_time_operator
drop_table_time_operator >> create_table_time_operator
start_operator >> drop_table_user_operator
drop_table_user_operator >> create_table_user_operator
start_operator >> drop_table_staging_events_operator
drop_table_staging_events_operator >> create_table_staging_events_operator
start_operator >> drop_table_staging_songs_operator
drop_table_staging_songs_operator >> create_table_staging_songs_operator
create_table_artist_operator >> end_operator
create_table_songplays_operator >> end_operator
create_table_songs_operator >> end_operator
create_table_time_operator >> end_operator
create_table_user_operator >> end_operator
create_table_staging_events_operator >> end_operator
create_table_staging_songs_operator >> end_operator

