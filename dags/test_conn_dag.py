import random
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}
with DAG(dag_id='conn_to_postgres', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:

    create_table_if_not_exists = PostgresOperator(
        task_id='create_table_if_not_exists',
        postgres_conn_id='postgres_default',
        sql="""create table if not exists test_table(
            dt date,
            dag_id character varying,
            primary key (dt, dag_id))"""
    )
    create_table_if_not_exists
