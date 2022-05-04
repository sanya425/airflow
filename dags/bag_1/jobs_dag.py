import random
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from custom_operators.postgres import PostgreSQLCountRows

config = {
    'dag_id_1': {
        "start_date": datetime(2022, 1, 1),
        "table_name": "table_name_1"},
    'dag_id_2': {
        "start_date": datetime(2022, 1, 1),
        "table_name": "table_name_2"}
}


def create_dag(dag_id, default_args):
    def print_process_start() -> str:
        """
        Print DB`s name
        :return: string with name db
        """
        return f"{dag_id} start processing tables in database: PostgreSQL"

    def get_schema(hook, sql_to_get_schema: str) -> str:
        """
        :return schema name
        """
        query = hook.get_records(sql=sql_to_get_schema)
        for result in query:
            if 'airflow' in result:
                schema = result[0]
                print(schema)
                return schema

    def make_query(hook, schema, sql_query, table_name):
        """
         make query to table by schema
        :return: first row of query
        """
        query = hook.get_first(sql=sql_query.format(schema, table_name))
        return query

    def _check_table_exist(sql_to_get_schema, sql_to_check_table_exist, table_name):
        """
        callable function to get schema name and after that check if table exist
        :return: first row of query
        """
        hook = PostgresHook(postgres_conn_id='postgres_local')
        schema = get_schema(hook, sql_to_get_schema)
        query = make_query(hook, schema, sql_to_check_table_exist, table_name)
        print("Query:", query)
        query = "dummy_pass" if query else "create_table"
        print("Query:", query)
        return query

    def end_process(task_instance):
        """
        Push to the Xcom dag_id
        :return: None
        """
        val = "{{ run_id }} ended"
        task_instance.xcom_push(key="result_of_task", value=val)

    with DAG(dag_id=dag_id, schedule_interval='*/4 * * * *', default_args=default_args, catchup=False) as dag:
        print_process_start = PythonOperator(
            task_id="print_process_start",
            python_callable=print_process_start
        )
        get_current_user = BashOperator(
            task_id='get_current_user',
            bash_command='whoami',
            do_xcom_push=True
        )
        my_table_name = 'my_shop'

        check_table_exist = BranchPythonOperator(
            task_id="check_table_exist",
            python_callable=_check_table_exist,
            op_args=["SELECT * FROM pg_tables;",
                     """ SELECT * FROM information_schema.tables 
                        WHERE table_schema = '{}'
                        AND table_name = '{}';""",
                     my_table_name],
            provide_context=True,

        )

        create_table = PostgresOperator(
            task_id="create_table",
            sql=f"""CREATE TABLE {my_table_name}(custom_id integer NOT NULL,
                                        user_name VARCHAR (150) NOT NULL, 
                                        timestamp TIMESTAMP NOT NULL); """,
        )

        dummy_pass = DummyOperator(task_id='dummy_pass')

        insert_new_row = PostgresOperator(
            task_id="insert_new_row",
            sql=f"""
                INSERT INTO {my_table_name}
                VALUES ( %s, '{{{{ ti.xcom_pull(task_ids='get_current_user', key='return_value') }}}}', %s);
                """,
            parameters=(random.randint(1, 100), datetime.now()),
            trigger_rule=TriggerRule.NONE_FAILED,
            postgres_conn_id='postgres_local',
        )

        query_the_table = PostgreSQLCountRows(
            task_id="query_the_table",
            sql_to_get_schema="SELECT * FROM pg_tables;",
            sql_to_count=f"SELECT COUNT(*) FROM {my_table_name};",
            table_name=my_table_name,
            do_xcom_push=True
        )

        end_dag = PythonOperator(
            task_id='end_dag',
            python_callable=end_process
        )

        print_process_start >> get_current_user >> check_table_exist
        check_table_exist >> [create_table, dummy_pass] >> insert_new_row >> query_the_table >> end_dag
    return dag


for d_id, args in config.items():
    dag_id = d_id
    default_args = args
    globals()[dag_id] = create_dag(dag_id, default_args)
