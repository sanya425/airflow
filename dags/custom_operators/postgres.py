from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgreSQLCountRows(BaseOperator):
    def __init__(self,
                 sql_to_get_schema: str,
                 sql_to_count: str,
                 table_name: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.sql_to_get_schema = sql_to_get_schema
        self.sql_to_count = sql_to_count
        self.table_name = table_name

    def execute(self, context) -> str:
        hook = PostgresHook(postgres_conn_id='postgres_local')
        schema, query = None, None
        query = hook.get_records(sql=self.sql_to_get_schema)
        for result in query:
            if 'airflow' in result:
                schema = result[0]
        query = hook.get_first(sql=self.sql_to_count.format(schema, self.table_name))
        self.log.info(f"SQL query: {self.sql_to_count}")
        self.log.info(f"Number of rows: {query}")
        if self.do_xcom_push:
            context['ti'].xcom_push(key='count_row', value=query)
        return query