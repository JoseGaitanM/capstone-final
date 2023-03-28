from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

class PostgreSQLCountRows(BaseOperator):
    
    @apply_defaults
    def __init__(
        self,
        table_name,
        postgres_conn_id='db_connection',
        *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        query = f"SELECT COUNT(*) FROM {self.table_name};"
        cursor.execute(query)
        result = cursor.fetchone()[0]
        self.log.info(f"The table {self.table_name} has {result} rows.")
        return result