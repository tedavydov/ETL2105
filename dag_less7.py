from airflow import DAG
from operators.postgres import DataTransferPostgres, table_list
from datetime import datetime

connect = {'src': "host='db1' port=5432 dbname='my_database' user='root' password='postgres'",
           'dest': "host='db2' port=5432 dbname='my_database' user='root' password='postgres'",
           'meta': "host='db2' port=5432 dbname='my_database' user='root' password='postgres'"}

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 25),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

with DAG(
        dag_id="pg-data-flow",
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        max_active_runs=1,
        tags=['data-flow'],
) as dag1:
    for tab in table_list(connect['src']):
        tab_task = DataTransferPostgres(
            config={'table': f'public.{tab}'},
            query=f'select * from {tab} limit 500000',  # добавил ограничение - на слабой виртуалке не тянет
            task_id=f'{tab}',
            source_pg_conn_str=connect['src'],
            pg_conn_str=connect['dest'],
            pg_meta_conn_str=connect['meta'])  # modify
