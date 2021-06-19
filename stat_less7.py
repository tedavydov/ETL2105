from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from operators.postgres import table_list
from operators.statistic import DataStatisticPostgres
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
        dag_id="pg-data-stat",
        default_args=DEFAULT_ARGS,
        schedule_interval="@daily",
        max_active_runs=1,
        tags=['data-stat'],
) as dag1:
    for tab in table_list(connect['src']):
        sens_task = ExternalTaskSensor(
            task_id=f'{tab}_sens',
            external_dag_id="pg-data-flow",
            external_task_id=f'{tab}',
            check_existence=False,
            mode='reschedule',
            poke_interval=60 * 60,
            timeout=60 * 60 * 20,
            soft_fail=True,
            # allowed_states=[State.SUCCESS],
            retries=0,
        )  # new

        stat_task = DataStatisticPostgres(
            config={'table': f'public.{tab}'},
            query=f'select * from {tab}',
            task_id=f'{tab}_stat',
            source_pg_conn_str=connect['src'],
            pg_conn_str=connect['dest'],
            pg_meta_conn_str=connect['meta']
        )  # modify

        sens_task >> stat_task
