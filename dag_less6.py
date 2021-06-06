from airflow import DAG
from operators.postgres import DataTransferPostgres, table_list
from datetime import datetime

connect = {'src': "host='db1' port=5432 dbname='my_database' user='root' password='postgres'",
           'dest': "host='db2' port=5432 dbname='my_database' user='root' password='postgres'"}

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
    # tables = table_list(connect['src'])
    for tab in table_list(connect['src']):
        tab_task = DataTransferPostgres(
        config={'table': f'public.{tab}'},
        query=f'select * from {tab}',
        task_id=f'{tab}',
        source_pg_conn_str=connect['src'],
        pg_conn_str=connect['dest'],
        )
    # tab0_task = DataTransferPostgres(
    #     config={'table': f'public.{tables[0]}'},
    #     query=f'select * from {tables[0]}',
    #     task_id=f'{tables[0]}',
    #     source_pg_conn_str=connect['src'],
    #     pg_conn_str=connect['dest'],
    # )
    # tab1_task = DataTransferPostgres(
    #     config={'table': f'public.{tables[1]}'},
    #     query=f'select * from {tables[1]}',
    #     task_id=f'{tables[1]}',
    #     source_pg_conn_str=connect['src'],
    #     pg_conn_str=connect['dest'],
    # )
    # tab2_task = DataTransferPostgres(
    #     config={'table': f'public.{tables[2]}'},
    #     query=f'select * from {tables[2]}',
    #     task_id=f'{tables[2]}',
    #     source_pg_conn_str=connect['src'],
    #     pg_conn_str=connect['dest'],
    # )
    # tab3_task = DataTransferPostgres(
    #     config={'table': f'public.{tables[3]}'},
    #     query=f'select * from {tables[3]}',
    #     task_id=f'{tables[3]}',
    #     source_pg_conn_str=connect['src'],
    #     pg_conn_str=connect['dest'],
    # )
    # tab4_task = DataTransferPostgres(
    #     config={'table': f'public.{tables[4]}'},
    #     query=f'select * from {tables[4]}',
    #     task_id=f'{tables[4]}',
    #     source_pg_conn_str=connect['src'],
    #     pg_conn_str=connect['dest'],
    # )
    # tab5_task = DataTransferPostgres(
    #     config={'table': f'public.{tables[5]}'},
    #     query=f'select * from {tables[5]}',
    #     task_id=f'{tables[5]}',
    #     source_pg_conn_str=connect['src'],
    #     pg_conn_str=connect['dest'],
    # )
    # tab6_task = DataTransferPostgres(
    #     config={'table': f'public.{tables[6]}'},
    #     query=f'select * from {tables[6]}',
    #     task_id=f'{tables[6]}',
    #     source_pg_conn_str=connect['src'],
    #     pg_conn_str=connect['dest'],
    # )
    # tab7_task = DataTransferPostgres(
    #     config={'table': f'public.{tables[7]}'},
    #     query=f'select * from {tables[7]}',
    #     task_id=f'{tables[7]}',
    #     source_pg_conn_str=connect['src'],
    #     pg_conn_str=connect['dest'],
    # )
    # # Порядок выполнения тасок
    # # tab0_task >> tab1_task >> tab2_task >> tab3_task >> tab4_task >> tab5_task >> tab6_task >> tab7_task
    # tab0_task
    # tab1_task
    # tab2_task
    # tab3_task
    # tab4_task
    # tab5_task
    # tab6_task
    # tab7_task
