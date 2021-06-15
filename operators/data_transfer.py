import logging
import time
import datetime
import psycopg2
from airflow.utils.decorators import apply_defaults
from utils import DataFlowBaseOperator, table_gen_str


class DataTransfer(DataFlowBaseOperator):  # modify
    @apply_defaults
    def __init__(self, config, table_src,
                 pg_conn_str,
                 pg_meta_conn_str,
                 date_check=False, *args, **kwargs):
        super(DataTransfer, self).__init__(
            pg_meta_conn_str,
            *args,
            **kwargs
        )
        self.config = config
        self.table_src = table_src
        self.pg_conn_str = pg_conn_str
        self.date_check = date_check
        # self.pg_meta_conn_str = pg_meta_conn_str

    def provide_data(self, csv_file, context):
        pass

    def execute(self, context):
        copy_statement = """
        COPY {target_schema}.{target_table} ({columns}) FROM STDIN with
        DELIMITER '\t'
        CSV
        ESCAPE '\\'
        NULL '';
        """
        schema_name = "{table}".format(**self.config).split(".")
        self.config.update(
            target_schema=schema_name[0],
            target_table=schema_name[1],
            target_table_gen_str=table_gen_str(schema_name[0], schema_name[1]),
            job_id=context["task_instance"].job_id,  # modify
            dt=context["task_instance"].execution_date,  # modify
        )
        # modify
        if self.date_check \
                and context["execution_date"] in self.get_load_dates(self.config):
            logging.info("Data already load")
            return
        with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cursor:
            start = time.time()  # modify
            # ---------------------------------
            try:
                cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS {target_schema};
                """.format(**self.config))
                self.log.info('CREATE SCHEMA: {target_schema}'.format(**self.config))
                # ---------------------------------
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} ( 
                {target_table_gen_str} 
                );""".format(**self.config))
                self.log.info('CREATE TABLE: {target_schema}.{target_table}'.format(**self.config))
            except Exception as e:
                self.log.info('CREATE ERROR: {target_schema}.{target_table}'.format(**self.config)
                              + f'ERROR info: {e}')
            # ---------------------------------
            cursor.execute("""
            select column_name
              from information_schema.columns
             where table_schema = '{target_schema}'
               and table_name = '{target_table}'
               and column_name not in ('launch_id', 'effective_dttm');
            """.format(**self.config))
            # ---------------------------------
            result = cursor.fetchall()
            columns = ", ".join('"{}"'.format(row) for row, in result)
            self.config.update(columns=columns)

            csv_file_name = f"./dags/transfer_{self.table_src}_to_{schema_name[0]}({schema_name[1]}).csv"

            with open(csv_file_name, "w", encoding="utf-8") as csv_file:
                self.provide_data(csv_file, context)

            self.log.info("writing succed")

            with open(csv_file_name, 'r', encoding="utf-8") as f:
                cursor.copy_expert(copy_statement.format(**self.config), f)

            self.config.update(  # modify
                launch_id=-1,
                duration=datetime.timedelta(seconds=time.time() - start),
                row_count=cursor.rowcount
            )
            self.write_etl_log(self.config)  # modify
