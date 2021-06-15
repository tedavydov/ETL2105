from utils import DataFlowBaseOperator

import psycopg2
import datetime
import time


class DataStatisticPostgres(DataFlowBaseOperator):
    def __init__(
            self, config,
            source_pg_conn_str, pg_conn_str, pg_meta_conn_str,
            query, date_check=False, *args, **kwargs
    ):
        super(DataStatisticPostgres, self).__init__(
            pg_meta_conn_str,
            *args,
            **kwargs
        )
        self.config = config
        self.pg_conn_str = pg_conn_str
        self.source_pg_conn_str = source_pg_conn_str
        self.query = query
        self.date_check = date_check
        # self.pg_meta_conn_str = pg_meta_conn_str

    def execute(self, context):
        schema_name = "{table}".format(**self.config).split(".")
        self.config.update(
            target_schema=schema_name[0],
            target_table=schema_name[1],
            job_id=context["task_instance"].job_id,  # modify
            dt=context["task_instance"].execution_date,  # modify
        )
        # modify
        if self.date_check \
                and context["execution_date"] in self.get_load_dates(self.config):
            logging.info("Data already load")
            return
        with psycopg2.connect(self.pg_conn_str) as conn:
            table = self.config['target_table']
            with conn.cursor() as cursor:
                start = time.time()  # modify
                # modify
                cursor.execute(
                    """
                select column_name
                  from information_schema.columns
                 where table_schema = '{target_schema}'
                   and table_name = '{target_table}'
                   and column_name not in ('launch_id', 'effective_dttm');
                """.format(
                        **self.config
                    )
                )

                columns_fetch = cursor.fetchall()
                columns = ", ".join('"{}"'.format(row) for row, in columns_fetch)
                self.config.update(columns=columns)
                with conn.cursor() as column_cursor:
                    columns_data = {}
                    for column in list(columns.split(', ')):
                        column_cur = {}
                        # -----------------------------------------------------------
                        column_cursor.execute(f"select count({column}) from {table}")
                        cnt_full_data = column_cursor.fetchone()[0]
                        column_cur.update(cnt_full=cnt_full_data)
                        self.config.update(cnt_full=cnt_full_data)
                        # -----------------------------------------------------------
                        column_cursor.execute(f"select count({column}) from {table} "
                                              f"where {column} is null;")
                        cnt_null_data = column_cursor.fetchone()[0]
                        column_cur.update(cnt_null=cnt_null_data)
                        self.config.update(cnt_null=cnt_null_data)
                        # -----------------------------------------------------------
                        dct = {f'{column}': column_cur}
                        columns_data.update(dct)
                        self.config.update(columns_data=columns_data)
                        self.config.update(column=column,
                                           duration=datetime.timedelta(seconds=time.time() - start))
                        self.write_etl_statistic(self.config)  # new
                        # -----------------------------------------------------------
                self.log.info("writing succed")
