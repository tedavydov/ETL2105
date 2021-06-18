import csv
import psycopg2
from data_transfer import DataTransfer
from utils import table_list


class DataTransferPostgres(DataTransfer):
    def __init__(
            self, config, query, task_id, table_src,
            source_pg_conn_str, pg_conn_str, pg_meta_conn_str,
            *args, **kwargs
    ):
        super(DataTransferPostgres, self).__init__(
            config=config,
            query=query, task_id=task_id, table_src=table_src,
            # source_pg_conn_str=source_pg_conn_str,
            pg_conn_str=pg_conn_str,
            pg_meta_conn_str=pg_meta_conn_str,
            *args, **kwargs
        )
        self.source_pg_conn_str = source_pg_conn_str
        self.query = query

    def provide_data(self, csv_file, context):
        pg_conn = psycopg2.connect(self.source_pg_conn_str)
        pg_cursor = pg_conn.cursor()
        query_to_execute = self.query
        self.log.info("Executing query: {}".format(query_to_execute))
        pg_cursor.execute(query_to_execute)
        csvwriter = csv.writer(
            csv_file,
            delimiter="\t",
            quoting=csv.QUOTE_NONE,
            lineterminator="\n",
            escapechar='\\'
        )

        while True:
            rows = pg_cursor.fetchmany(size=1000)
            if rows:
                for row in rows:
                    _row = list(row)
                    csvwriter.writerow(_row)
            else:
                break
        pg_conn.close()
