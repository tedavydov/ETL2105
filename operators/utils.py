import psycopg2
import logging
import json
from airflow.models import BaseOperator


def table_gen_str(schema_name_str, table_name_str,
                  json_file="./dags/operators/table_gen_settings.json"):
    '''
    Считывает параметры таблицы из файла JSON
    :param table_name_str: строка - имя таблицы
    :param json_file: по умолч. "table_gen_settings.json"
    :return: строка подставляемая в запрос для создания таблицы
    '''
    tbl_str = ''
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            sett = json.load(f)
            if sett:
                schema = sett.get(schema_name_str.lower())
                table = schema.get(table_name_str.lower())
                postfix = schema.get('postfix')
                # for k in table.keys():
                #     tbl_str += f'{k} {table[k]}, '
                tbl_str += ", ".join(f'{k} {table[k]}' for k in table.keys())
                if postfix:
                    tbl_str += f'{postfix}'
                logging.info(f'Log table_gen_str: '
                             f'\nschema : {schema_name_str} . table : {table_name_str}'
                             f'\npostfix : {postfix}'
                             f'\nresult : {tbl_str}')
                return tbl_str
    # -----------------------------------
    except Exception as e:
        # print('=' * 100, f'\n ERROR JSON LOAD: {schema_name_str}.{table_name_str} for file {file_name}\nERROR info: {e}')
        logging.info(f'ERROR JSON LOAD: table_gen_str({schema_name_str}.{table_name_str}) ERROR info: {e}')


class DataFlowBaseOperator(BaseOperator):
    def __init__(self, pg_meta_conn_str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pg_meta_conn_str = pg_meta_conn_str

    def write_etl_log(self, config):
        with psycopg2.connect(self.pg_meta_conn_str) as conn, conn.cursor() as cursor:
            query = '''
            insert into log (
                   source_launch_id
                 , target_schema
                 , target_table
                 , target_launch_id
                 , row_count
                 , duration
                 , load_date
            )
            select {launch_id}
                , '{target_schema}'
                , '{target_table}'
                , {job_id}
                , {row_count}
                , '{duration}'
                , '{dt}'
            '''
            cursor.execute(query.format(**config))
            logging.info('Log update: {target_table} : {job_id}'.format(**config))
            conn.commit()

    def write_etl_statistic(self, config):
        with psycopg2.connect(self.pg_meta_conn_str) as conn, conn.cursor() as cursor:
            query = '''
            insert into statistic (
                   table_name
                 , column_name
                 , cnt_null
                 , cnt_full
                 , load_date
            )
            with x as (
                select '{table}' as table_name
                     , '{column}' as column_name
                     , {cnt_null} as cnt_null
                     , {cnt_full} as cnt_full
                     , {job_id} as launch_id
            )
            select table_name
                 , column_name
                 , cnt_null
                 , cnt_full
                 , '{dt}' as load_date
              from x left join log l
                on x.launch_id = l.target_launch_id
            '''
            cursor.execute(query.format(**config))
            conn.commit()

    def get_load_dates(self, config):
        with psycopg2.connect(self.pg_meta_conn_str) as conn, conn.cursor() as cursor:
            query = '''
            select array_agg(distinct load_date order by load_date)
                from log
                where target_table = '{target_table}'
                and target_schema = '{target_schema}'
                and source_launch_id = -1
            '''
            cursor.execute(query.format(**config))
            dates = cursor.fetchone()[0]
        if dates:
            return dates
        else:
            return []
