import psycopg2
import json


def table_gen(config):
    '''
    Создает таблицу по шаблону из файла JSON //
    config.{drop_flag}: True - удаление существующей таблицы
    config.{target_schema}.{target_table}: схема и таблица  //
    config.{json_file}: по умолч. "table_gen_settings.json" //
    :param config: словарь содержащий все нужные параметры
    :return: config: обновленный словарь
    '''
    json_f = config.get('json_file')
    table_columns_str = ''
    table_gen_str = ''
    try:
        q_schema = """CREATE SCHEMA IF NOT EXISTS {target_schema};"""
        q_drop = """DROP TABLE IF EXISTS {target_schema}.{target_table} CASCADE;"""
        q_create = """
        CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} ( 
        {columns_gen});"""
        with open(json_f, "r", encoding="utf-8") as f:
            sett = json.load(f)
            if sett:
                schema = sett.get(config.get('target_schema').lower())
                table = schema.get(config.get('target_table').lower())
                postfix = schema.get('postfix')
                table_gen_str += ", ".join(f'{k} {table[k]}' for k in table.keys())
                table_columns_str += ", ".join(f'{k}' for k in table.keys())
                if postfix:
                    table_gen_str += f'{postfix}'
        config.update(columns=table_columns_str, columns_gen=table_gen_str)
        with psycopg2.connect(config.get('pg_conn_str')) as conn, conn.cursor() as cursor:
            cursor.execute(q_schema.format(**config))
            dr = config.get('drop_flag')
            if dr:
                cursor.execute(q_drop.format(**config))
            cursor.execute(q_create.format(**config))
        return config
    # -----------------------------------
    except Exception as e:
        print(f'table_gen({config})\nERROR: {e}')


def view_from_two_tables(config):
    '''
    Создает VIEW по запросу из конфига  //
    config.{drop_flag}: True - удаление существующего VIEW
    config {view_query}: запрос для создания VIEW  //
    config {view_table}: наименование VIEW таблицы  //
    config {table_one} {key_one}: первая таблица и ключ для связи таблиц  //
    config {table_two} {key_two}: вторая таблица и ключ для связи таблиц  //
    config {col_1} {col_2}: заголовки столбцов VIEW таблицы  //
    config {columns}: перечень столбцов основной таблицы
    !!! ВАЖНО чтобы столбцы основной таблицы не совпадали со столбцами второй таблицы !!!   //
    :param config: словарь содержащий все нужные параметры
    :return: 0/1: 0 - успешное выполнение запроса
    '''
    try:
        # q_schema_src = """CREATE SCHEMA IF NOT EXISTS {source_schema};"""  # не имеет смысла
        q_schema_targ = """CREATE SCHEMA IF NOT EXISTS {target_schema};"""
        q_drop = """DROP VIEW IF EXISTS {target_schema}.{view_table};"""
        q_create = """
        CREATE OR REPLACE VIEW {target_schema}.{view_table} AS 
        SELECT t1.{key_one} {col_1}, t2.{col_2} {col_2}, {columns}
            FROM {source_schema}.{table_one} t1
            JOIN {source_schema}.{table_two} t2 ON t1.{key_one} = t2.{key_two} 
            ORDER BY {col_1}, {col_2}
        ;"""
        with psycopg2.connect(config.get('pg_conn_str')) as conn, conn.cursor() as cursor:
            cursor.execute(q_schema_targ.format(**config))
            # cursor.execute(q_schema_src.format(**config))  # не имеет смысла
            dr = config.get('drop_flag')
            if dr:
                cursor.execute(q_drop.format(**config))
            cursor.execute(q_create.format(**config))
        return 0
    # -----------------------------------
    except Exception as e:
        print(f'view_from_two_tables({config})\nERROR: {e}')
        return 1


def copy_csv_to_db(config):
    # ---------------------------------
    qr = config.get('query')
    if qr:
        copy_statement = qr.format(**config)
    else:
        copy_statement = """
        COPY {target_schema}.{target_table} ({columns}) FROM STDIN with
        DELIMITER '\t'
        CSV
        ESCAPE '\\'
        NULL '';
        """.format(**config)
    # ---------------------------------
    pg_conn = config.get('pg_conn_str')
    if pg_conn:
        pg_conn_str = pg_conn
    else:
        pg_conn_str = "host='localhost' port=5432 dbname='bi2106' user='postgres' password='PostgreS'"
    # ---------------------------------
    csv_file = config.get('csv_file')
    if csv_file:
        csv_file_name = csv_file
    else:
        csv_file_name = "./transfer_{target_schema}_{target_table}.csv".format(**config)
    # ---------------------------------
    try:
        with psycopg2.connect(pg_conn_str) as conn, conn.cursor() as cursor:
            with open(csv_file_name, 'r', encoding="utf-8") as f:
                cursor.copy_expert(copy_statement.format(**config), f)
        return 0
    except Exception as e:
        print(f'copy_csv_to_db({config})\nERROR: {e}')
        return 1


copy_statement = """
COPY {target_schema}.{target_table} ({columns}) FROM STDIN with
DELIMITER ';'
CSV
ESCAPE '\\'
NULL '';
"""

json_file = "./table_gen_settings.json"
conn_str = "host='localhost' port=5432 dbname='bi2106' user='postgres' password='PostgreS'"
config_src = dict(pg_conn_str=conn_str,
                  target_schema='public',
                  json_file=json_file,
                  drop_flag=False,
                  query=copy_statement)

# columns='mcc, description'
config_src.update(csv_file='MCC_2021-all.csv', target_table='mcc', drop_flag=True)
config_src.update(table_gen(config_src))
tr1 = copy_csv_to_db(config_src)

# columns='date, card, currency, payment, mcc, transaction'
config_src.update(csv_file='report_2021-all.csv', target_table='report', drop_flag=True)
config_src.update(table_gen(config_src))
columns_string = config_src.get('columns')
tr2 = True
if not tr1:
    tr2 = copy_csv_to_db(config_src)
    print(f'Exit code copy_csv ({str(tr2)})')

config_view = dict(pg_conn_str=conn_str,
                   source_schema='public',
                   target_schema='tableau',
                   view_table='report_view',
                   table_one='report',
                   table_two='mcc',
                   key_one='mcc',
                   key_two='mcc',
                   col_1='mcc',
                   col_2='description',
                   columns=columns_string.replace('mcc,', ''),
                   drop_flag=True)

vw = True
if not tr2:
    vw = view_from_two_tables(config_view)
    print(f'Exit code view ({str(vw)})')
