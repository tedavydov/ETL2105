import psycopg2


# import pandas as pd
# import pprint
# import traceback


# from https://gist.github.com/viewpointsa/bba4d475126ec4ef9427fd3c2fdaf5c1
# def copy_table(connectionStringSrc, connectionStringDst, table_name, verbose=False, condition=""):
def copy_tables(connectionStringSrc, connectionStringDst):
    with psycopg2.connect(connectionStringSrc) as connSrc:
        with psycopg2.connect(connectionStringDst) as connDst:
            # Получаем список таблиц из исходной БД :
            with connSrc.cursor() as curSrc1:
                curSrc1.execute("""SELECT table_name 
                                FROM information_schema.tables 
                                WHERE table_schema = 'public'""")
                for tbl_X in curSrc1.fetchall():
                    table_name = tbl_X[0]
                    # =======================================================
                    # создаем таблицу в новой БД по информации из исходной БД
                    qr2 = f"CREATE TABLE IF NOT EXISTS {table_name} ("
                    with connSrc.cursor() as curSrc2:
                        # информация из исходной БД
                        curSrc2.execute(f"""SELECT column_name, data_type, character_maximum_length
                                                    FROM information_schema.columns
                                                    WHERE table_name = '{table_name}';""")
                        # =========================================
                        for ix, el in enumerate(curSrc2.fetchall()):
                            if ix != 0:
                                qr2 += ","
                            qr2 += f" {el[0]} {el[1]}"
                            if el[2]:
                                qr2 += f"({el[2]})"
                        # создаем таблицу в новой БД
                        qr2 += ')'
                        with connDst.cursor() as curDst0:
                            curDst0.execute(qr2)
                    # =======================================================
                    condition = ""
                    # query1 = "SELECT * FROM {} {};".format(table_name, condition)
                    query1 = "SELECT * FROM {} {}".format(table_name, condition)
                    queryCOUNT = f"SELECT COUNT(*) FROM {table_name};"
                    with connDst.cursor() as curDst:
                        with connSrc.cursor() as curSrc3:
                            # ================================================
                            curSrc3.execute(queryCOUNT)
                            for cur in curSrc3.fetchall():
                                if int(cur[0]) > 100000:
                                    query1 += " limit 100000;"
                                else:
                                    query1 += ";"
                                print(query1)
                                print(f"Table {table_name} SOURCE count = {cur[0]}")
                            # ================================================
                            curSrc3.execute(query1)
                            for row in curSrc3.fetchall():
                                # generate %s x columns
                                query_columns = ','.join([el[0] for el in curSrc3.description])
                                query_values = ','.join('%s' for x in range(len(curSrc3.description)))
                                query2 = "INSERT INTO {} ({}) VALUES ({});".format(table_name, query_columns, query_values)
                                param = [item for item in row]
                                # print curDst.mogrify(query,param )
                                # import pdb; pdb.set_trace()  # дебагер - отладка программы в интерактивном режиме
                                curDst.execute(query2, param)
                    # =======================================================
                    queryEND = f"SELECT * FROM {table_name} limit 3;"
                    with connDst.cursor() as curDstEND:
                        # ================================================
                        curDstEND.execute(queryCOUNT)
                        for cur in curDstEND.fetchall():
                            print(f"Table {table_name} DESTINATION count = {cur[0]}")
                        # ================================================
                        curDstEND.execute(queryEND)
                        for row in curDstEND.fetchall():
                            print(f"{row[0]} {row[1]}")
                    print(f"copy table {table_name} is OK")
                    # =======================================================


def etl_proc():
    conn_string1 = "host='localhost' port=54320 dbname='my_database' user='root' password='postgres'"
    conn_string2 = "host='localhost' port=5433 dbname='my_database' user='root' password='postgres'"
    try:
        copy_tables(conn_string1, conn_string2)
    except Exception as e:
        print(f'ERROR copy_table() Exception: {e}')


if __name__ == '__main__':
    etl_proc()
