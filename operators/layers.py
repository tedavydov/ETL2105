import time
import datetime
import psycopg2
from airflow.utils.decorators import apply_defaults
from utils import DataFlowBaseOperator, table_gen_str, table_list
import logging


class SalOperator(DataFlowBaseOperator):  # sae -> sal
    defaults = {
        'target_schema': 'sal',
        'source_schema': 'sae',
    }

    @apply_defaults
    def __init__(self, config, pg_conn_str, query=None, *args, **kwargs):
        super(SalOperator, self).__init__(
            config=config,
            pg_conn_str=pg_conn_str,
            *args,
            **kwargs
        )
        self.pg_conn_str = pg_conn_str
        self.config = dict(self.defaults, **config)
        self.query = query

    def execute(self, context):
        # ---------------------------------
        self.config.update(
            target_table_gen_str=table_gen_str(self.config.get('target_schema'),
                                               self.config.get('target_table')),
        )
        # ---------------------------------
        try:
            with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cur_create:
                cur_create.execute("""
                CREATE SCHEMA IF NOT EXISTS {target_schema};
                """.format(**self.config))
                self.log.info('CREATE SCHEMA: {target_schema}'.format(**self.config))
                # ---------------------------------
                q_create = """
                CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} ( 
                {target_table_gen_str} 
                );"""
                self.log.info(f'TABLE Generate: {q_create.format(**self.config)}')
                self.log.info('TABLE Columns: {target_table_gen_str}'.format(**self.config))
                cur_create.execute(q_create.format(**self.config))
                self.log.info('CREATE TABLE: {target_schema}.{target_table}'.format(**self.config))
        except Exception as e:
            self.log.info('CREATE ERROR: {target_schema}.{target_table}'.format(**self.config)
                          + f'ERROR info: {e}')
        # ---------------------------------
        with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cursor:
            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            for launch_id in ids:
                start = time.time()
                self.config.update(
                    launch_id=launch_id,
                )
                self.log.info("Ids launch_id: {}".format(launch_id))

                cols_sql = """
                select column_name
                     , data_type
                  from information_schema.columns
                 where table_schema = '{target_schema}'
                   and table_name = '{target_table}'
                   and column_name not in ('launch_id', 'effective_dttm');
                """.format(**self.config)
                cursor.execute(cols_sql)
                # ---------------------------------
                cols_list = list(cursor.fetchall())
                self.log.info("cols_list: {}".format(cols_list))
                self.log.info("self.query: {}".format(self.query))
                cols_dtypes = ",\n".join(('{}::{}'.format(col[0], col[1]) for col in cols_list))
                cols = ",\n".join(col[0] for col in cols_list)
                if self.query:
                    transfer_sql = """
                    with x as ({query})
                    insert into {target_schema}.{target_table} (launch_id, {cols})
                    select {job_id}::int as launch_id,\n{cols_dtypes}\n from x
                    """.format(query=self.query, cols_dtypes=cols_dtypes, cols=cols, **self.config)
                else:
                    transfer_sql = """
                    with x as (select * from {source_schema}.{source_table})\n 
                    insert into {target_schema}.{target_table} (launch_id, {cols})\n 
                    select {job_id}::int as launch_id,\n{cols_dtypes}\n from x 
                    """.format(cols_dtypes=cols_dtypes, cols=cols, **self.config)
                self.log.info('Executing query: {}'.format(transfer_sql))
                cursor.execute(transfer_sql)

                self.config.update(
                    source_schema=self.defaults['source_schema'],
                    duration=datetime.timedelta(seconds=time.time() - start),
                    row_count=cursor.rowcount
                )
                self.log.info('Inserted rows: {row_count}'.format(**self.config))
                self.write_etl_log(self.config)


class DdsHOperator(DataFlowBaseOperator):  # sal -> dds for hubs
    defaults = {
        'target_schema': 'dds',
        'source_schema': 'sal',
    }

    @apply_defaults
    def __init__(self, config, pg_conn_str, *args, **kwargs):
        self.config = dict(
            self.defaults,
            target_table='h_{hub_name}'.format(**config),
            hub_bk='{hub_name}_bk'.format(**config),
            **config
        )
        super(DdsHOperator, self).__init__(
            config=config,
            pg_conn_str=pg_conn_str,
            *args,
            **kwargs
        )
        self.pg_conn_str = pg_conn_str

    def execute(self, context):
        # ---------------------------------
        self.config.update(
            target_table_gen_str=table_gen_str(self.config.get('target_schema'),
                                               self.config.get('target_table')),
        )
        # ---------------------------------
        try:
            with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cur_create:
                cur_create.execute("""
                CREATE SCHEMA IF NOT EXISTS {target_schema};
                """.format(**self.config))
                self.log.info('CREATE SCHEMA: {target_schema}'.format(**self.config))
                # ---------------------------------
                q_create = """
                CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} ( 
                {target_table_gen_str} 
                );"""
                self.log.info(f'TABLE Generate: {q_create.format(**self.config)}')
                self.log.info('TABLE Columns: {target_table_gen_str}'.format(**self.config))
                cur_create.execute(q_create.format(**self.config))
                self.log.info('CREATE TABLE: {target_schema}.{target_table}'.format(**self.config))
        except Exception as e:
            self.log.info('CREATE ERROR: {target_schema}.{target_table}'.format(**self.config)
                          + f'ERROR info: {e}')
        # ---------------------------------
        with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cursor:
            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            for launch_id in ids:
                start = time.time()

                self.config.update(
                    launch_id=launch_id
                )
                clmn = self.config.get('bk_column')
                if clmn == 'hashkey':
                    bk_clmn1 = 'part_bk'
                    bk_clmn2 = 'supplier_bk'
                    insert_sql = '''
                    with x as (
                       select CONCAT({bk_clmn1}, {bk_clmn2})  
                            , {job_id} 
                         from {source_schema}.{source_table} s  
                        where {bk_clmn1} is not null 
                          and {bk_clmn2} is not null 
                          and s.launch_id = {launch_id} 
                        group by 1
                    )
                    insert into {target_schema}.{target_table} ({hub_bk}, launch_id)
                    select * from x
                        on conflict ({hub_bk})
                        do nothing;
                    '''.format(bk_clmn1=bk_clmn1, bk_clmn2=bk_clmn2, **self.config)
                else:
                    insert_sql = '''
                    with x as (
                       select {bk_column}
                            , {job_id}
                         from {source_schema}.{source_table} s 
                        where {bk_column} is not null
                          and s.launch_id = {launch_id}
                        group by 1 
                    )
                    insert into {target_schema}.{target_table} ({hub_bk}, launch_id)
                    select * from x
                        on conflict ({hub_bk})
                        do nothing;
                    '''.format(**self.config)

                self.log.info('Executing query: {}'.format(insert_sql))
                cursor.execute(insert_sql)
                # ---------------------------------
                self.config.update(
                    row_count=cursor.rowcount
                )
                self.log.info('Row count: {row_count}'.format(**self.config))
                self.config.update(
                    duration=datetime.timedelta(seconds=time.time() - start)
                )
                self.write_etl_log(self.config)


class DdsLOperator(DataFlowBaseOperator):  # sal -> dds for links
    defaults = {
        'target_schema': 'dds',
        'source_schema': 'sal',
    }

    @apply_defaults
    def __init__(self, config, pg_conn_str, *args, **kwargs):
        super(DdsLOperator, self).__init__(
            config=config,
            pg_conn_str=pg_conn_str,
            *args,
            **kwargs
        )
        self.config = dict(
            self.defaults,
            target_table='l_{l_hub_name}_{r_hub_name}'.format(**config),
            **config
        )
        self.pg_conn_str = pg_conn_str

    def execute(self, context):
        # ---------------------------------
        self.config.update(
            target_table_gen_str=table_gen_str(self.config.get('target_schema'),
                                               self.config.get('target_table')),
        )
        # ---------------------------------
        try:
            with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cur_create:
                cur_create.execute("""
                CREATE SCHEMA IF NOT EXISTS {target_schema};
                """.format(**self.config))
                self.log.info('CREATE SCHEMA: {target_schema}'.format(**self.config))
                # ---------------------------------
                q_create = """
                CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} ( 
                {target_table_gen_str} 
                );"""
                self.log.info(f'TABLE Generate: {q_create.format(**self.config)}')
                self.log.info('TABLE Columns: {target_table_gen_str}'.format(**self.config))
                cur_create.execute(q_create.format(**self.config))
                self.log.info('CREATE TABLE: {target_schema}.{target_table}'.format(**self.config))
        except Exception as e:
            self.log.info('CREATE ERROR: {target_schema}.{target_table}'.format(**self.config)
                          + f'ERROR info: {e}')
        # ---------------------------------
        with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cursor:
            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            for launch_id in ids:
                start = time.time()
                self.config.update(
                    launch_id=launch_id,
                )

                clmn_l = self.config.get('l_bk_column')
                clmn_r = self.config.get('r_bk_column')
                hub_l = self.config.get('l_hub_name')
                hub_r = self.config.get('r_hub_name')
                if clmn_l == 'hashkey' and hub_l == 'partsupp':
                    insert_sql = '''
                    with x as (
                        select distinct
                               l.partsupp_id
                             , r.{r_hub_name}_id
                          from {source_schema}.{source_table} s
                          join dds.h_partsupp l
                          on CONCAT(s.part_bk, s.supplier_bk) = l.partsupp_bk 
                          join dds.h_{r_hub_name} r
                          on s.{r_bk_column} = r.{r_hub_name}_bk
                          where s.launch_id = {launch_id}
                    )
                    insert into {target_schema}.{target_table} (partsupp_id, {r_hub_name}_id, launch_id)
                    select partsupp_id
                         , {r_hub_name}_id
                         , {job_id}
                      from x;
                    '''.format(**self.config)
                elif clmn_r == 'hashkey' and hub_r == 'partsupp':
                    insert_sql = '''
                    with x as (
                        select distinct
                               l.{l_hub_name}_id
                             , r.partsupp_id
                          from {source_schema}.{source_table} s
                          join dds.h_{l_hub_name} l
                          on s.{l_bk_column} = l.{l_hub_name}_bk
                          join dds.h_partsupp r
                          on CONCAT(s.part_bk, s.supplier_bk) = r.partsupp_bk
                          where s.launch_id = {launch_id}
                    )
                    insert into {target_schema}.{target_table} ({l_hub_name}_id, partsupp_id, launch_id)
                    select {l_hub_name}_id
                         , partsupp_id
                         , {job_id}
                      from x;
                    '''.format(**self.config)
                else:
                    insert_sql = '''
                    with x as (
                        select distinct
                               l.{l_hub_name}_id
                             , r.{r_hub_name}_id
                          from {source_schema}.{source_table} s
                          join dds.h_{l_hub_name} l
                          on s.{l_bk_column} = l.{l_hub_name}_bk
                          join dds.h_{r_hub_name} r
                          on s.{r_bk_column} = r.{r_hub_name}_bk
                          where s.launch_id = {launch_id}
                    )
                    insert into {target_schema}.{target_table} ({l_hub_name}_id, {r_hub_name}_id, launch_id)
                    select {l_hub_name}_id
                         , {r_hub_name}_id
                         , {job_id}
                      from x;
                    '''.format(**self.config)

                self.log.info('Executing query: {}'.format(insert_sql))
                cursor.execute(insert_sql)
                self.config.update(
                    row_count=cursor.rowcount,
                    duration=datetime.timedelta(seconds=time.time() - start)
                )
                self.log.info('Row count: {row_count}'.format(**self.config))
                self.write_etl_log(self.config)


class DdsSOperator(DataFlowBaseOperator):  # sal -> dds for sattelites
    defaults = {
        'target_schema': 'dds',
        'source_schema': 'sal',
    }

    @apply_defaults
    def __init__(self, config, pg_conn_str, *args, **kwargs):
        super(DdsSOperator, self).__init__(
            config=config,
            pg_conn_str=pg_conn_str,
            *args,
            **kwargs
        )
        self.config = dict(
            self.defaults,
            target_table='s_{hub_name}_{satellite_name}'.format(**config),
            **config
        )
        self.pg_conn_str = pg_conn_str

    def execute(self, context):
        # ---------------------------------
        self.config.update(
            target_table_gen_str=table_gen_str(self.config.get('target_schema'),
                                               self.config.get('target_table')),
        )
        # ---------------------------------
        try:
            with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cur_create:
                cur_create.execute("""
                CREATE SCHEMA IF NOT EXISTS {target_schema};
                """.format(**self.config))
                self.log.info('CREATE SCHEMA: {target_schema}'.format(**self.config))
                # ---------------------------------
                q_create = """
                CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} ( 
                {target_table_gen_str} 
                );"""
                self.log.info(f'TABLE Generate: {q_create.format(**self.config)}')
                self.log.info('TABLE Columns: {target_table_gen_str}'.format(**self.config))
                cur_create.execute(q_create.format(**self.config))
                self.log.info('CREATE TABLE: {target_schema}.{target_table}'.format(**self.config))
        except Exception as e:
            self.log.info('CREATE ERROR: {target_schema}.{target_table}'.format(**self.config)
                          + f'ERROR info: {e}')
        # ---------------------------------
        with psycopg2.connect(self.pg_conn_str) as conn, conn.cursor() as cursor:
            self.config.update(
                job_id=context['task_instance'].job_id,
                dt=context["task_instance"].execution_date,
            )
            ids = self.get_launch_ids(self.config)
            self.log.info("Ids found: {}".format(ids))

            for launch_id in ids:
                start = time.time()
                self.config.update(
                    launch_id=launch_id,
                )

                clmn = self.config.get('bk_column')
                h_hub = self.config.get('hub_name')
                if clmn == 'hashkey' and h_hub == 'partsupp':
                    insert_sql = '''
                    with x as (
                        select distinct
                               h.partsupp_id
                             , s.{satellite_name} 
                          from sal.partsupp s
                          join dds.h_partsupp h
                          on CONCAT(s.part_bk, s.supplier_bk) = h.partsupp_bk
                          where s.launch_id = {launch_id}
                    )
                    insert into {target_schema}.{target_table} (partsupp_id, {satellite_name}, launch_id)
                    select partsupp_id
                         , {satellite_name}
                         , {job_id}
                      from x;
                    '''.format(**self.config)
                else:
                    insert_sql = '''
                    with x as (
                        select distinct
                               s.{hub_name}_bk
                             , s.{satellite_name}
                          from {source_schema}.{source_table} s
                          join dds.h_{hub_name} h
                          on s.{bk_column} = h.{hub_name}_bk
                          where s.launch_id = {launch_id}
                    )
                    insert into {target_schema}.{target_table} ({hub_name}_id, {satellite_name}, launch_id)
                    select {hub_name}_bk
                         , {satellite_name}
                         , {job_id}
                      from x;
                    '''.format(**self.config)

                self.log.info('Executing query: {}'.format(insert_sql))
                cursor.execute(insert_sql)
                self.config.update(
                    row_count=cursor.rowcount,
                    duration=datetime.timedelta(seconds=time.time() - start)
                )
                self.log.info('Row count: {row_count}'.format(**self.config))
                self.write_etl_log(self.config)
