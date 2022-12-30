import configparser
import datetime
import sys
import traceback
import pandas as pd

import config.basic_config as con
from utils import MySQLWrapper as Mysql
from utils.aws_utils import S3Utils
from utils.customlogger import basiclogger
# from utils.common_utils import CommonUtils
from start_ingestion import read_control_partition_config

update_config_logger = basiclogger(logname=f"UpdateConfig_default")
update_config_except_logger = basiclogger(logname=f"UpdateConfig_exceptions", section=f"EXCEPTION")

db_host = f'10.6.0.36'  # nrtdbgeo  f'10.210.12.4'  # tugelav2
dl_target = f's3://databi-testing/Tugela/datalake_data'
source_dbs = (f'lendingstream', f'drafty')
full_tables = (f'lendingstream.loan', f'drafty.loan')

partition_by_yyyy = (f'lendingstream.transactions', f'lendingstream.ledger',)
partition_by_yyyymm = (f'lendingstream.application', f'lendingstream.decision_info',)
partition_by_yyyymmdd = (f'lendingstream.multitier_lead_info',
                         f'drafty.multitier_lead_info',
                         f'lendingstream.ruleservice_response',
                         f'lendingstream.riskprofile_eligibility_history',
                         f'lendingstream.application_history',
                         f'lendingstream.application_abtf_variants',
                         f'drafty.application_profile_update',
                         f'lendingstream.request_tracker',
                         f'lendingstream.application_state_history',)

control_config_columns = [f'lake_ingestion_id', f'ingestion_type', f'servername',
                          f'databasename', f'tablename', f'delta_field',
                          f'delta_field_expr', f'date_not_available',
                          f'active_indicator', f'invalidated_by',
                          f'crawler_name', f'glue_db_name', f'target_type',
                          f'target_location', f'primary_key', f'athena_view_db',
                          f'athena_view_name', f'view_needed', f'partition_config',
                          f'airflow_dag_group', f'update_datetime']

config = configparser.ConfigParser()
config.read(con.DB_CONFIG)


class ConfigData:
    """
    This class holds all the methods for generating excel config
    dataframe based on the supplied input parameters.
    i.e. dbname = if None, will fetch from the config,
    tables = if None, will fetch from the mysql's information_schema.tables
        for the respective db
    Based on this information, the existing config is cross-verified and only the missing
    dbname.tablename combination is considered for adding to the new Excel,
    which will copy the existing Excel and append to form the <filename>_new.xlsx
    """

    max_ingestion_id = 0  # variable for tracking the latest ingestion id

    def __init__(self, dbname, current_config, tables=None):
        self.tables_list = tables
        self.dbname = dbname
        self.read_con = Mysql.MySQLWrapper(section=self.dbname.upper())
        self.control_file = current_config

    def get_tables(self):
        if self.tables_list is None:
            list_query = f"select table_name from information_schema.tables where " \
                         f"table_schema = '{self.dbname}' and table_type = 'BASE TABLE' "
            self.tables_list = self.read_con.processQuery(list_query)
            # convert the tuple of dict to list
            if len(self.tables_list) == 0:
                self.tables_list = []
            else:
                tb = []
                for t in self.tables_list:
                    tb.append(t['table_name'])
                self.tables_list = tb
        existing_tables = \
            self.control_file[self.control_file[f'databasename'] == self.dbname][f'tablename']
        existing_tables = existing_tables.to_list()
        self.tables_list = [x for x in self.tables_list if x not in existing_tables]

    def is_created_datetime_available(self, tablename):
        check_created_datetime_query = f"select column_name " \
                                       f"from information_schema.columns " \
                                       f"where table_schema = '{self.dbname}' " \
                                       f"and table_name = '{tablename}' " \
                                       f"and column_name = 'created_datetime' "
        check_created_datetime = self.read_con.processQuery(check_created_datetime_query)
        if len(check_created_datetime) > 0:
            return True
        else:
            return False

    def is_modified_datetime_available(self, tablename):
        check_modified_datetime_query = f"select column_name " \
                                        f"from information_schema.columns " \
                                        f"where table_schema = '{self.dbname}' " \
                                        f"and table_name = '{tablename}' " \
                                        f"and column_name = 'modified_datetime' "
        check_modified_datetime = self.read_con.processQuery(check_modified_datetime_query)
        if len(check_modified_datetime) > 0:
            return True
        else:
            return False

    def is_auto_increment_available(self, tbl):
        check_auto_inc_query = f"select column_name " \
                               f"from information_schema.columns " \
                               f"where table_schema = '{self.dbname}' " \
                               f"and table_name = '{tbl}' " \
                               f"and extra = 'auto_increment'"
        check_auto_inc = self.read_con.processQuery(check_auto_inc_query)
        if len(check_auto_inc) > 0:
            return check_auto_inc[0]['column_name']
        else:
            return False

    def calc_max_ingestion_id(self):
        max_id = self.control_file[f'lake_ingestion_id'].max()
        ConfigData.max_ingestion_id = ConfigData.max_ingestion_id + 1
        max_ingest = ConfigData.max_ingestion_id + max_id
        return max_ingest

    def check_datefield_availability(self, tbl):
        is_datefield = 0 if (self.is_modified_datetime_available(tbl)
                             or self.is_created_datetime_available(tbl)) else 1
        return is_datefield

    def check_table_need_for_ingestion(self, tbl):
        # if either created_datetime or modified_datetime is available
        # or if any primary keys are there, then we can ensure this table is valid
        # for considering into lake ingestion
        if (self.is_created_datetime_available(tbl)
                or self.is_modified_datetime_available(tbl)
                or self.is_auto_increment_available(tbl)):
            return 1
        else:
            return 0

    def update_delta_field(self, tbl):
        if self.is_created_datetime_available(tbl):
            return f'created_datetime'
        elif self.is_modified_datetime_available(tbl):
            return f'modified_datetime'
        elif self.is_auto_increment_available(tbl):
            return self.is_auto_increment_available(tbl)
        else:
            return f''

    def check_update_delta_expr(self, tbl):
        if self.is_created_datetime_available(tbl) \
                and self.is_modified_datetime_available(tbl):
            return f'coalesce(modified_datetime, created_datetime)'
        else:
            return f''

    def check_update_primary_keys(self, tbl):
        check_p_key = f"select COLUMN_NAME from information_schema.columns " \
                      f"where table_schema = '{self.dbname}' " \
                      f"and table_name = '{tbl}' and COLUMN_KEY = 'PRI'"
        p_keys = self.read_con.processQuery(check_p_key)

        # check for multiple primary keys and convert the output to str
        p_key = f""
        for p in p_keys:
            p_key = p_key + f"{p[f'COLUMN_NAME']},"

        if p_key == f"":
            return p_key
        else:
            return p_key[:-1]

    def update_partition_config(self, tbl):
        # Partition rules
        # 1. For created_datetime/modified_datetime tables, default value : YYYYMMDD
        # 2. For tables that do not have any datetime, but has auto_increment field,
        # default value : for every 1M rows, the range partition would be created.
        # For ex : id <= 1000000 --> 1M;
        # id > 1000001 and id <= 2000000 --> 2M and so on..

        # chk_name = f'{self.dbname}.{tbl}'
        # if chk_name in partition_by_yyyy:
        #     part_format = f'YYYY'
        # elif chk_name in partition_by_yyyymm:
        #     part_format = f'YYYYMM'
        # elif chk_name in partition_by_yyyymmdd:
        #     part_format = f'YYYYMMDD'
        # else:
        #     part_format = f'XXXX'  # we will not consider this

        if self.is_created_datetime_available(tbl):
            part_col = f'created_datetime'
            part_format = f'YYYYMMDD'
            part_type = f'time-based'
        elif self.is_modified_datetime_available(tbl):
            part_col = f'modified_datetime'
            part_format = f'YYYYMMDD'
            part_type = f'time-based'
        else:
            col_name = self.is_auto_increment_available(tbl)
            if col_name:
                part_col = col_name
                part_type = f'non-time-based'
                part_format = f'1M'
            else:
                part_col = None
                part_type = None
                part_format = None

        if part_col is not None and part_type is not None:  # self.is_created_datetime_available(tbl) and
            # part_format[:4] == 'YYYY':
            part_tuple = ({f'partition_seq_no': 1,
                           f'partition_column_type': part_type,
                           f'partition_column': part_col,
                           f'partition_format': part_format},)
        else:
            part_tuple = ()

        return part_tuple

    def check_view_needed(self, tbl):
        chk_view = f'{self.dbname}.{tbl}'
        if chk_view in partition_by_yyyy \
                or chk_view in partition_by_yyyymm \
                or chk_view in partition_by_yyyymmdd:
            return 0
        else:
            return 1

    def update_config_per_table(self, t_name):
        row_dict = {}
        for k in control_config_columns:
            if k == f'lake_ingestion_id':
                row_dict[k] = self.calc_max_ingestion_id()
            elif k == f'ingestion_type':
                row_dict[k] = f'mysql-table'
            elif k == f'servername':
                row_dict[k] = db_host
            elif k == f'databasename':
                row_dict[k] = self.dbname
            elif k == f'tablename':
                row_dict[k] = t_name
            elif k == f'delta_field':
                row_dict[k] = self.update_delta_field(t_name)
            elif k == f'delta_field_expr':
                row_dict[k] = self.check_update_delta_expr(t_name)
            elif k == f'date_not_available':
                row_dict[k] = self.check_datefield_availability(t_name)
            elif k == f'active_indicator':
                row_dict[k] = self.check_table_need_for_ingestion(t_name)
            elif k == f'invalidated_by':
                row_dict[k] = f'NULL'  # by default NULL
            elif k == f'crawler_name':
                row_dict[k] = f'datalake_{self.dbname}'
            elif k == f'glue_db_name':
                row_dict[k] = f'dl_{self.dbname}'
            elif k == f'target_type':
                row_dict[k] = f's3'
            elif k == f'target_location':
                row_dict[k] = dl_target
            elif k == f'primary_key':
                row_dict[k] = self.check_update_primary_keys(t_name)
            elif k == f'athena_view_db':
                row_dict[k] = f'dl_{self.dbname}_lv'
            elif k == f'athena_view_name':
                row_dict[k] = f'{t_name}_lv'
            elif k == f'view_needed':
                row_dict[k] = 1  # self.check_view_needed(t_name)
            elif k == f'partition_config':
                row_dict[k] = self.update_partition_config(t_name)
            elif k == f'airflow_dag_group':
                row_dict[k] = f'dag_{self.dbname}'
            elif k == f'update_datetime':
                row_dict[k] = str(datetime.datetime.now())
        return row_dict

    def generate_config_per_db(self):
        # empty list of dictionaries for all the tables within the given db
        dfi = []
        if self.tables_list is None:
            self.get_tables()
        if self.tables_list is None:
            update_config_logger.info(f"All tables in {self.dbname} are already added "
                                      f"to the config or there was no tables given!! ")
        else:
            for table in self.tables_list:
                dfi.append(self.update_config_per_table(table))

        return dfi


if __name__ == f"__main__":
    # noinspection PyBroadException
    try:

        #  dbname param is mandatory; whereas tablename param is optional
        input_dbs = sys.argv[1].split(',') if len(sys.argv) > 1 else source_dbs
        input_tables = sys.argv[2].split(',') if len(sys.argv) > 2 else None

        update_config_logger.info(f"Fetch the current control config from s3")
        curr_config = read_control_partition_config(con.s3_service_name,
                                                    con.aws_config_section)

        list_dic = []
        for i in input_dbs:
            update_config_logger.info(f"Start updating the tables for {i}")
            db = ConfigData(dbname=i, current_config=curr_config, tables=input_tables)
            list_dic = list_dic + db.generate_config_per_db()

        df = pd.DataFrame(list_dic)
        if len(df.index) == 0:
            update_config_logger.info(f"All the tables within {input_dbs} are already added ")
        else:
            # merge the curr_config (existing) dataframe
            # with df (newly added) dataframe
            df_new = pd.concat([curr_config, df], ignore_index=True).\
                sort_values(f'lake_ingestion_id')
            #  write this df to s3 in new config excel
            s3_write = S3Utils(service_name=f's3', config_section=con.aws_config_section,
                               input_df=df_new, bucket_name=con.s3_config_bucket,
                               bucket_key=con.s3_new_config_key,
                               excel_sheet_name=f'ingestion_control', file_type=f'xlsx')
            s3_write.write_into_s3()
            update_config_logger.info(f"Created a new ingestion_config and uploaded to s3")

    except Exception as e:
        exception_occ = traceback.format_exc()
        update_config_except_logger.error(e)
        update_config_except_logger.error(exception_occ)
