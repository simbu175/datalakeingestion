import configparser
import datetime
import traceback

import awswrangler as wr
import pandas as pd
from sqlalchemy import create_engine

import config.basic_config as con
from utils.aws_utils import AwsConnect
from utils.aws_utils import S3Utils
from utils.customlogger import basiclogger
from utils.common_utils import CommonUtils

ingestion_data_logger = basiclogger(logname=f"Mysql_ingestion_process",
                                    section=f"PROCESS")
ingestion_except_logger = basiclogger(logname=f"Mysql_exceptions",
                                      section=f"EXCEPTION")


class IngestTable:
    def __init__(self, tableid, server, dbname, tablename, delta_field, delta_field_expr,
                 date_ind, target_type, target_location, control_df,
                 partition_tup, mysql_details, pandas_details):

        self.files_list = []
        self.tableid = tableid
        self.server = server
        self.dbname = dbname
        self.tablename = tablename
        self.delta_field = delta_field
        self.delta_field_expr = delta_field_expr
        self.date_ind = date_ind
        self.target_type = target_type
        self.target_location = target_location
        self.control_df = control_df
        self.partition_tup = partition_tup
        self.s3_file_path = {}
        self.ret_sts = f"First-time-ingestion"

        self.datatype_config = configparser.ConfigParser()
        self.datatype_config.read(con.MAPPING_CONFIG)
        self.bigint_type = self.datatype_config.get(f'mysql_pandas', f'bigint')
        self.int_type = self.datatype_config.get(f'mysql_pandas', f'int')
        self.mediumint_type = self.datatype_config.get(f'mysql_pandas', f'mediumint')
        self.smallint_type = self.datatype_config.get(f'mysql_pandas', f'smallint')
        self.tinyint_type = self.datatype_config.get(f'mysql_pandas', f'tinyint')
        self.char_type = self.datatype_config.get(f'mysql_pandas', f'char')
        self.enum_type = self.datatype_config.get(f'mysql_pandas', f'enum')
        self.longtext_type = self.datatype_config.get(f'mysql_pandas', f'longtext')
        self.mediumtext_type = self.datatype_config.get(f'mysql_pandas', f'mediumtext')
        self.varchar_type = self.datatype_config.get(f'mysql_pandas', f'varchar')
        self.text_type = self.datatype_config.get(f'mysql_pandas', f'text')
        self.json_type = self.datatype_config.get(f'mysql_pandas', f'json')
        self.decimal_type = self.datatype_config.get(f'mysql_pandas', f'decimal')
        self.double_type = self.datatype_config.get(f'mysql_pandas', f'double')
        self.float_type = self.datatype_config.get(f'mysql_pandas', f'float')
        self.date_type = self.datatype_config.get(f'mysql_pandas', f'date')
        self.datetime_type = self.datatype_config.get(f'mysql_pandas', f'datetime')
        self.timestamp_type = self.datatype_config.get(f'mysql_pandas', f'timestamp')
        self.time_type = self.datatype_config.get(f'mysql_pandas', f'time')

        self.curs = mysql_details  # mysql cursor details
        self.db_conn = pandas_details  # pandas connection details

        self.aws_section_name = con.aws_config_section  # f"default_{os.environ['Mode']}"

        self.S3session = AwsConnect(service_name=f's3',
                                    config_section=self.aws_section_name)
        self.s3_session = self.S3session.get_aws_session()
        self.s3 = self.S3session.get_aws_resource()

    def get_last_run_value(self):
        # the files data is in the form,
        # <bucket_name>/<execution_log_folder>/<db>/<table>/<YYYYMM>/<DD>
        s3_exec_data = None
        latest_file = CommonUtils.get_prev_log_file(db=self.dbname,
                                                    table=self.tablename)

        if latest_file is None:  # exceptions scenario
            ingestion_data_logger.error(f"There is some exceptions "
                                        f"fetching the logs data")
            raise Exception(f"Unable to access the previous execution \
                              logs for {self.dbname}.{self.tablename}")

        elif len(latest_file) == 0:  # first time ingestion scenario
            ingestion_data_logger.info(f"There is no previous logs data found. \
                                         Hence starting the ingestion for the first time")
            s3_exec_data = pd.DataFrame([])

        else:  # subsequent ingestion scenario
            try:
                s3_read = S3Utils(service_name=con.s3_service_name,
                                  config_section=con.aws_config_section,
                                  input_df=None,
                                  file_type=con.s3_execution_log_file_type,
                                  file_full_path=latest_file)
                s3_exec_data = s3_read.read_from_s3()
                ingestion_data_logger.info(f"Read the last execution "
                                           f"log, {latest_file}")

            except Exception as S3_connect:
                s3_exec_data = pd.DataFrame([])
                ingestion_except_logger.info(f"Exception occured while reading. "
                                             f"Hence resetting to an empty dataframe")

        # s3_exec_data == f'Empty file' or s3_exec_data == f'No Such file exists':
        # This is the case when the file is not empty (or empty)
        # but there are no previous successful completions
        if s3_exec_data.empty or \
                s3_exec_data[s3_exec_data['execution_status'] == 'success'].empty:
            ingestion_data_logger.info(f"This is a first-time ingestion")
            if self.date_ind == 0:
                # Default start date is 2020 January 1st. (As confirmed in LEN-20888)
                # check for voluminous tables
                if f"{self.dbname}.{self.tablename}" in con.voluminous_tables:
                    ingestion_data_logger.info(f"Processing "
                                               f"voluminous table block")
                    self.ret_sts = f"{self.ret_sts}_voluminous_table"
                    arch_start_value = datetime.datetime(2020, 1, 1, 0, 0, 0)
                else:
                    arch_start_value = datetime.datetime(2020, 1, 1, 0, 0, 0)
            else:
                arch_start_value = 1

        else:  # elif isinstance(s3_exec_data, pd.DataFrame):
            ingestion_data_logger.info(f"This is a Subsequent-ingestion")
            self.ret_sts = f"Subsequent-ingestion"
            if f"{self.dbname}.{self.tablename}" in con.voluminous_tables:
                self.ret_sts = f"{self.ret_sts}_voluminous_table"

            s3_last_run_value = \
                s3_exec_data[s3_exec_data['execution_status'] == 'success']
            # fetch the latest end_value
            s3_last_run_value = s3_last_run_value[f'end_value'].max()
            ingestion_data_logger.info(f"Extracted end_value : {s3_last_run_value}")

            if self.date_ind == 0:
                # convert the pandas datetime column to python datetime and add 1 second
                arch_start_value = \
                    datetime.datetime.strptime(s3_last_run_value[:19],
                                               f'%Y-%m-%d %H:%M:%S') + \
                    datetime.timedelta(seconds=1)
            else:
                arch_start_value = int(s3_last_run_value) + 1  # add 1 for incrementing
            ingestion_data_logger.info(f"Current ingestion start "
                                       f"value : {arch_start_value}")

        return self.ret_sts, arch_start_value, s3_exec_data

    def fetch_current_run_data(self, arch_start_val, f_list):

        col_types = f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, \
                      NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_TYPE \
                      FROM information_schema.columns \
                      WHERE table_schema = '{self.dbname}' \
                      and table_name = '{self.tablename}' "
        col_values = self.curs.processQuery(col_types)

        # column data types dictionary to keep track of mysql data types for each column
        col_dtypes = {}
        col_dates = {}  # date format parsing dictionary
        query = f"select "
        query_cols = f""  # full column list from information_schema.columns

        for col_val in col_values:
            col_nam = col_val[f'COLUMN_NAME']
            col_dtype = col_val[f'DATA_TYPE']
            # col_length = col_val[f'CHARACTER_MAXIMUM_LENGTH']
            # col_num_precision = col_val[f'NUMERIC_PRECISION']
            # col_num_scale = col_val[f'NUMERIC_SCALE']
            # col_type = col_val[f'COLUMN_TYPE']

            col_dtypes[col_nam] = eval(f"self.{col_dtype}_type")
            # col_dtypes[col_nam] = getattr(self, f"{col_dtype}_type", 'Int64')

            if col_dtype == f'date' or col_dtype == f'datetime' or \
                    col_dtype == f'timestamp':
                if col_dtype == f'date':
                    col_dates[col_nam] = {f'format': f'%Y-%m-%d'}
                else:
                    col_dates[col_nam] = {f'format': f'%Y-%m-%d %H:%M:%S'}
                # casting data from 0000-00-00 to 1900-01-01 -
                # to handle year 0000 is out of range in pandas
                # also making the datetime within the pandas bounds
                col_nam = \
                    f"case when date({col_nam}) >= '2262-04-11' then '2262-04-11' " \
                    f"when date({col_nam}) <= '1900-01-01' then '1900-01-01' " \
                    f"else {col_nam} end {col_nam}"

            query_cols = query_cols + f"{col_nam},"
        #  Adding an expr_column at the end of the dataset
        if len(self.delta_field_expr) > 0:
            query_cols = query_cols + f" {self.delta_field_expr} expr_column "

        if len(query_cols) > 0:
            if query_cols[-1] == f',':
                query_cols = query_cols[:-1]
        else:
            query_cols = f" * "

        ingestion_data_logger.info(f"Date_ind : {self.date_ind}")

        if f"{self.dbname}.{self.tablename}" in con.voluminous_tables:
            chunks = con.voluminous_chunks
            load_sts = self.update_data(query_cols=query_cols, start_val=arch_start_val,
                                        parse_dates=col_dates, dtypes=col_dtypes,
                                        chunksize=chunks, log_file=f_list, v_flag=1)
        else:
            chunks = con.normal_chunk_size
            load_sts = self.update_data(query_cols=query_cols, start_val=arch_start_val,
                                        parse_dates=col_dates, dtypes=col_dtypes,
                                        chunksize=chunks, log_file=f_list, v_flag=0)

        return load_sts

    def update_data(self, query_cols, start_val, parse_dates, dtypes,
                    chunksize, log_file, v_flag):
        #  stream_results will create a server side cursor and not
        #  client side cursor for iterating over the data source
        engine = create_engine(self.db_conn)
        db_con = engine.connect().execution_options(stream_results=True)

        query = f"select "
        minval = []
        maxval = []
        counts = 0
        ret_status = []
        return_status = f""

        if v_flag:
            end_val = datetime.datetime.today().replace(hour=0, minute=0, second=0)
            if start_val is None:
                start_val = datetime.datetime(2020, 1, 1, 0, 0, 0)
                curr_val = start_val.replace(hour=23, minute=59, second=59)
            else:
                start_val = datetime.datetime.strptime(start_val[:19],
                                                       '%Y-%m-%d %H:%M:%S')
                if str(start_val)[11:19] != '00:00:00':  # for handling last few seconds
                    curr_val = (start_val + datetime.timedelta(days=1)). \
                        replace(hour=23, minute=59, second=59)
                else:
                    curr_val = start_val.replace(hour=23, minute=59, second=59)

            while start_val <= curr_val <= end_val:
                ingestion_data_logger.info(f"Starting data pull "
                                           f"between {start_val} and {curr_val}")

                if len(self.delta_field_expr) == 0:
                    query = f"select " + query_cols + f" from {self.dbname}.{self.tablename} " \
                                                      f"where {self.delta_field} " \
                                                      f"between '{str(start_val)}' " \
                                                      f"and '{str(curr_val)}'"
                else:
                    # use case for handling coalesce or ifnull to override functions based index
                    expr = self.delta_field_expr
                    if expr[:8] == f'coalesce' or expr[:6] == f'ifnull':
                        expr_col_1 = expr[expr.find('(') + 1:expr.find(',')].strip()
                        expr_col_2 = expr[expr.find(',') + 2:expr.find(')')].strip()
                        query = f"select " + query_cols + f" from {self.dbname}.{self.tablename} \
                                  where ( {expr_col_1} between '{str(start_val)}' and '{str(curr_val)}' \
                                  or {expr_col_2} between '{str(start_val)}' and '{str(curr_val)}' )"
                    else:
                        query = f"select " + query_cols + f" from {self.dbname}.{self.tablename} " \
                                                          f"where {self.delta_field_expr} " \
                                                          f"between '{str(start_val)}' " \
                                                          f"and '{str(curr_val)}'"

                for chunk in pd.read_sql_query(sql=query, con=db_con,
                                               parse_dates=parse_dates,
                                               chunksize=chunksize, dtype=dtypes):
                    if len(self.delta_field_expr) == 0:
                        minval.append(chunk[f'{self.delta_field}'].min())
                        maxval.append(chunk[f'{self.delta_field}'].max())
                    else:
                        expr = self.delta_field_expr
                        if expr[:8] == f'coalesce' or expr[:6] == f'ifnull':
                            expr_col_1 = expr[expr.find('(') + 1:expr.find(',')].strip()
                            expr_col_2 = expr[expr.find(',') + 2:expr.find(')')].strip()
                            minval.append(chunk[f'expr_column'].min())
                            maxval.append(min(chunk[f'{expr_col_1}'].max(),
                                              chunk[f'{expr_col_2}'].max()))
                        else:
                            minval.append(chunk[f'expr_column'].min())
                            maxval.append(chunk[f'expr_column'].max())
                        # remove the expr_column which was added externally
                        # for tracking the last run time
                        chunk.drop(f'expr_column', axis=1, inplace=True)
                    counts = counts + len(chunk.index)

                    return_status, files = self.load_chunk(chunk)
                    ret_status.append(return_status)
                    self.append_target_files(files)
                ingestion_data_logger.info(f"Updated data for range "
                                           f"{str(start_val)} "
                                           f"and {str(curr_val)}")

                start_val = (curr_val + datetime.timedelta(days=1)). \
                    replace(hour=0, minute=0, second=0)
                curr_val = (curr_val + datetime.timedelta(days=1)). \
                    replace(hour=23, minute=59, second=59)

            ingestion_data_logger.info(f"Data pull completed")

        else:
            if self.date_ind == 0:
                # expression with date field
                if len(self.delta_field_expr) == 0:
                    query = query + query_cols + f" from {self.dbname}.{self.tablename} " \
                                                 f"where {self.delta_field} >= '{start_val}' "
                # no expression with date field
                else:
                    query = query + query_cols + f" from {self.dbname}.{self.tablename} " \
                                                 f"where {self.delta_field_expr} >= '{start_val}' "
            else:
                # without date field; not needed for expression
                query = query + query_cols + f" from {self.dbname}.{self.tablename} " \
                                             f"where {self.delta_field} >= '{str(start_val)}' "

            for chunk in pd.read_sql_query(sql=query, con=db_con,
                                           parse_dates=parse_dates,
                                           chunksize=chunksize,
                                           dtype=dtypes):

                if len(self.delta_field_expr) == 0:
                    minval.append(chunk[f'{self.delta_field}'].min())
                    maxval.append(chunk[f'{self.delta_field}'].max())
                else:
                    expr = self.delta_field_expr
                    if expr[:8] == f'coalesce' or expr[:6] == f'ifnull':
                        expr_col_1 = expr[expr.find('(') + 1:expr.find(',')].strip()
                        expr_col_2 = expr[expr.find(',') + 2:expr.find(')')].strip()
                        minval.append(chunk[f'expr_column'].min())
                        maxval.append(min(chunk[f'{expr_col_1}'].max(),
                                          chunk[f'{expr_col_2}'].max()))
                    else:
                        minval.append(chunk[f'expr_column'].min())
                        maxval.append(chunk[f'expr_column'].max())
                    # remove the expr_column which was added externally
                    # for tracking the last run time
                    chunk.drop(f'expr_column', axis=1, inplace=True)
                counts = counts + len(chunk.index)

                return_status, files = self.load_chunk(chunk)
                ret_status.append(return_status)
                self.append_target_files(files)

                ingestion_data_logger.info(f"Data pull completed")

        db_con.close()
        if len(ret_status) == 0:
            min_v = start_val
            max_v = start_val
            len_d = 0
        else:
            min_v = min(minval)
            max_v = max(maxval)
            len_d = counts
        self.log_data(log_list=log_file, min_val=min_v,
                      max_val=max_v, len_df=len_d)

        return_status = [i for i in ret_status if i is not None]
        ingestion_data_logger.info(f"Return load status : {return_status} and {ret_status}")

        if len(return_status) == 0:
            return f"Empty Dataframe"
        else:
            return ret_status[0]

    def load_chunk(self, df_chunk):

        if df_chunk.empty:
            res_code = f"Empty Dataframe"
            ingestion_data_logger.info(f"No incoming data to load! success-no-data")
            file_data = None

        else:
            ingestion_data_logger.info(f"Starting data load to s3")
            res_code, file_data = self.load_into_target(target_frame=df_chunk)

        return res_code, file_data

    def append_target_files(self, file_details):

        if file_details['paths']:  # 'paths' key will have the s3 file name
            self.files_list = self.files_list + file_details['paths']

        self.s3_file_path[f"{self.dbname}.{self.tablename}"] = self.files_list
        ingestion_data_logger.info(f"Data file loaded to s3: {file_details['paths']}")

    def log_data(self, log_list, min_val, max_val, len_df):

        if len_df == 0:
            exec_mess = f'no-data-to-load'
            loaded_files = f'No-data'
        else:
            exec_mess = f'data-loaded-to-s3'
            loaded_files = self.s3_file_path[f"{self.dbname}.{self.tablename}"]
        insert_exec_status = \
            CommonUtils.generate_execution_log_dict(ingestion_id=self.tableid,
                                                    exec_status=exec_mess,
                                                    start_value=min_val,
                                                    end_value=max_val,
                                                    source_count=len_df,
                                                    target_count=0,
                                                    reason_code=loaded_files)
        log_list.append(insert_exec_status)

        # appending all the dictionary status to a dataframe and
        # that can be written to s3 for execution log
        ingestion_data_logger.info(f"Converting the list of dictionary "
                                   f"to a df and storing in s3")
        log_df = pd.DataFrame(log_list)
        if CommonUtils.update_execution_log(log_df, self.dbname,
                                            self.tablename):
            ingestion_data_logger.info(f"Execution log updated")

    @staticmethod
    def validate_partition_data(part_type, part_col, part_format):

        ingestion_data_logger.info(f"Running validate_partition_data "
                                   f"for : {part_type}, {part_col}, {part_format}")
        reason_code = f'1'
        if part_type is None and part_col is None and part_format is None:
            reason_code = f"1"

        elif part_type == f'non-time-based':
            reason_code = f"2"
            supported_types = [f'smallint', f'int', f'bigint']
            if part_col is not None and part_format is None:
                reason_code = f"Partition format is missing"
            elif ',' in part_col:
                reason_code = f"Only one column name per row " \
                              f"is allowed for partition config"

        elif part_type == f'time-based':
            reason_code = f"3"

            if type(part_col).__name__ == f'timedelta':
                reason_code = f"Time field can't be used for partitioning"
            elif part_col is not None and part_format is None:
                reason_code = f"Partition format is missing"
            elif ',' in part_col:
                reason_code = f"Only one column name per row " \
                              f"is allowed for partition config"

        ingestion_data_logger.info(f"Result of validate_partition_data "
                                   f": {reason_code}")

        return reason_code

    @staticmethod
    def find_partition_range(n, p_col, i_div=1000000):
        return int(n[p_col] / i_div)

    @staticmethod
    def prepare_partitions(prep_df, prep_col, prep_format, r_code):

        if r_code == f"3":
            if prep_format == f'YYYYMMDD':
                part_form = f'%Y%m%d'
            elif prep_format == f'YYYYMM':
                part_form = f'%Y%m'
            else:  # if prep_format == f'YYYY':
                part_form = f'%Y'
            ingestion_data_logger.info(f"Partition format : {part_form}")
            new_part_col = f"{prep_col}_{prep_format}"
            prep_df[new_part_col] = prep_df[prep_col].dt.strftime(part_form)
            return new_part_col, prep_df

        elif r_code == f"2":
            if prep_format == f'1M':
                new_part_col = f"{prep_col}_range"
                ingestion_data_logger.info(f"Partition format : {prep_format}")
                prep_df[new_part_col] = \
                    prep_df.apply(lambda x: IngestTable.find_partition_range(x, prep_col, 1000000), axis=1)
                return new_part_col, prep_df

    def get_partition_data(self, target_dataframe):

        reason_code = f""
        ingestion_data_logger.info(f"Getting the partition data "
                                   f"from {self.partition_tup}")

        if len(self.partition_tup) <= 0:  # No partition scenario
            ingestion_data_logger.info(f"No partition scenario")
            reason_code = f"1"
            list_columns = None
            return target_dataframe, reason_code, list_columns

        else:  # partitioned scenario
            ingestion_data_logger.info(f"Partitioned scenario")
            list_columns = []
            # for index, row in self.partition_tup.iterrows():
            for row in self.partition_tup:
                ingestion_data_logger.info(f"{row}")
                i_seq_no = row[f'partition_seq_no']
                i_col_type = row[f'partition_column_type']
                i_column = row[f'partition_column']
                i_format = row[f'partition_format']
                ingestion_data_logger.info(f"Partition values : "
                                           f"{i_seq_no} || {i_col_type} "
                                           f"|| {i_column} || {i_format}")

                # validate the partition data from the rules method
                reason_code = self.validate_partition_data(i_col_type,
                                                           i_column, i_format)

                ingestion_data_logger.info(f"Return from validate_partition_data "
                                           f": {reason_code}")
                if reason_code == f"3" or reason_code == f"2":
                    new_part_col_name, target_dataframe = \
                        self.prepare_partitions(target_dataframe, i_column, i_format, reason_code)
                    ingestion_data_logger.info(f"New partition column "
                                               f"added : {new_part_col_name}")
                    list_columns.append(new_part_col_name)

                else:
                    list_columns.append(i_column)
                    ingestion_data_logger.info(f"Breaking out of loop")
                    break

            return target_dataframe, reason_code, list_columns

    def s3_put_parquet(self, put_df, target_path, data_mode,
                       partition_column_list=None, dataset_needed=True):

        if partition_column_list is None:
            ingestion_data_logger.info(f"Loading data for no partition scenario")
            output_details = wr.s3.to_parquet(df=put_df, path=target_path,
                                              compression=f'snappy',
                                              boto3_session=self.s3_session,
                                              mode=data_mode, dataset=dataset_needed)
        else:
            ingestion_data_logger.info(f"Loading data for partitioned scenario")
            output_details = wr.s3.to_parquet(df=put_df, path=target_path,
                                              partition_cols=partition_column_list,
                                              compression=f'snappy',
                                              boto3_session=self.s3_session,
                                              mode=data_mode,
                                              dataset=dataset_needed)
        return output_details

    def load_into_target(self, target_frame):

        if self.target_type.upper() == f'S3':

            if self.target_location[-1] != f'/':
                self.target_location = self.target_location + f'/'

            ingestion_data_logger.info(f"Removed the / at the end "
                                       f"of the target location, if present")

            target_loc = f"{self.target_location}{self.dbname}/{self.tablename}/"
            ingestion_data_logger.info(f"Target location : {target_loc}")
            target_frame, reason_code, part_col = self.get_partition_data(target_frame)

            ingestion_data_logger.info(f"Output reason code : {reason_code}")

            output_details = None
            if reason_code == f"1" or reason_code == f"2" or reason_code == f"3":
                output_details = self.s3_put_parquet(put_df=target_frame,
                                                     target_path=target_loc,
                                                     partition_column_list=part_col,
                                                     data_mode=f'append')

            return reason_code, output_details

    def remove_s3_data(self, files_dict):

        ingestion_data_logger.info(f"Files list : {files_dict}")
        files_to_delete = files_dict[f'paths']

        for file in files_to_delete:
            bucket_name = file[5:file.find(f"/", 5)]  # 's3://<bucket-name>/'
            key_name = file[file.find(f"/", 5) + 1:]  # 's3://<bucket-name>/key_name'
            self.s3.Object(bucket_name, key_name).delete()

        ingestion_data_logger.info(f"Files removed : {files_to_delete}")

        return True

    def extract_and_load_table(self):
        res_code = None
        empty_list = []
        try:
            ingestion_data_logger.info(f"Starting get_last_run_value")
            return_status, start_value, exec_df = self.get_last_run_value()
            # form the header row if there is no file for the first time load
            # Instead of updating the s3 file each time, rather
            # save the contents locally in a list of dictionary
            # then we can finally save the dictionary-list to s3 as csv
            # for tracking the execution logs

            ingestion_data_logger.info(f"Converting the s3 df to a list of "
                                       f"dictionary for updates")
            insert_exec_status = \
                CommonUtils.generate_execution_log_dict(ingestion_id=self.tableid,
                                                        exec_status=f'started',
                                                        start_value=start_value,
                                                        end_value=None,
                                                        source_count=0, target_count=0,
                                                        reason_code=return_status)
            empty_list.append(insert_exec_status)

            res_code = self.fetch_current_run_data(arch_start_val=str(start_value),
                                                   f_list=empty_list)
            return res_code

        except Exception as load_err:
            ingestion_except_logger.info(
                f"Exceptions for {self.dbname}.{self.tablename} "
                f"{load_err}")
            insert_exec_status = \
                CommonUtils.generate_execution_log_dict(ingestion_id=self.tableid,
                                                        exec_status=f'failed',
                                                        start_value=None,
                                                        end_value=None,
                                                        source_count=0, target_count=0,
                                                        reason_code=f'exception-occured')
            empty_list.append(insert_exec_status)
            exec_df = pd.DataFrame(empty_list)

            if CommonUtils.update_execution_log(exec_df, self.dbname, self.tablename):
                ingestion_except_logger.info(
                    f"Exceptions for {self.dbname}.{self.tablename} "
                    f"- execution log updated into s3")

            if res_code == f"1" or res_code == f"2" or res_code == f"3":
                ingestion_data_logger.info(
                    f"Exceptions for {self.dbname}.{self.tablename} "
                    f"- removing the already loaded data files, if any")
                file = f"{self.dbname}.{self.tablename}"
                if CommonUtils.remove_s3_data(self.s3_file_path[file]):
                    ingestion_data_logger.info(f"Files {self.s3_file_path[file]} "
                                               f"removed successfully")

            res_code = f"exception-occured"

            load_error = traceback.format_exc()
            ingestion_except_logger.error(f"{res_code.upper()} : {load_error} "
                                          f"for {self.dbname}.{self.tablename}")
            return res_code


if __name__ == f"__main__":

    # noinspection PyBroadException
    try:

        target_loc = f's3://databi-testing/Tugela/archive_data_test/'
        ingestion_data_logger.info(f"Start of the program")
        # noinspection PyArgumentList
        lending_table = IngestTable(tableid=8,
                                    server=f'10.6.0.36',
                                    dbname=f'lendingstream',
                                    tablename=f'batch_creation',
                                    delta_field=f'modified_datetime',
                                    date_ind=0,
                                    target_type=f's3',
                                    target_location=target_loc)
        lending_table.extract_and_load_table()
        ingestion_data_logger.info(f"End of the program")

    except Exception as main_err:
        write_error = traceback.format_exc()
        ingestion_except_logger.error(write_error)
        exit(1)
