import datetime
import sys
import awswrangler as wr
import pandas as pd
import traceback

# sys.path.append(f'data/codes/Lake_Ingestion_Codes/')
# sys.path.append(f"D:\\Initial_Version\\Lake_Ingestion_Codes\\")
from config import basic_config as con
from utils.aws_utils import AwsConnect
from utils.aws_utils import S3Utils
from utils.customlogger import basiclogger


class CommonUtils:
    comm_util_logger = basiclogger(logname=f"Common_Utils_processing",
                                   section=f"PROCESS")
    comm_util_except = basiclogger(logname=f"Common_Utils_exceptions",
                                   section=f"EXCEPTION")

    @staticmethod
    def read_s3_config_as_df(config_section, config_type,
                             use_wrangler=0, file_type=f'xlsx', apply_filter=None):
        # Extracting the control file from s3
        # This file holds the config setup for each table and the target schema in glue catalog
        # This is a xlsx file. We have a choice to use wrangler for fetching this xlsx data as binary data
        # return the dataframe
        conf_file_type = file_type
        CommonUtils.comm_util_logger.info(f"Reading config - {config_section}")
        if config_type == f"config-control":
            bucket_name = con.s3_config_bucket
            bucket_key = con.s3_control_config_key
            excel_sheet_name = con.s3_control_sheet_name
        elif config_type == f"config-partition":
            bucket_name = con.s3_partition_bucket
            bucket_key = con.s3_partition_config_key
            excel_sheet_name = con.s3_partition_sheet_name
        CommonUtils.comm_util_logger.info(f"Additional details "
                                          f"- {bucket_name} || "
                                          f"{bucket_key} || {excel_sheet_name}")

        s3_ctl = S3Utils(service_name=con.s3_service_name,
                         config_section=config_section,
                         input_df=None, file_type=conf_file_type,
                         bucket_name=bucket_name,
                         bucket_key=bucket_key, excel_sheet_name=excel_sheet_name,
                         use_wrangler=0)
        config_data = s3_ctl.read_from_s3()
        CommonUtils.comm_util_logger.info(f"Config file successfully "
                                          f"read into a dataframe")
        CommonUtils.comm_util_logger.info(config_data.head(1))
        # print(control_data.head())
        # control_df = pd.read_excel(io.BytesIO(control_data), sheet_name=con.s3_control_sheet_name,
        #                            dtype=str)  # , header=1)
        if apply_filter:
            config_data = config_data.query(apply_filter)
            CommonUtils.comm_util_logger.info(f"Filtered the applied filter "
                                              f"to the s3 config extracted")

        return config_data

    @staticmethod
    def generate_execution_log_dict(ingestion_id, exec_status, start_value,
                                    end_value=None,
                                    source_count=0, target_count=0, reason_code=f""):
        # Grab the inputs and assign each of the incoming value to the corresponding key element
        # associated with the execution log format available in the config
        return_dict = {}
        CommonUtils.comm_util_logger.info(f"Generating the dictionary from the "
                                          f"config containing the list of headers")
        for i in con.execution_log_header_list:
            if i == f'lake_ingestion_id':
                return_dict[i] = ingestion_id
            elif i == f'execution_status':
                return_dict[i] = exec_status
            elif i == f'start_value':
                return_dict[i] = start_value
            elif i == f'end_value':
                return_dict[i] = end_value
            elif i == f'source_count':
                return_dict[i] = source_count
            elif i == f'target_count':
                return_dict[i] = target_count
            elif i == f'reason_code':
                return_dict[i] = reason_code
            elif i == f'insert_datetime':
                return_dict[i] = datetime.datetime.now()

        CommonUtils.comm_util_logger.info(f"Dict : {return_dict}")
        return return_dict

    @staticmethod
    def update_execution_log(df_to_update_log, db_name, tbl_name, full_path=None):
        # This method is used to update the incoming dataframe as a log file in s3
        # The s3 bucket details are fetched from config
        try:

            if full_path is None:
                year_month_format = datetime.datetime.now().strftime(f'%Y%m')
                date_format = datetime.datetime.now().strftime(f'%d')
                file_name = f"{datetime.datetime.now().strftime('%Y%m%d%H%M%f')}.{con.s3_execution_log_file_type}"
                s3_key = \
                    f"{con.s3_execution_log_bucket_key_prefix}/{db_name}/{tbl_name}/{year_month_format}/{date_format}/{file_name}"
                s3_write_exec = S3Utils(service_name=con.s3_service_name,
                                        config_section=con.aws_config_section,
                                        input_df=df_to_update_log,
                                        file_type=con.s3_execution_log_file_type,
                                        bucket_name=con.s3_execution_log_bucket_name,
                                        bucket_key=s3_key)
                CommonUtils.comm_util_logger.info(f"Writing the log into {s3_key}")
            else:
                s3_write_exec = S3Utils(service_name=con.s3_service_name,
                                        config_section=con.aws_config_section,
                                        input_df=df_to_update_log,
                                        file_type=con.s3_execution_log_file_type,
                                        file_full_path=full_path)
                CommonUtils.comm_util_logger.info(f"Writing the log into prefix, {full_path}")

            s3_write_exec.write_into_s3()

            CommonUtils.comm_util_logger.info(f"Data updated to s3")
            return True

        except Exception as write_exec:
            CommonUtils.comm_util_except.info(f"Exceptions occured when updating {s3_key}")
            return False

    @staticmethod
    def remove_s3_data(s3_files, use_wrangler=0):
        # Check if the input parameter is string or list and change them to list
        # The parameter s3_files_list will have the
        # list of s3 files that are loaded by the program
        # And through a loop, this code will remove them safely
        # from s3 in case whenever a validation is failed
        # or whenever the data load is corrupted within s3
        # Note: This file will have both 'paths' and 'partitions'
        # data, but we're only interested in the path values for removing them

        # CommonUtils.comm_util_logger.info(f"Files list to delete: {s3_files_list}")
        CommonUtils.comm_util_logger.info(f"Input file parameter "
                                          f": {s3_files} and type : {type(s3_files)}")

        # If the input parameter s3_files is a string, convert it to a list
        if isinstance(s3_files, str):
            from ast import literal_eval
            s3_files_list = literal_eval(s3_files)
        else:
            s3_files_list = s3_files

        if use_wrangler == 0:
            S3session = AwsConnect(service_name=con.s3_service_name,
                                   config_section=con.aws_config_section)
            s3_session = S3session.get_aws_session()
            s3 = S3session.get_aws_resource()

            # files_to_delete = s3_files_list[f'paths']
            CommonUtils.comm_util_logger.info(f"Files list to delete: {s3_files_list}")
            for file in s3_files_list:
                bucket_name = file[5:file.find(f"/", 5)]  # 's3://<bucket-name>/'
                key_name = file[file.find(f"/", 5) + 1:]  # 's3://<bucket-name>/key_name'
                try:
                    s3.Object(bucket_name, key_name).delete()
                except Exception as del_error:
                    CommonUtils.comm_util_logger.info(
                        f"Issues with accessing the s3 "
                        f"object - {bucket_name}/{key_name}")

            CommonUtils.comm_util_logger.info(f"Files removed : {s3_files_list}")

        else:
            CommonUtils.comm_util_logger.info(f"Files list to delete: {s3_files_list}")
            try:
                wr.s3.delete_objects(s3_files_list)
            except Exception as del_error:
                CommonUtils.comm_util_logger.info(f"Issues with accessing the s3 "
                                                  f"object - {s3_files_list}")

            CommonUtils.comm_util_logger.info(f"Files removed : {s3_files_list}")

        return True

    @staticmethod
    def list_s3_objects(file_path, wrangler_flag=1):
        # A method which gets the file prefix as input and
        # from configuration, fetches the bucket name
        # Thereby able to use awswrangler's inbuilt
        # list_objects method for listing the output files within list
        # return type : list : an empty list if no files
        # were found in the path provided
        S3obj = AwsConnect(service_name=con.s3_service_name,
                           config_section=con.aws_config_section)
        s3_session = S3obj.get_aws_session()
        CommonUtils.comm_util_logger.info(f"Defined s3 objects for sessions")

        if wrangler_flag:
            CommonUtils.comm_util_logger.info(f"Using wrangler for listing")
            file_list = []
            file_list = wr.s3.list_objects(path=file_path, boto3_session=s3_session)

        else:
            #  this block is for boto3 or any other SDK for listing s3 objects
            pass

        CommonUtils.comm_util_logger.info(f"Objects returned from list_objects : {file_list}")
        return file_list

    @staticmethod
    def return_success_only_objects(file_obj, wrangler_flag=1):
        # A method which gets the file object as input and from configuration, fetches the bucket name
        # Thereby able to use awswrangler's inbuilt describe_objects method for fetching the metadata
        # filter only the dataframe which has success as the execution_status in it.
        # return type : list : an empty list if no files were found in the path provided

        s3_child = S3Utils(service_name=con.s3_service_name,
                           config_section=con.aws_config_section,
                           input_df=None, file_type=con.s3_execution_log_file_type,
                           file_full_path=file_obj, use_wrangler=0)
        s3_session = s3_child.get_aws_session()
        df = s3_child.read_from_s3()
        CommonUtils.comm_util_logger.info(f"Reading data for method, "
                                          f"return_success_only_objects")

        file_meta = f""
        if wrangler_flag:
            #  dict_df = wr.s3.describe_objects(path=file_obj, boto3_session=s3_session)
            #  check if the file has execution_status == 'success'
            #  convert this dict to dataframe for fetching the success only files

            CommonUtils.comm_util_logger.info(f"Log df : {df}; type - {type(df)}")
            file_df = df[df['execution_status'] == 'success']
            file_meta = file_df[f'end_value'].max()
            CommonUtils.comm_util_logger.info(f"Previous success "
                                              f"time : {file_meta}; "
                                              f"type - {type(file_meta)}")

        else:
            pass  # block to do describe objects with boto3

        if pd.isna(file_meta):  # .empty: # check for nan
            #  If there are no success entries nullify the file name when returning the data
            file_meta = f""

        return file_meta

    @staticmethod
    def fetch_recent_success_file(file_list):
        file_details = {}  # empty dictionary to track the last modified time of a file
        CommonUtils.comm_util_logger.info(f"Instantiating an s3 object for "
                                          f"getting s3 session from aws_utils")

        for file in file_list:
            file_objects = CommonUtils.return_success_only_objects(file_obj=file)
            # since the file is returned with a default UTC timezone,
            # removing the timezone part for comparisons
            # file_objects[file]['LastModified'].replace(tzinfo=None)
            file_details[file] = file_objects

        # filter only {!= ""} values for checking the success entries
        success_files = {k: v for k, v in file_details.items() if v != f""}
        CommonUtils.comm_util_logger.info(f"Files with 'success' "
                                          f"execution_status : {success_files}")

        if len(success_files) == 0:  # if there are no success files, then make it empty
            recent_file = []
        else:
            recent_file = max(success_files, key=success_files.get)

        CommonUtils.comm_util_logger.info(f"Fetch recent success "
                                          f"file : {recent_file}")
        return recent_file

    @staticmethod
    def fetch_current_file(file_list):
        S3session = AwsConnect(service_name=con.s3_service_name,
                               config_section=con.aws_config_section)
        s3_session = S3session.get_aws_session()

        file_details = {}  # empty dictionary to track the last modified time of a file
        CommonUtils.comm_util_logger.info(f"Instantiating an s3 object for current log fetch")

        # iterate over all the files and get the lastModified time and store the response
        for file in file_list:
            dict_output = wr.s3.describe_objects(path=file, boto3_session=s3_session)
            file_details[file] = dict_output[file]['LastModified'].replace(tzinfo=None)

        CommonUtils.comm_util_logger.info(f"File details : {file_details}")
        recent_file = max(file_details, key=file_details.get)

        CommonUtils.comm_util_logger.info(f"Recent file : {recent_file}")
        return recent_file

    @staticmethod
    def get_prev_log_file(db, table, current_flag=0, use_wrangler=1):
        # This method concatenates the most recent log
        # files that is needed for current ingestion
        # If there is only one file available, then the
        # single file is returned as a filename
        # In case, if there is no files available for
        # the given prefix, then the code automatically fetches
        # the next older file based on the given path and returns that log
        # This is used when the processing starts for the
        # first date or for the first file within the partitions

        try:
            year_month_format = datetime.datetime.now().strftime(f'%Y%m')
            date_format = datetime.datetime.now().strftime(f'%d')
            s3_prefix = f"{con.s3_execution_log_bucket_key_prefix}/{db}/{table}/{year_month_format}/{date_format}"
            CommonUtils.comm_util_logger.info(f"Initiating listing of "
                                              f"csv files within {s3_prefix}")

            file_list = []  # empty list of s3 files
            recent_file = []  # return file names

            if current_flag:  # fetching the recent file from the current ingsetion
                if use_wrangler == 1:
                    CommonUtils.comm_util_logger.info(f"Searching current "
                                                      f"ingestion's file")
                    file_list = CommonUtils.list_s3_objects(
                        file_path=f's3://{con.s3_execution_log_bucket_name}/{s3_prefix}')

                    if len(file_list) > 0:
                        recent_file = CommonUtils.fetch_current_file(file_list=file_list)

                    else:
                        recent_file = None
                        CommonUtils.comm_util_logger.info(f"Issues in "
                                                          f"accessing, {s3_prefix}")
                        raise Exception(f"Issues in accessing, {s3_prefix}")

                    return recent_file

            else:  # fetching the previous run's log file
                if use_wrangler == 1:
                    CommonUtils.comm_util_logger.info(f"Searching recent "
                                                      f"log files in the day")
                    file_list = CommonUtils.list_s3_objects(
                        file_path=f's3://{con.s3_execution_log_bucket_name}/{s3_prefix}')

                    if len(file_list) > 0:
                        CommonUtils.comm_util_logger.info(f"Log files available in the day")
                        recent_file = CommonUtils.fetch_recent_success_file(
                            file_list=file_list)  # call fetch_success_data

                    if not recent_file:
                        CommonUtils.comm_util_logger.info(f"Searching all logs "
                                                          f"within current month")
                        # prev_month = (datetime.datetime.now().replace(day=1) -
                        #               datetime.timedelta(days=1)).strftime(f'%Y%m')
                        s3_prefix = f"{con.s3_execution_log_bucket_key_prefix}/{db}/{table}/{year_month_format}/"
                        file_list = CommonUtils.list_s3_objects(
                            file_path=f's3://{con.s3_execution_log_bucket_name}/{s3_prefix}')

                        if len(file_list) > 0:
                            CommonUtils.comm_util_logger.info(f"Log files available "
                                                              f"in current month")
                            recent_file = CommonUtils.fetch_recent_success_file(
                                file_list=file_list)  # call fetch_success_data

                        if not recent_file:
                            CommonUtils.comm_util_logger.info(f"Searching all logs "
                                                              f"within previous month")
                            prev_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime(
                                f'%Y%m')
                            s3_prefix = f"{con.s3_execution_log_bucket_key_prefix}/{db}/{table}/{prev_month}/"
                            file_list = CommonUtils.list_s3_objects(
                                file_path=f's3://{con.s3_execution_log_bucket_name}/{s3_prefix}')

                            if len(file_list) > 0:
                                CommonUtils.comm_util_logger.info(f"Log files "
                                                                  f"available in previous month")
                                recent_file = CommonUtils.fetch_recent_success_file(
                                    file_list=file_list)  # call fetch_success_data

                            if not recent_file:
                                CommonUtils.comm_util_logger.info(f"Searching all the "
                                                                  f"log files for the table")
                                s3_prefix = f"{con.s3_execution_log_bucket_key_prefix}/{db}/{table}"
                                file_list = CommonUtils.list_s3_objects(
                                    file_path=f's3://{con.s3_execution_log_bucket_name}/{s3_prefix}')

                                if len(file_list) > 0:
                                    CommonUtils.comm_util_logger.info(f"Log files only "
                                                                      f"available for 1+ "
                                                                      f"month old data")
                                    recent_file = CommonUtils.fetch_recent_success_file(
                                        file_list=file_list)  # call fetch_success_data

                else:
                    #  a block for listing s3 objects with boto3
                    pass

                CommonUtils.comm_util_logger.info(f"Returning from "
                                                  f"Get prev log file : {recent_file}")
                return recent_file

        except Exception as list_error:
            exception_occ = traceback.format_exc()
            CommonUtils.comm_util_logger.info(f"Exceptions happened. "
                                              f"Please check the exceptions log")
            CommonUtils.comm_util_except.error(exception_occ)
            return None
