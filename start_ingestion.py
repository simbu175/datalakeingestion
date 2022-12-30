import configparser
import sys
import traceback
from multiprocessing import cpu_count
from multiprocessing.dummy import Pool as ThreadPool

import pandas as pd

# sys.path.append(f'data/codes/Lake_Ingestion_Codes')
# sys.path.append(f"C:\\Projects\\PyCharmRepo\\ArchivalSolution\\Lake_Ingestion_Codes\\")
import config.basic_config as con
from utils import MySQLWrapper as Mysql
from utils.aws_utils import S3Utils
from utils.aws_utils import GlueUtils
from utils.aws_utils import AthenaUtils
from extractors.ingest_mysql_data import IngestTable
from extractors.ingest_files import IngestFile
from validators.base_validator import BaseValidator
from utils.customlogger import basiclogger
from utils.common_utils import CommonUtils

# queue = queue.Queue()
ingestion_logger = basiclogger(logname=f"Ingestion_Logs_default")
exception_logger = basiclogger(logname=f"Ingestion_exceptions", section=f"EXCEPTION")


def extract_and_load_data(ingsetion_id, ingestion_type, servername, dbname,
                          tablename, delta_field, delta_field_expr, date_ind, trg_type,
                          trg_loc, control_data, partition_data, mysql_connect,
                          pandas_connect):
    # Extract the dataframe and iterate it for all active tables
    # Call the extract_and_load_table method for extracting the data from mysql
    # and load it to a corresponding s3 bucket as per the config
    # A similar route is for extracting files and loading to s3

    run_status = None
    if ingestion_type == f'mysql-table':
        extractor = IngestTable(tableid=ingsetion_id, server=servername, dbname=dbname,
                                tablename=tablename, delta_field=delta_field,
                                delta_field_expr=delta_field_expr,
                                date_ind=date_ind, target_type=trg_type,
                                target_location=trg_loc,
                                control_df=control_data,
                                partition_tup=partition_data,
                                mysql_details=mysql_connect,
                                pandas_details=pandas_connect)
        ingestion_logger.info(f"Starting ingestion for {dbname}.{tablename}")
        run_status = extractor.extract_and_load_table()
        return run_status

    elif ingestion_type == f'xlsx-file':
        extractor = IngestFile()
        run_status = extractor.extractloadfile()

        return run_status


def update_catalog(to_update_catalog, aws_conf_section):
    # dist_crawler_names = to_update_catalog['crawler_name'].unique()
    ingestion_logger.info(f"catalog update is in progress")
    for c_name in to_update_catalog:  # to_update_catalog['crawler_name'].unique():
        # crawler_name = c_name
        ingestion_logger.info(f"Starting crawler - {c_name}")
        glue_serv = GlueUtils(service_name=con.glue_service_name,
                              config_section=aws_conf_section,
                              crawler_name=c_name)
        glue_serv.start_glue_crawler()


def validate_data(to_validate, validate_config_sec):
    for index, row in to_validate.iterrows():
        glue_table_id = row['lake_ingestion_id']
        date_indicator = row[f'date_not_available']
        glue_dbname = row['glue_db_name']
        glue_table = row[f'tablename']
        delta_field = row[f'delta_field']
        delta_field_expr = row[f'delta_field_expr']
        validate_db = row[f'databasename']
        validate_tbl = row[f'tablename']
        using_wrangler = 1

        ingestion_logger.info(f"Starting validations for ingsetion : {glue_table_id}")
        recent_log = CommonUtils.get_prev_log_file(db=validate_db, table=validate_tbl,
                                                   current_flag=1)
        ingestion_logger.info(f"Recent log file from, {recent_log}")

        s3_read = S3Utils(service_name=con.s3_service_name,
                          config_section=validate_config_sec,
                          input_df=None, file_type=con.s3_execution_log_file_type,
                          file_full_path=recent_log, use_wrangler=0)
        to_update_data = s3_read.read_from_s3()

        if not to_update_data.empty:
            last_executed_row = \
                to_update_data.query(f"execution_status == 'data-loaded-to-s3'")
            to_update_max = last_executed_row[
                last_executed_row['insert_datetime'] == last_executed_row['insert_datetime'].max()]

            validate_result = f"failure"
            if not to_update_max.empty:
                ingestion_logger.info(
                    f"Mysql count : {to_update_max.iloc[0][f'source_count']} \
                    Start_val : {to_update_max.iloc[0][f'start_value']} \
                    s3_file : {to_update_max.iloc[0][f'reason_code']}")

                validate_mysql_count = 0
                validate_athena_count = 0

                if len(delta_field_expr) == 0:
                    validator_data = \
                        BaseValidator(mysql_count_value=to_update_max.iloc[0][f'source_count'],
                                      config_section=validate_config_sec,
                                      date_ind=date_indicator, glue_db=glue_dbname,
                                      glue_tbl=glue_table, glue_column=delta_field,
                                      glue_col_expr=delta_field_expr,
                                      glue_start_val=to_update_max.iloc[0][f'start_value'],
                                      glue_end_val=to_update_max.iloc[0]['end_value'],
                                      control_df=to_validate,
                                      use_wrangler=using_wrangler)
                    validate_result, validate_mysql_count, validate_athena_count = \
                        validator_data.compare_count()
                    ingestion_logger.info(f"Validation result : {validate_result}")

                #  Read the execution log from s3 path

                ingestion_logger.info(
                    f"Min value : {to_update_max.iloc[0][f'start_value']} and "
                    f"max value : {to_update_max.iloc[0][f'end_value']}")

                # hack to convert the pandas df to a list of dictionaries
                validate_control_dict = to_update_data.to_dict('records')

                #  block for successful validation
                if validate_result == f"success" or len(delta_field_expr) > 0:
                    validate_res = f"success"
                    validate_code = f'validation-success'
                    if len(delta_field_expr) > 0:
                        validate_code = f'validation-skipped'

                else:
                    validate_res = f"validation-failure"
                    validate_code = f'validation-failure'
                    ingestion_logger.info(f"Remove the s3 loaded data as part "
                                          f"of this ingestion as validation failed")
                    CommonUtils.remove_s3_data(s3_files=to_update_max.iloc[0][f'reason_code'])

                # Introduce the athena views and enable it only for
                # First-time-ingestion and not for Subsequent-ingestion
                ingestion_logger.info(f"Checking for First-time-ingestion in logs")
                first_time_ingested_row = \
                    to_update_data.query(f"reason_code == 'First-time-ingestion' "
                                         f"or reason_code == "
                                         f"'First-time-ingestion_voluminous_table'")

                if not first_time_ingested_row.empty:
                    #  call the athena view creation function
                    #  check if the athena view creation is needed as per config input
                    if row[f'view_needed'] == 1:
                        ingestion_logger.info(f"Proceeding for view creation "
                                              f"for {glue_dbname}.{glue_table} ")
                        update_athena_latest_views(glue_table_id,
                                                   validate_config_sec, to_validate)
                        validate_code = f'validate-view_creation-success'

                # creating a temporary dictionary for  validation-success log capture
                start_val = to_update_max.iloc[0][f'start_value']
                end_val = to_update_max.iloc[0][f'end_value']

            else:
                ingestion_logger.info(f"Issues in data extraction from source")
                last_executed_row = \
                    to_update_data.query(f"execution_status == 'started'")
                to_update_max = last_executed_row[
                    last_executed_row['insert_datetime'] == last_executed_row['insert_datetime'].max()]
                validate_control_dict = to_update_data.to_dict('records')

                start_val = to_update_max.iloc[0][f'start_value']
                end_val = to_update_max.iloc[0][f'end_value']
                validate_code = f"validation-did-not-occur"
                validate_res = f"extraction-failure"
                validate_mysql_count = to_update_max.iloc[0][f'source_count']
                validate_athena_count = to_update_max.iloc[0][f'target_count']

            to_insert_dict = \
                CommonUtils.generate_execution_log_dict(
                    ingestion_id=glue_table_id,
                    exec_status=validate_res,
                    start_value=start_val,
                    end_value=end_val,
                    source_count=validate_mysql_count,
                    target_count=validate_athena_count,
                    reason_code=validate_code)
            validate_control_dict.append(to_insert_dict)

            to_update_data = pd.DataFrame(validate_control_dict)
            ingestion_logger.info(f"Updating the validation results to \
                    the s3 execution logs - {validate_result}")

            # write the updated df into the s3 bucket
            if CommonUtils.update_execution_log(to_update_data, validate_db,
                                                validate_tbl, full_path=recent_log):
                ingestion_logger.info(f"Update to s3 execution logs \
                            - {validate_result} - successful")


def read_control_partition_config(service_nm, config_sec, add_filter_group=None):
    # This method gets the control data and
    # partition config from a xlsx file residing in s3
    # The control data is a dataframe which holds
    # the control variable for ingesting data to the lake
    # When using the apply_filter parameter,
    # we have to ensure the filters are valid for query method within pandas
    # The partition config has the partition level details
    # for each table that is to be stored as in s3

    s3_control_data = pd.DataFrame([])
    # removed partition data as it's clubbed to the main excel
    # s3_partition_data = pd.DataFrame([])

    if service_nm == con.s3_service_name:
        if add_filter_group:
            in_filter = f''
            for i in input_groups:
                in_filter = in_filter + f" airflow_dag_group == '{i.strip(' ')}' or "
            if in_filter[-3:] == f'or ':
                in_filter = in_filter[:-3]
            apply_filter = f"active_indicator == 1 and ({in_filter})"
        else:
            apply_filter = f"active_indicator == 1"
        ingestion_logger.info(f"Filter : {apply_filter}")
        s3_control_data = \
            CommonUtils.read_s3_config_as_df(config_section=config_sec,
                                             config_type=f"config-control",
                                             apply_filter=apply_filter)
        # s3_partition_data = \
        #     CommonUtils.read_s3_config_as_df(config_section=config_sec,
        #                                      config_type=f"config-partition")
        ingestion_logger.info(f"Extracted the control config")

    return s3_control_data  # , s3_partition_data


def get_input_data_list(input_config):
    # This method iterates over the list of
    # active tables/files to be loaded to data-lake
    # And returns a list of tuples that would be used for further execution in
    # multiple threads as per the multiprocessing.dummy.Pool class
    # using map and join functions
    # return dictionary of ingestion ids and corresponding s3 load details

    return_status = []

    config = configparser.ConfigParser()
    config.read(con.DB_CONFIG)
    input_config.fillna('', inplace=True)  # converting 'nan' to ''

    for index, row in input_config.iterrows():
        ingsetion_id = row[f'lake_ingestion_id']
        ingestion_type = row[f'ingestion_type']
        server = row[f'servername']
        db = row[f'databasename']
        tablename = row[f'tablename']
        delta_field = row[f'delta_field']
        delta_field_expr = row[f'delta_field_expr']
        # frequency_val = row[f'frequency_value']
        # frequency_uom = row[f'frequency_uom']
        date_indicator = row[f'date_not_available']
        target_type = row[f'target_type']
        target_location = row[f'target_location']
        partition_df_data = eval(row[f'partition_config'])  # read as tuple
        db_user = config.get(db.upper(), f'username')
        db_password = config.get(db.upper(), f'password')
        mysql_conn = Mysql.MySQLWrapper(section=db.upper())
        panda_conn = f'mysql+pymysql://{db_user}:{db_password}@{server}/{db}'

        ingestion_logger.info(f"Preparing for {db}.{tablename} table")
        inner_dicts = \
            {'ingsetion_id': ingsetion_id, 'ingestion_type': ingestion_type,
             'servername': server, 'dbname': db, 'tablename': tablename,
             'delta_field': delta_field, 'delta_field_expr': delta_field_expr,
             'date_ind': date_indicator,
             'trg_type': target_type, 'trg_loc': target_location,
             'control_data': input_config, 'partition_data': partition_df_data,
             'mysql_connection': mysql_conn, 'pandas_connection': panda_conn}
        return_status.append(inner_dicts)

    ingestion_logger.info(f"Data prepped for further processing : {return_status}")
    return return_status


def load_ingested_data(*input_dicts):
    # *args - *input_dicts - Input dictionary within tuple, containing the
    # needed details for starting the respective ingestion
    # Return values from the extract_and_load_data method
    # 1 - table loaded to s3 without partitions
    # 2 - table loaded to s3 with hierarchial partitions (non-time based)
    # 3 - table loaded to s3 with datetime partitions (time based)
    # only for these 3 use-cases we must call the crawler and the validations

    return_status = {}
    ingestion_logger.info(f"Started the thread for ingestion id : "
                          f"{input_dicts[0]['ingsetion_id']}")

    s3_load_return = \
        extract_and_load_data(ingsetion_id=input_dicts[0]['ingsetion_id'],
                              ingestion_type=input_dicts[0]['ingestion_type'],
                              servername=input_dicts[0]['servername'],
                              dbname=input_dicts[0]['dbname'],
                              tablename=input_dicts[0]['tablename'],
                              delta_field=input_dicts[0]['delta_field'],
                              delta_field_expr=input_dicts[0]['delta_field_expr'],
                              date_ind=input_dicts[0]['date_ind'],
                              trg_type=input_dicts[0]['trg_type'],
                              trg_loc=input_dicts[0]['trg_loc'],
                              control_data=input_dicts[0]['control_data'],
                              partition_data=input_dicts[0]['partition_data'],
                              mysql_connect=input_dicts[0]['mysql_connection'],
                              pandas_connect=input_dicts[0]['pandas_connection'])
    ingestion_logger.info(f"Ingested data to s3 : {return_status}")

    # should be the format of the return status for crawling and validations
    try:
        return_status[input_dicts[0]['ingsetion_id']] = s3_load_return

    except Exception:
        error_outcome = traceback.format_exc()
        exception_logger.error(f"Some exceptions happened while loading "
                               f"data to s3 .. {error_outcome}")

    return return_status


def update_athena_latest_views(input_ingestions, def_config, config_df):
    # This method gest the input list of ingestions
    # for which the athena views to be created
    # The config record for the corresponding
    # ingestion id is fetched from the config dataframe
    # For each of the ingestions, we have to ensure the view in athena is created
    # As part of the latest data-zone

    ingestion_logger.info(f"Create the athena views as part of latest data zone")

    # for ing_id in input_ingestions:
    ingestion_logger.info(f"Starting view creation for "
                          f"ingestion_id : {input_ingestions} ")
    athena_ing = config_df[(config_df.lake_ingestion_id == input_ingestions)]

    for index, row in athena_ing.iterrows():
        athena_dbname = row[f'athena_view_db']
        data_db = row['glue_db_name']
        data_table = row[f'tablename']
        athena_view = row[f'athena_view_name']
        delta_field = row[f'delta_field']
        delta_field_expr = row[f'delta_field_expr']
        athena_pkey = row[f'primary_key']
        date_ind = row['date_not_available']

        if len(delta_field_expr) != 0:
            delta_field = delta_field_expr

        athena_query = f"""
                        CREATE OR REPLACE VIEW {athena_dbname}.{athena_view} AS 
                        SELECT *
                        FROM
                          (
                           SELECT
                             *
                           , "row_number"() OVER (PARTITION BY {athena_pkey} 
                             ORDER BY {delta_field} DESC) "row_num"
                           FROM
                             {data_db}.{data_table}
                        ) 
                        WHERE ("row_num" = 1)
                        """
        athena_query = athena_query.replace('\n', '')

        athena_obj = AthenaUtils(service_name=con.athena_service_name,
                                 config_section=def_config,
                                 dbname=athena_dbname, tablename=data_table,
                                 date_ind=date_ind, query=athena_query)
        athena_obj.run_athena_query()

    ingestion_logger.info(f"The athena view is created or replaced "
                          f"for the ingestion : {input_ingestions} ")


def update_crawlers_and_validate_data(ingestion_status, config_section,
                                      s3_control_config_df):
    # Glue Crawlers are called appropriately based on the
    # information available within the control data
    # Based on the ingestion status for each of the table/files,
    # we segregate the need for crawler or not
    # Basically the crawlers to be updated are collected together and stored in a list
    # Count validations between mysql-table/flat-file v
    # athena validations is also delivered
    # Once the validations are successful, we mark the successful
    # completion of the table(s) in the config
    # All these execution logs are updated as a csv file in a s3 location

    # break down the dictionary into a unique list of items with actual crawler names
    ingsetion_ids = []
    ignore_ingestions = []
    crawlers_to_run = []

    #  Pick only the valid ingestions - whose return types are 1, 2 or 3
    ingestion_logger.info(f"Fetching the list of crawlers to run")
    for k, v in ingestion_status.items():
        if v == f'1' or v == f'2' or v == f'3':
            ingsetion_ids.append(k)
        elif v == f"Empty Dataframe":
            ignore_ingestions.append(k)

    for i in ingsetion_ids:
        # crawler_frame_df = s3_control_data.query(f"lake_ingestion_id == {i}")
        crawler_frame_df = s3_control_config_df[s3_control_config_df[
                                                    'lake_ingestion_id'] == i]
        # For a single ingestion_id, there will always be one row and hence,
        # we are using max() to convert the output from df to a cell value
        crawler_name = crawler_frame_df['crawler_name'].max()
        if crawler_name not in crawlers_to_run:
            crawlers_to_run.append(crawler_name)

    ingestion_logger.info(f"Calling start-crawler only for active, "
                          f"successfully s3 loaded cases - {crawlers_to_run}")
    update_catalog(crawlers_to_run, config_section)

    for ing_id in ingestion_status.keys():
        if ing_id not in ignore_ingestions:
            ingestion_logger.info(f"Starting validation for ingestion_id : {ing_id} ")
            validate_ing = \
                s3_control_config_df[(s3_control_config_df.lake_ingestion_id == ing_id)]
            validate_data(validate_ing, config_section)
        else:
            ingestion_logger.info(f"Excluding validations for "
                                  f"ingestion_id : {ing_id} as there is no data in source")

    return ingsetion_ids


if __name__ == "__main__":
    # noinspection PyBroadException
    try:

        input_groups = sys.argv[1].split(',') if len(sys.argv) > 1 else None
        ingestion_logger.info(f"Starting the ingestion load for {input_groups}")
        default_config_read_service = con.s3_service_name
        # f'default_preprod' - f"default_{os.environ['Mode']}"
        default_config_section = con.aws_config_section

        # Read s3 config data
        control_config_df = \
            read_control_partition_config(default_config_read_service,
                                          default_config_section, input_groups)
        # Load the active table/file to s3 data-lake
        # Create a Pool of workers equal to the cpu cores
        pool = ThreadPool(processes=cpu_count())
        input_prep_data = \
            get_input_data_list(control_config_df)

        # Load the function load_ingested_data in their own threads and
        # return the results as list of dictionary
        results = pool.map(load_ingested_data, input_prep_data)
        # close the pool and wait for workers to finish
        pool.close()
        pool.join()

        # This converts the list of dictionary to a dictionary for triggering the crawlers
        input_dict = {}
        for r in results:
            input_dict.update(r)

        # table_loaded_status = \
        #    load_ingested_data(control_config_df, partition_config_df, default_config_section)
        ingestion_logger.info(f"Table load status : {input_dict}")
        # Start the respective crawlers (for tables) and
        # validate them against Mysql through counts
        valid_ingestion_ids = \
            update_crawlers_and_validate_data(input_dict,
                                              default_config_section,
                                              control_config_df)
        # update_athena_latest_views(valid_ingestion_ids,
        #                            default_config_section, control_config_df)
        ingestion_logger.info(f"Ingestion load is complete")

    except Exception as e:
        exception_occ = traceback.format_exc()
        ingestion_logger.error(exception_occ)
        exception_logger.error(exception_occ)
