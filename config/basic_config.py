import os
import datetime

crawler_sleep_time_in_seconds = 25
db_connect_retry_attempts = 3
athena_query_wait_time_in_seconds = 3
voluminous_tables = [f'lendingstream.application', ]
normal_chunk_size = 400000
voluminous_chunks = 100000

BASE_PATH = os.environ['PROJECT_PATH']  # '/data/codes'
# BASE_PATH = f"D:\\Git_AD_Codes\\"
DB_CONFIG = BASE_PATH + f"/Lake_Ingestion_Codes/config/db.config"
# DB_CONFIG = BASE_PATH + f"\\Lake_Ingestion_Codes\\config\\db.config"
LOGGER_FILE = BASE_PATH + f"/Lake_Ingestion_Codes/config/logger.config"
# LOGGER_FILE = BASE_PATH + f"\\Lake_Ingestion_Codes\\config\\logger.config"
MAPPING_CONFIG = BASE_PATH + f"/Lake_Ingestion_Codes/config/datatype_mapping.config"
# MAPPING_CONFIG = BASE_PATH + f"Lake_Ingestion_Codes\\config\\datatype_mapping.config"
AWS_CONFIG = BASE_PATH + f"/Lake_Ingestion_Codes/config/aws.config"
# AWS_CONFIG = f"D:\\Git_AD_Codes\\Lake_Ingestion_Codes\\config\\aws.config"
LOG_PATH = BASE_PATH + f"/Lake_Ingestion_Codes/Logs/"
# LOG_PATH = BASE_PATH + f"Lake_Ingestion_Codes\\Logs\\"

""" DO CHANGE THESE VALUES WITH ABSOLUTE PRECAUTION """
s3_service_name = f"s3"
glue_service_name = f"glue"
athena_service_name = f"athena"

aws_config_section = f"default_preprod"  # f"default_{os.environ['Mode']}"
s3_execution_log_bucket_name = f"databi-testing"
s3_execution_log_bucket_key_prefix = f"LakeIngestion/execution_logs"
s3_execution_log_file_type = f"csv"

s3_config_bucket = f"databi-testing"
s3_control_config_key = f"LakeIngestion/control-files/lake_ingestion_control_local.xlsx"
s3_new_config_key = f"LakeIngestion/control-files/new_lake_ingestion_control_local.xlsx"
s3_control_sheet_name = f"ingestion_control"
s3_config_file = f"s3://{s3_config_bucket}/{s3_control_config_key}"

s3_partition_bucket = f"databi-testing"
s3_partition_config_key = f"LakeIngestion/control-files/lake_ingestion_partition_config_local.xlsx"
s3_partition_sheet_name = f"partition_config"
s3_partition_file = f"s3://{s3_partition_bucket}/{s3_partition_config_key}"

s3_athena_query_results_bucket = f"databi-testing"
s3_athena_query_results_key = f"LakeIngestion/api-athena-query-results"

execution_log_format = {f'lake_ingestion_id': None, f'execution_status': None,
                        f'start_value': None, f'end_value': None,
                        f'source_count': None, f'target_count': None,
                        f'reason_code': None, f'insert_datetime': datetime.datetime.now()}

execution_log_header_list = [f'lake_ingestion_id', f'execution_status',
                             f'start_value', f'end_value',
                             f'source_count', f'target_count',
                             f'reason_code', f'insert_datetime']

""" Files ingestion - BingAds/GoogleAds/DiscoveryAds """
zmailid = f""
zmailpassword = f""
bing_mailsender = f"adctr@microsoft.com"
bing_subject = f"Your scheduled report is ready to view"
bing_mailfolder = f"Inbox"
attachment_path = os.path.join(LOG_PATH, "PPC_Files/")
file_ppc_webmail_url = f"webmail.lendingstream.co.uk"
file_ppc_webmail_port = 993
