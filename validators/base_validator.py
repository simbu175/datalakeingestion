# import sys

# sys.path.append(f'/data/codes/Lake_Ingestion_Codes/')
# sys.path.append(f"D:\\Git_AD_Codes\\Lake_Ingestion_Codes\\")
from config import basic_config as con
# from utils.aws_utils import AwsConnect
from utils.aws_utils import AthenaUtils
from utils.customlogger import basiclogger

validator_logger = basiclogger(logname=f"Validations_processing")
validator_exceptions_logger = basiclogger(logname=f"Validations_exceptions", section=f"EXCEPTION")


class BaseValidator:
    def __init__(self, mysql_count_value, config_section, date_ind, glue_db,
                 glue_tbl, glue_column, glue_col_expr, glue_start_val, glue_end_val,
                 control_df, use_wrangler):
        self.mysql_count_value = mysql_count_value
        self.config_section = config_section
        self.date_ind = date_ind
        self.glue_db = glue_db
        self.glue_tbl = glue_tbl
        self.glue_column = glue_column
        self.glue_col_expr = glue_col_expr
        self.glue_start_val = glue_start_val
        self.glue_end_val = glue_end_val
        self.control_df = control_df
        self.use_wrangler = use_wrangler
        # self.athena = AwsConnect(service_name=f'athena', config_section=self.config_section)
        # self.athena_client = self.athena.get_aws_client()

    def compare_count(self):
        # mysql_count = len(self.mysql_df.index)  # Mysql dataframe count - from the MySQL Extract
        try:
            athena_obj = AthenaUtils(service_name=con.athena_service_name,
                                     config_section=self.config_section,
                                     dbname=self.glue_db,
                                     tablename=self.glue_tbl, col_name=self.glue_column,
                                     col_expr=self.glue_col_expr,
                                     col_start_value=self.glue_start_val,
                                     col_end_value=self.glue_end_val,
                                     date_ind=self.date_ind, use_wrangler=self.use_wrangler,
                                     results_bucket_name=con.s3_athena_query_results_bucket,
                                     bucket_key=con.s3_athena_query_results_key)
            athena_count = athena_obj.run_athena_query()
            validator_logger.info(f"Athena count : {athena_count}")

        except Exception:
            athena_count = 0
            validator_exceptions_logger.error(f"Exception occured while fetching athena count")

        if self.mysql_count_value == athena_count and athena_count != 0:
            return f"success", self.mysql_count_value, athena_count
        else:
            return f"validation-failure", self.mysql_count_value, athena_count
