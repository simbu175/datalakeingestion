import time
import boto3
# import configparser
# import sys
# import awswrangler as wr
import io
import pandas as pd

# sys.path.append(f'/Lake_Ingestion_Codes')
# sys.path.append(f'D:\\Initial_Version\\Lake_Ingestion_Codes\\')
from config import basic_config as con


class AwsConnect:

    def __init__(self, service_name, config_section, region_name=f'us-east-1'):
        # aws_con = configparser.ConfigParser()
        # aws_con.read(con.AWS_CONFIG)
        self.service_name = service_name
        self.config_section = config_section
        self.region_name = region_name
        # self.aws_access_key_id = \
        # aws_con.get(self.config_section, f'aws_access_key_id')
        # self.aws_secret_access_key = \
        # aws_con.get(self.config_section, f'aws_secret_access_key')
        # self.aws_session_token = \
        # aws_con.get(self.config_section, f'aws_session_token')

    def get_aws_client(self):
        aws_client_connect = boto3.client(self.service_name, region_name=self.region_name)
        return aws_client_connect

    def get_aws_resource(self):
        aws_resource_connect = boto3.resource(self.service_name, region_name=self.region_name)
        return aws_resource_connect

    def get_aws_session(self):
        aws_session_connect = boto3.Session(region_name=self.region_name)
        return aws_session_connect


class GlueUtils(AwsConnect):

    def __init__(self, service_name, config_section, crawler_name,
                 use_wrangler=0):
        super().__init__(service_name, config_section)
        self.service_name = con.glue_service_name
        self.crawler_name = crawler_name
        self.use_wrangler = use_wrangler

    def run_glue_crawler(self):
        start_crawler = \
            self.get_aws_client().start_crawler(Name=self.crawler_name)

        if start_crawler[f'ResponseMetadata'][f'HTTPStatusCode'] == 200:
            # Wait till it completes
            while True:
                state = \
                    self.get_aws_client().get_crawler(Name=self.crawler_name)
                curr_st = state[f'Crawler'][f'State']

                if curr_st == f'RUNNING':  # or curr_st == f'STOPPING':
                    time.sleep(con.crawler_sleep_time_in_seconds)
                else:
                    break

            return f'Crawler {self.crawler_name} ran successfully'
        else:
            #  Issues with running the crawler
            return None

    def start_glue_crawler(self):
        # Attempts to run the given crawler from self.crawler_name
        # Check if the crawler is READY for running
        # If yes, call the start_crawler API
        # Else, see if the crawler state is STOPPING from previous run
        # In that case, wait till it changes the state to READY
        # And then run the crawler otherwise exit out of the function

        start_crawler = None
        if self.use_wrangler == 0:
            #  default boto3's execution
            stx = self.get_aws_client().get_crawler(Name=self.crawler_name)

            if stx[f'Crawler'][f'State'] == f'READY':
                start_crawler = self.run_glue_crawler()

            elif stx[f'Crawler'][f'State'] == f'STOPPING':
                #  Crawler is NOT READY or STOPPING from previous run
                while True:
                    state = \
                        self.get_aws_client().get_crawler(Name=self.crawler_name)

                    curr_st = state[f'Crawler'][f'State']
                    if curr_st == f'STOPPING':
                        time.sleep(con.crawler_sleep_time_in_seconds)
                    elif curr_st == f'READY':
                        break
                    else:
                        print(f'Something else happened')

                start_crawler = self.run_glue_crawler()

            return start_crawler

        else:
            # block for starting crawler through aws-wrangler
            # whenever aws-wrangler supports starting crawler
            return None


class AthenaUtils(AwsConnect):

    def __init__(self, service_name, config_section, dbname,
                 tablename, date_ind, col_name=None, col_expr=None,
                 col_start_value=None, col_end_value=None, use_wrangler=1, query=None,
                 results_bucket_name=f'databi-testing',
                 bucket_key=f'LakeIngestion/api-athena-query-results'):
        super().__init__(service_name, config_section)
        self.service_name = con.athena_service_name
        self.dbname = dbname
        self.tablename = tablename
        self.col_name = col_name
        self.col_expr = col_expr
        self.col_start_value = col_start_value
        self.col_end_value = col_end_value
        self.date_ind = date_ind
        self.use_wrangler = use_wrangler
        self.results_bucket_name = results_bucket_name
        self.bucket_key = bucket_key
        self.query = query

    def run_athena_query(self):
        # if the self.query is None, then generate the query
        # otherwise use the query being supplied from the inputs
        query = f""
        if self.query is None:
            # restricting the first 19 characters within the datetime.datetime.now() value
            if self.date_ind == 0:
                if len(self.col_expr) == 0:
                    query = \
                        f"select count(0) t_count from {self.dbname}.{self.tablename} \
                        where {self.col_name} >= timestamp '{self.col_start_value[:19]}'"
                else:
                    if f"{self.dbname}.{self.tablename}" in con.voluminous_tables:
                        expr = self.col_expr
                        if expr[:8] == f'coalesce' or expr[:6] == f'ifnull':
                            expr_col_1 = expr[expr.find('(') + 1:expr.find(',')].strip()
                            expr_col_2 = expr[expr.find(',') + 2:expr.find(')')].strip()
                            query = f"select count(0) t_count from {self.dbname}.{self.tablename} \
                                      where ( {expr_col_1} between timestamp '{self.col_start_value[:19]}' \
                                      and timestamp '{self.col_end_value[:19]}' \
                                      or {expr_col_2} between timestamp '{self.col_start_value[:19]}' \
                                      and '{self.col_end_value[:19]}')"
                    else:
                        query = \
                            f"select count(0) t_count from {self.dbname}.{self.tablename} \
                            where {self.col_expr} >= timestamp '{self.col_start_value[:19]}'"

            else:
                query = \
                    f"select count(0) t_count from {self.dbname}.{self.tablename} \
                    where {self.col_name} >= {int(self.col_start_value)} "

        if self.use_wrangler == 1:
            import awswrangler as wr
            # print(f"Athena query : {query}")
            if self.query is None:
                ctas = True
            else:
                ctas = False
                query = self.query
            df = wr.athena.read_sql_query(sql=query,
                                          database=self.dbname,
                                          ctas_approach=ctas,
                                          boto3_session=self.get_aws_session())
            # print(df[f't_count'][0])
            if self.query is None:
                # this is for fetching the validation counts
                return df[f't_count'][0]
            else:
                return None

        else:
            athena_client = self.get_aws_client()
            query_start = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    f'Database': f"{self.dbname}"
                },
                ResultConfiguration={f'OutputLocation': f's3://{self.results_bucket_name}/{self.bucket_key}'}
            )

            query_status = None
            while query_status == f'QUEUED' or \
                    query_status == f'RUNNING' or query_status is None:
                query_status = athena_client.get_query_execution(
                    QueryExecutionId=query_start[f'QueryExecutionId'])
                query_status = query_status[f'QueryExecution'][f'Status'][f'State']
                print(query_status)
                if query_status == f'FAILED' or query_status == f'CANCELLED':
                    raise Exception(f'Athena query {query} failed or was cancelled')
                time.sleep(con.athena_query_wait_time_in_seconds)

            athena_result = athena_client.get_query_results(
                QueryExecutionId=query_start[f'QueryExecutionId'])
            # print(athena_result[f'ResultSet'][f'Rows'][1]['Data'][0]['VarCharValue'])
            if self.query is None:
                return athena_result[f'ResultSet'][f'Rows'][1]['Data'][0]['VarCharValue']
            else:
                return None


class S3Utils(AwsConnect):
    def __init__(self, service_name, config_section, input_df, file_type,
                 bucket_name=None, bucket_key=None, excel_sheet_name=f'Sheet1',
                 file_full_path=None, use_wrangler=0, file_mode=f'append',
                 pandas_index=False):
        super().__init__(service_name, config_section)
        self.service_name = con.s3_service_name
        self.input_df = input_df
        self.file_type = file_type
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.excel_sheet_name = excel_sheet_name
        self.full_path = file_full_path
        self.use_wrangler = use_wrangler
        self.file_mode = file_mode
        self.pandas_index = pandas_index

    def write_into_s3(self):
        if self.full_path:
            to_file = self.full_path
            # 's3://<bucket-name>/'
            self.bucket_name = to_file[5:to_file.find(f"/", 5)]
            # 's3://<bucket-name>/key_name'
            self.bucket_key = to_file[to_file.find(f"/", 5) + 1:]
        else:
            to_file = f's3://{self.bucket_name}/{self.bucket_key}'

        if self.use_wrangler == 1:
            import awswrangler as wr
            if self.file_type == f'csv':
                data = wr.s3.to_csv(df=self.input_df,
                                    path=to_file, index=self.pandas_index)
            elif self.file_type == f'xlsx':
                data = wr.s3.to_excel(df=self.input_df,
                                      path=to_file,
                                      sheet_name=self.excel_sheet_name,
                                      index=self.pandas_index)
        else:
            if self.file_type == f'csv':
                write_buffer = io.StringIO()
                self.input_df.to_csv(write_buffer, index=self.pandas_index)
            elif self.file_type == f'xlsx':
                write_buffer = io.BytesIO()
                self.input_df.to_excel(write_buffer,
                                       sheet_name=self.excel_sheet_name,
                                       index=self.pandas_index)
            data = self.get_aws_resource().Object(self.bucket_name,
                                                  self.bucket_key).put(
                Body=write_buffer.getvalue())
        return data

    def read_from_s3(self):
        if self.full_path:
            read_file = self.full_path
            # 's3://<bucket-name>/'
            self.bucket_name = read_file[5:read_file.find(f"/", 5)]
            # 's3://<bucket-name>/key_name'
            self.bucket_key = read_file[read_file.find(f"/", 5) + 1:]
        else:
            read_file = f's3://{self.bucket_name}/{self.bucket_key}'

        data = None
        if self.use_wrangler == 1:
            import awswrangler as wr
            if self.file_type == f'xlsx':
                data = wr.s3.read_excel(path=read_file)
            elif self.file_type == f'csv':
                data = wr.s3.read_csv(path=read_file)

        else:
            if self.file_type == f'xlsx' or self.file_type == f'csv':
                try:
                    obj = self.get_aws_client().get_object(Bucket=self.bucket_name,
                                                           Key=self.bucket_key)
                    if self.file_type == f'csv':
                        data = pd.read_csv(obj['Body'], index_col=self.pandas_index)
                    elif self.file_type == f'xlsx':
                        data = pd.read_excel(obj[f'Body'].read(),
                                             sheet_name=self.excel_sheet_name)
                        # data = obj[f'Body'].read()
                except self.get_aws_client().exceptions.NoSuchKey:
                    data = pd.DataFrame([])  # f'No Such file exists'
                except pd.errors.EmptyDataError:
                    data = pd.DataFrame([])  # f'Empty file'

        return data


if __name__ == f"__main__":

    # s3service = AwsConnect(service_name=f's3',
    #                        config_section=f'default_preprod')
    # aws_client = s3service.get_aws_client()
    # aws_resource = s3service.get_aws_resource()
    # aws_session = s3service.get_aws_session()

    glueserv = GlueUtils(service_name=f'glue',
                         config_section=f'default_preprod',
                         crawler_name=f'datalake_lendingstream')
    glue_service = glueserv.start_glue_crawler()

    if glue_service is not None or glue_service != f'Something else happened':
        print(glue_service)
    else:
        print(f'Issues with running the crawler')

    # athenac = AthenaUtils(service_name=f'athena', config_section=f'default_preprod',
    # dbname=f'archive_lendingstream', tablename=f'batch_creation', col_name=f'modified_datetime',
    # col_start_value=f'2017-04-19', col_end_value=f'2017-04-19', date_ind=0, use_wrangler=0,
    # results_bucket_name=f'databi-testing', bucket_key=f'LakeIngestion/api-athena-query-results') athena =
    # athenac.run_athena_query() print(athena)

    """s3_obj = S3Utils(service_name=f's3', config_section=f'default_preprod',
                     input_df=pd.DataFrame({f'col': [1, 2, 3, 4, 5]}), file_type=f'xlsx',
                     bucket_name=f'databi-testing',
                     bucket_key=f'LakeIngestion/control-files/lake_ingestion_control.xlsx',
                     use_wrangler=0, file_mode=f'append')
    # output_data = s3_obj.write_into_s3()
    out_data = s3_obj.read_from_s3()
    if isinstance(out_data, pd.DataFrame):
        print(out_data)
        # control_df = pd.read_excel(io.BytesIO(out_data), sheet_name="ingsetion_control", dtype=str)  # , header=1)
        # print(out_data.start_value.max())
        # dic = out_data.to_dict('records')
        out_dic_data = out_data.query(f"execution_status == 'completed'")
        print(out_dic_data[f'end_value'].max())
        # dict1 = {'lake_table_id': 16, 'execution_status': 'failed', 'start_value': '2020-01-31 00:00:00',
        #          'end_value': '2020-02-01 00:00:00', 'source_count': 22, 'target_count': 22,
        #          'reason_code': 'success-code', 'insert_datetime': '2022-06-10 23:38:03'}
        # dic.append(dict1)
        # put_data = pd.DataFrame(dic)
        # out_data.append(dict1, ignore_index=True)

        # s3_write = S3Utils(service_name=f's3', config_section=f'default_preprod',
        #                    input_df=put_data, file_type=f'csv',
        #                    bucket_name=f'databi-testing',
        #                    bucket_key=f'LakeIngestion/execution_logs/ukl_silambarasan/edw_reassessment_info_drafty/table.csv',
        #                    excel_sheet_name=None,
        #                    use_wrangler=0, file_mode=f'append')
        # print(s3_write.write_into_s3())
    else:
        print(out_data)"""
