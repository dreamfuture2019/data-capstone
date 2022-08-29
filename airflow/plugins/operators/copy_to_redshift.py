from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class CopyToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {} {}
        REGION '{}'
        {};
    """
    copy_sql_parquet = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS PARQUET;
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 file_format="",
                 format_path="",
                 additional="",
                 *args, **kwargs):

        super(CopyToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format= file_format
        self.format_path = format_path
        self.additional = additional
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        self.log.info('CopyToRedshiftOperator init external connection')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('CopyToRedshiftOperator start executing')
        self.log.info(f'CopyToRedshiftOperator delete data table {self.table}')
        redshift.run(f"DELETE FROM {self.table}")
        self.log.info(f'CopyToRedshiftOperator deleted data table {self.table} successfully')
        
        self.log.info('CopyToRedshiftOperator copy data from s3 to redshift table')
        
        s3_path = "s3://{}".format(self.s3_bucket)
        if self.execution_date:
            self.log.info('CopyToRedshiftOperator load data for specific date')
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            day = self.execution_date.strftime("%d")
            s3_path = '/'.join([s3_path, str(year), str(month), str(day)])
        s3_path = s3_path + '/' + self.s3_key
        
        self.log.info('CopyToRedshiftOperator start formatting the sql')
        
        self.log.info(f'CopyToRedshiftOperator start formatting the sql {self.file_format}')
        
        formatted_sql = CopyToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.file_format,
                self.format_path,
                self.region,
                self.additional
            )
        
        if self.file_format=="parquet":
                formatted_sql = CopyToRedshiftOperator.copy_sql_parquet.format(
                    self.table,
                    s3_path,
                    credentials.access_key,
                    credentials.secret_key
                )
        
        self.log.info('CopyToRedshiftOperator start running sql to copy')
        redshift.run(formatted_sql)
        self.log.info(f'CopyToRedshiftOperator running {self.table} successfully')


