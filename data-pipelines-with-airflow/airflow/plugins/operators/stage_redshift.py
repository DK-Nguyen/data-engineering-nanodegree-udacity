from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Load JSON formatted files from S3 to Amazon Redshift. 
    """
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
        ;
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 clear_table=False,
                 extra_info_sql="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.clear_table = clear_table
        self.extra_info_sql = extra_info_sql
        
    def execute(self, context):
        self.log.info('Loading data from Amazon S3 to Redshift...')

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.clear_table:
            self.log.info(f'Clearing data from {self.table}')
            redshift.run('DELETE FROM {}'.format(self.table))
        
        self.log.info(f'Copying data from S3 to table {self.table} Redshift')
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.extra_info_sql
        )
        self.log.info(f"Executing {formatted_sql}")
        redshift.run(formatted_sql)
        