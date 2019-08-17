from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Check if the table is loaded or not.
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 retries=3,
                 dq_checks=None,
                 *args, **kwargs):
        
        # template for dq_checks
#         dq_checks= [
#             {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
#             {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
#         ]
    
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.retries = retries
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('Checking Data Quality')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.dq_checks is None:
            tries = self.retries
            while tries > 0:
                records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {self.table} returned no results")
                    tries = tries - 1
                    continue
                    
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
                    tries = tries - 1
                    continue

            if tries > 0:
                self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
            else:
                self.log.info(f"Data quality on table {self.table} failed after {self.retries} tries")
        else:
            records = redshift_hook.get_records(self.dq_checks.check_sql)
            if records != self.dq_checks.expected_result:
                raise ValueError(f"Data quality check failed: different records than ",
                                 f"expected ({self.dq_checks.expected_result}) records")
