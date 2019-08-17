from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Load the data from staging tables into dimension tables.
    """
    
    ui_color = '#80BD9E'
    
    insert_sql = """
        INSERT INTO {}
        {}
        ;
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_command="",
                 clear_table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_command = sql_command
        self.clear_table = clear_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.clear_table:
            self.log.info(f'Clearing data from {self.table}')
            redshift.run('DELETE FROM {}'.format(self.table))
        
        self.log.info(f'Loading data into dimension table {self.table}')
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql_command
        )
        self.log.info(f"Executing {formatted_sql}")
        redshift.run(formatted_sql)
        
        
        