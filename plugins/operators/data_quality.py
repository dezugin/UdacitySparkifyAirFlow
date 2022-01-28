from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries as q

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Operator params 
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # params map 
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        self.log.info('DataQualityOperator initializing')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for count, query in enumerate(q.null_verify_list):
            self.log.info(f'Verifying null in table {count} ')
            nulls = redshift.run(query)
            self.log.info(f'Table {count} has {nulls} nulls')
        for count, query in enumerate(q.count_verify_list):
            self.log.info(f'Verifying counts in table {count}')
            counts = redshift.run(query)
            self.log.info(f'Table {count} has {counts} rows')
        self.log.info('DataQualityOperator finished')