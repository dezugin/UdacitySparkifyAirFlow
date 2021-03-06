from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 # operator params
                 redshift_conn_id="",
                 query="",
                 table="",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # params map
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.table= table

    def execute(self, context):
        self.log.info('LoadDimensionOperator started')
        self.log.info("Initializing Redshift connection...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection initialized.")
        sql_query = str(self.query)
        sql = sql_query.format(
            self.table
        )
        self.log.info("Running query")
        redshift.run(sql)
        self.log.info('LoadDimensionOperator finished')
