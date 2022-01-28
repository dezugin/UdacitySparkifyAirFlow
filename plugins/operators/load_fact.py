from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # operators params
                 redshift_conn_id="",
                 query="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # params map
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.table = table
    
    def execute(self, context):
        self.log.info('LoadFactOperator starting')
        self.log.info("Initializing Redshift connection...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection initialized.")
        sql = LoadFactOperator.query.format(
            self.table
        )
        self.log.info("Running query")
        redshift.run(sql)
        self.log.info('LoadFactOperator finished')