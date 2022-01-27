from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):

    ui_color = '#40BD6E'

    @apply_defaults
    def __init__(self,
                 # operator params
                 redshift_conn_id="",
                 query="",
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        # params map
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        self.log.info('CreateTableOperator started')
        self.log.info("Initializing Redshift connection...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection initialized.")
        self.log.info("Running query")
        query = self.query
        redshift.run(CreateTableOperator.query)
        self.log.info('CreateTableOperator finished')
