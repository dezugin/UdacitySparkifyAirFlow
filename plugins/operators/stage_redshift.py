from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # operator params definition
                 redshift_conn_id="",
                 aws_credentials_id="",
                 target_table="",
                 s3_path="",
                 json_paths="",
                 query="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # params map
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_path = s3_path
        self.json_paths = json_paths
        self.query= query
        
    def execute(self, context):
        # Set AWS S3 / Redshift connections
        self.log.info("Initializing Redshift connection...")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection initialized.")
        self.log.info("Deleting data from Redshift target table...")
        redshift.run("DELETE FROM {}".format(self.target_table))

        self.log.info("Preparing for JSON input data")
        query = self.query
        formatted_sql = StageToRedshiftOperator.query.format(
            self.target_table,
            self.s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_paths
        )
        
        # Executing COPY operation
        self.log.info("Executing Redshift COPY operation")
        redshift.run(formatted_sql)
        self.log.info("Redshift COPY operation DONE.")
