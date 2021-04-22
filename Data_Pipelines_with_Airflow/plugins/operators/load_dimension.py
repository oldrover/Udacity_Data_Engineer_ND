from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 delete=False,
                 query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id 
        self.table = table
        self.delete = delete
        self.query = query

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.delete:
            self.log.info("Deleting from table {}".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
                
        self.log.info("Loading Data into dimension table {}".format(self.table))
        
        redshift.run(self.query)