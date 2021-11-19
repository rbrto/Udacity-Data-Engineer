from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for dq_check in self.dq_checks:
            self.log.info(f"Checking Data Quality for {dq_check['table']} table")
            records = redshift_hook.get_records(dq_check['check_sql'])
            if records[0][0] != dq_check['expected_result']:
                raise ValueError(f"Data quality check failed: Table {dq_check['table']} contained 0 rows")
            self.log.info(f"Data quality on table {dq_check['table']} check passed with {records[0][0]} records")