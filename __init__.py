from airflow.plugins_manager import AirflowPlugin
from spreadsheet_plugin.operators.s3_to_spreadsheet_operator import S3ToSpreadsheetOperator


class S3ToSpreadsheetPlugin(AirflowPlugin):
    name = "s3_to_spreadsheet_plugin"
    operators = [S3ToSpreadsheetOperator]
