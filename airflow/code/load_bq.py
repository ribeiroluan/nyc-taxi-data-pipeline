from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from transform import TransformTaxiData
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class LoadToBQ:
    
    def __init__(self, data:dict):
        self.data = data

    def _get_bq_credentials(self):
        return service_account.Credentials.from_service_account_file('airflow/code/data-with-luan-credentials.json')

    def load(self):
        for df_name, df in self.data.items():
            df.to_gbq(
                destination_table=f"uber_data_pipeline.{df_name}", 
                project_id="data-with-luan", 
                if_exists="replace", 
                credentials=self._get_bq_credentials()
            )
            logger.info(f"{df_name} loaded to BigQuery!")