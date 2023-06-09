import pandas as pd
import requests
from datetime import datetime
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ExtractTaxiData:
    
    def __init__(self):
        self.url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet"
        self.date = self._get_latest_dataset_date()
    
    def _get_latest_dataset_date(self):
        year = datetime.now().strftime("%Y")
        month = datetime.now().strftime("%m")
        found = False

        while not found:
            logger.info(f"Trying to find data for {year}-{month}")
            if requests.get(url=self.url.format(f"{year}-{month}")).status_code == 200:
                found = True
                logger.info(f"Data found for {year}-{month}!")
            else:
                if month == '01':
                    year = str(int(year) - 1)
                    month = '12'
                else:
                    if int(month) <= 10:
                        month = '0'+str(int(month) - 1)
                    else:
                        month = str(int(month) - 1)
        
        return f"{year}-{month}"
    
    def _get_latest_dataset(self):
        return requests.get(url=self.url.format(self.date))
    
    @property
    def filename(self) -> str:
        return f"airflow/code/tmp/yellow_trip_data.parquet"
    
    def writer(self):
        local_filename = self.filename()
        open(local_filename, 'wb').write(self._get_latest_dataset().content)
        logger.info(f"Data written to {local_filename}")
#ExtractTaxiData().writer()