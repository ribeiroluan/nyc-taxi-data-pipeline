import pandas as pd
import requests
from datetime import datetime
import logging

class ExtractTaxiData:
    
    def __init__(self):
        self.url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}-{}.parquet"
    
    def _get_latest_dataset_date(self):
        year = datetime.now().strftime("%Y")
        month = datetime.now().strftime("%m")
        found = False

        while not found:
            if requests.get(url=self.url.format(year, month)).status_code == 200:
                found = True
            else:
                if month == '01':
                    year = str(int(year) - 1)
                    month = '12'
                else:
                    if int(month) < 10:
                        month = '0'+str(int(month) - 1)
                    else:
                        month = str(int(month) - 1)
        return (year, month)
    
    def _get_latest_dataset(self):
        year = self._get_latest_dataset_date()[0]
        month = self._get_latest_dataset_date()[1]
        return requests.get(url=self.url.format(year, month))
    
    def _get_filename(self) -> str:
        return "airflow/code/tmp/yellow_tripdata_{}-{}.parquet".format(self._get_latest_dataset_date()[0], self._get_latest_dataset_date()[1])
    
    def writer(self):
        local_filename = self._get_filename()
        open(local_filename, 'wb').write(self._get_latest_dataset().content)

ExtractTaxiData().writer()