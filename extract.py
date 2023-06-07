import pandas as pd
import requests
from datetime import datetime

class ExtractTaxiData:
    
    def __init__(self):
        self.url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}-{}.parquet"
    
    def get_latest_dataset_month(self):
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
        year = self.get_latest_dataset_month()[0]
        month = self.get_latest_dataset_month()[1]
        return requests.get(url=self.url.format(year, month))
    
    def download_dataset(self):
        local_filename = f"yellow_tripdata_{self.get_latest_dataset_month()[0]}-{self.get_latest_dataset_month()[1]}.parquet"
        open(local_filename, 'wb').write(self._get_latest_dataset().content)

ExtractTaxiData().download_dataset()

data = pd.read_parquet("yellow_tripdata_2023-03.parquet", engine="fastparquet")
print(data.columns)