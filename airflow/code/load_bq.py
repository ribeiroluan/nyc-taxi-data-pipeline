from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

credentials = service_account.Credentials.from_service_account_file('airflow/code/data-with-luan-credentials.json')

df = pd.DataFrame(
    {
        'my_string': ['a', 'b', 'c'],
        'my_int64': [1, 2, 3],
        'my_float64': [4.0, 5.0, 6.0],
        'my_timestamp': [
            pd.Timestamp("1998-09-04T16:03:14"),
            pd.Timestamp("2010-09-13T12:03:45"),
            pd.Timestamp("2015-10-02T16:00:00")
        ],
    }
)

df.to_gbq(destination_table="uber_data_pipeline.test", project_id="data-with-luan", if_exists="replace", credentials=credentials)
