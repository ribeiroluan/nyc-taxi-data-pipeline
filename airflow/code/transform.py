import pandas as pd
import logging
from extract import ExtractTaxiData

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class TransformTaxiData:

    def read_downloaded_data(self) -> pd.DataFrame:
        obj = ExtractTaxiData()
        obj.writer #downloading dataset
        data = pd.read_parquet(obj.filename, engine="fastparquet") #reading dataset as a Pandas datafram
        return data
    
    def create_fact_and_dimensions(self) -> dict:
        df = self.read_downloaded_data()

        df = df.sample(n=10000) #having memory troubles trying to run the entire dataset (more than 3 million rows), so will select a sample

        #Reading a static lookup table that will be usefull when creating the pickupp and dropoff location dimensions
        lookup_table = pd.read_csv("airflow/code/auxiliary_data/zone_lookup.csv")

        #Transforming datetime from object to datetime
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        #Droping duplicates
        df = df.drop_duplicates().reset_index(drop = True)

        #Creating trip_id column
        df['trip_id'] = df.index

        #Creating datetime_dim
        logger.info(f"Creating datetime_dim")
        datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)

        datetime_dim['pickup_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
        datetime_dim['pickup_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
        datetime_dim['pickup_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
        datetime_dim['pickup_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
        datetime_dim['pickup_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

        datetime_dim['dropoff_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
        datetime_dim['dropoff_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
        datetime_dim['dropoff_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
        datetime_dim['dropoff_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
        datetime_dim['dropoff_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

        datetime_dim['datetime_id'] = datetime_dim.index

        datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pickup_hour', 'pickup_day', 'pickup_month', 'pickup_year', 'pickup_weekday',
                                    'tpep_dropoff_datetime', 'dropoff_hour', 'dropoff_day', 'dropoff_month', 'dropoff_year', 'dropoff_weekday']]

        #Creating rate_code_dim
        logger.info(f"Creating rate_code_dim")
        rate_code_type = {
            1:"Standard rate",
            2:"JFK",
            3:"Newark",
            4:"Nassau or Westchester",
            5:"Negotiated fare",
            6:"Group ride"
        }

        rate_code_dim = df[['RatecodeID']].reset_index(drop=True)
        rate_code_dim['rate_code_id'] = rate_code_dim.index
        rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
        rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]

        #Creating pickup_location_dim
        logger.info(f"Creating pickup_location_dim")
        pickup_location_dim = df[['PULocationID']].reset_index(drop=True)
        df_temp = pickup_location_dim.merge(lookup_table, how='left', left_on = 'PULocationID', right_on='LocationID')
        df_temp.rename(columns={"PULocationID":"pickup_location_id", "Borough": "pickup_location_borough", "Zone": "pickup_location_zone"}, inplace=True)
        pickup_location_dim = df_temp[['pickup_location_id','pickup_location_borough','pickup_location_zone']] 

        #Creating dropoff_location_dim
        logger.info(f"Creating dropoff_location_dim")
        dropoff_location_dim = df[['DOLocationID']].reset_index(drop=True)
        df_temp = dropoff_location_dim.merge(lookup_table, how='left', left_on = 'DOLocationID', right_on='LocationID')
        df_temp.rename(columns={"DOLocationID":"dropoff_location_id", "Borough": "dropoff_location_borough", "Zone": "dropoff_location_zone"}, inplace=True)
        dropoff_location_dim = df_temp[['dropoff_location_id','dropoff_location_borough','dropoff_location_zone']]

        #Creating payment_type_dim
        logger.info(f"Creating payment_type_dim")
        payment_type_name = {
            1:"Credit card",
            2:"Cash",
            3:"No charge",
            4:"Dispute",
            5:"Unknown",
            6:"Voided trip"
        }
        payment_type_dim = df[['payment_type']].reset_index(drop=True)
        payment_type_dim['payment_type_id'] = payment_type_dim.index
        payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
        payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]

        #Creating fact table
        logger.info(f"Creating fact_table")
        fact_table = df.merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id') \
                    .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') \
                    .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id')\
                    .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
                    .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') \
                    [['trip_id','VendorID', 'datetime_id', 'rate_code_id', 'store_and_fwd_flag',  
                    'passenger_count', 'trip_distance', 'pickup_location_id', 'dropoff_location_id',
                    'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                    'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'Airport_fee']]
        
        return {
            'datetime_dim':datetime_dim.to_dict(orient='dict'),
            'rate_code_dim':rate_code_dim.to_dict(orient='dict'),
            'pickup_location_dim':pickup_location_dim.to_dict(orient='dict'),
            'dropoff_location_dim':dropoff_location_dim.to_dict(orient='dict'),
            'payment_type_dim':payment_type_dim.to_dict(orient='dict'),
            'fact_table':fact_table.to_dict(orient='dict')
        }

TransformTaxiData().create_fact_and_dimensions()
