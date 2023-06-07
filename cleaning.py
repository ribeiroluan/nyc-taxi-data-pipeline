import pandas as pd
from extract import ExtractTaxiData


class TransformTaxiData:

    def _get_filename(self) -> str:
        year = ExtractTaxiData().get_latest_dataset_month()[0]
        month = ExtractTaxiData().get_latest_dataset_month()[1]
        return f"yellow_tripdata_{year}-{month}.parquet"

    def read_downloaded_data(self) -> pd.DataFrame:
        ExtractTaxiData().download_dataset()
        data = pd.read_parquet(self._get_filename(), engine="fastparquet")
        return data
    
    def create_fact_and_dimensions(self):
        df = self.read_downloaded_data()

        #Transforming datetime from object to datetime
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        #Droping duplicates
        df = df.drop_duplicates().reset_index(drop = True)

        #Creating trip_id column
        df['trip_id'] = df.index

        #Creating datetime_dim
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

        #Creating passenger_count_dim
        passenger_count_dim = df[['passenger_count']].reset_index(drop=True)
        passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
        passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]

        #Creating trip_distance_dim
        trip_distance_dim = df[['trip_distance']].reset_index(drop=True)
        trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
        trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]

        #Creating rate_code_dim
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
        pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
        pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
        pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']] 

        #Creating dropoff_location_dim
        dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
        dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
        dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]

        #Creating payment_type_dim
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
        fact_table = df.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id') \
                    .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id') \
                    .merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id') \
                    .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') \
                    .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id')\
                    .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
                    .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') \
                    [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
                    'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
                    'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                    'improvement_surcharge', 'total_amount']]

TransformTaxiData().create_fact_and_dimensions()
