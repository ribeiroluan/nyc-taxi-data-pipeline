from extract import ExtractTaxiData
from transform import TransformTaxiData
from load_bq import LoadToBQ


if __name__ == '__main__':
    ExtractTaxiData().writer()
    data = TransformTaxiData().create_fact_and_dimensions()
    LoadToBQ(data).load()