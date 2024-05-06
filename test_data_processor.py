import pytest
from data_processor import DataProcessor
import pandas as pd


@pytest.fixture
def data_processor():
    return DataProcessor("output_data")


def test_download_parquet_files(data_processor):
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
    years = [2024]
    parquet_files = data_processor.download_parquet_files(base_url, years)
    assert len(parquet_files) == 2


def test_append_parquet_files(data_processor):
    parquet_files = [
        "test_data/parquet_file1.parquet",
        "test_data/parquet_file2.parquet",
    ]
    output_file = "output_data/test_combined.parquet"
    combined_file = data_processor.append_parquet_files(parquet_files, output_file)
    assert combined_file == output_file


def test_calculate_average_trip_length(data_processor):
    data = {
        "tpep_pickup_datetime": pd.date_range("2024-01-01", periods=100, freq="D"),
        "tpep_dropoff_datetime": pd.date_range("2024-01-01", periods=100, freq="D")
        + pd.Timedelta(hours=1),
        "trip_duration": range(100),
    }
    df = pd.DataFrame(data)
    avg_trip_lengths = data_processor.calculate_average_trip_length(df)
    assert isinstance(avg_trip_lengths, dict)
    assert len(avg_trip_lengths) == 1
    assert list(avg_trip_lengths.keys())[0] == pd.Period("2024-01")


def test_calculate_rolling_average_trip_length(data_processor):
    data = {
        "tpep_pickup_datetime": pd.date_range("2024-01-01", periods=100, freq="D"),
        "tpep_dropoff_datetime": pd.date_range("2024-01-01", periods=100, freq="D")
        + pd.Timedelta(hours=1),
        "trip_duration": range(100),
    }
    df = pd.DataFrame(data)
    rolling_avg_trip_lengths = data_processor.calculate_rolling_average_trip_length(df)
    assert isinstance(rolling_avg_trip_lengths, pd.Series)
    assert len(rolling_avg_trip_lengths) == 100
