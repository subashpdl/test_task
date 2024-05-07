import unittest
from data_processor import DataProcessor
import pandas as pd


class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        # Initialize DataProcessor object
        self.data_processor = DataProcessor(output_folder="output_data")

    def test_calculate_average_trip_length(self):
        # Create a sample DataFrame
        data = {
            "tpep_pickup_datetime": pd.date_range("2024-01-01", periods=100, freq="D"),
            "tpep_dropoff_datetime": pd.date_range("2024-01-01", periods=100, freq="D"),
            "trip_duration": range(100),
        }
        df = pd.DataFrame(data)

        # Calculate average trip length
        result = self.data_processor.calculate_average_trip_length(df)

        # Check if the result is a DataFrame
        self.assertIsInstance(result, pd.DataFrame)

        # Check if the DataFrame has expected columns
        expected_columns = ["year", "month", "trip_duration"]
        self.assertListEqual(list(result.columns), expected_columns)

        # Check if the DataFrame has expected shape
        expected_shape = (1, 3)  # Only one row with year, month, and trip_duration
        self.assertEqual(result.shape, expected_shape)

        # Check if the calculated average trip duration is correct (for this example)
        expected_avg_trip_duration = 49.5  # Sum of range(100) / 100
        self.assertEqual(result["trip_duration"].iloc[0], expected_avg_trip_duration)


if __name__ == "__main__":
    unittest.main()
