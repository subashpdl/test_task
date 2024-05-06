from data_processor import DataProcessor
import logging
import pandas as pd


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("logfile.log"), logging.StreamHandler()],
    )


def main():
    setup_logging()

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
    output_folder = "output_data"
    years = [2024, 2025]
    output_file = "all_yellow_taxi_data.parquet"

    data_processor = DataProcessor(output_folder)

    # Download Parquet files
    parquet_files = data_processor.download_parquet_files(base_url, years)

    # Append Parquet files
    combined_parquet_file = data_processor.append_parquet_files(
        parquet_files, output_file
    )

    # Read the combined Parquet file into a DataFrame
    combined_df = pd.read_parquet(combined_parquet_file)

    # Calculate average trip length for each month
    average_trip_lengths = data_processor.calculate_average_trip_length(combined_df)
    logging.info("Average trip length for each month:")
    for month, avg_trip_length in average_trip_lengths.items():
        logging.info(f"{month}: {avg_trip_length:.2f} minutes")

    # Calculate 45-day rolling average trip length
    rolling_avg_trip_lengths = data_processor.calculate_rolling_average_trip_length(
        combined_df
    )
    logging.info("45-day rolling average trip length:")
    logging.info(rolling_avg_trip_lengths)


if __name__ == "__main__":
    main()
