import os
import pandas as pd
import requests
import logging


class DataProcessor:
    def __init__(self, output_folder):
        self.output_folder = output_folder

    def download_parquet_files(self, base_url, years):
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        downloaded_files = []
        for year in years:
            for month in range(1, 13):
                month_str = str(month).zfill(2)
                url = f"{base_url}{year}-{month_str}.parquet"
                filename = os.path.join(
                    self.output_folder, f"yellow_tripdata_{year}-{month_str}.parquet"
                )
                if not os.path.exists(filename):
                    try:
                        response = requests.get(url)
                        if response.status_code == 200:
                            with open(filename, "wb") as f:
                                f.write(response.content)
                            downloaded_files.append(filename)
                            logging.info(f"Downloaded and stored: {filename}")
                        else:
                            None
                    except Exception as e:
                        logging.error(f"Error occurred while downloading: {e}")
                else:
                    logging.info(f"File already exists: {filename}")
                    downloaded_files.append(filename)
        return downloaded_files

    def append_parquet_files(self, parquet_files, output_file):
        dfs = []
        for file_path in parquet_files:
            try:
                df = pd.read_parquet(file_path)
                dfs.append(df)
            except Exception as e:
                logging.error(
                    f"Error occurred while reading Parquet file {file_path}: {e}"
                )

        combined_df = pd.concat(dfs, ignore_index=True)

        output_file_path = os.path.join(self.output_folder, output_file)

        try:
            combined_df.to_parquet(
                output_file_path, engine="pyarrow", compression="snappy"
            )
            logging.info("Data appended to Parquet file.")
        except Exception as e:
            logging.error(
                f"Error occurred while writing to Parquet file {output_file_path}: {e}"
            )
        return output_file_path

    def calculate_average_trip_length(self, combined_df):
        try:
            if "year" not in combined_df.columns:
                combined_df["year"] = combined_df["tpep_pickup_datetime"].dt.year
            if "month" not in combined_df.columns:
                combined_df["month"] = combined_df["tpep_pickup_datetime"].dt.month

            combined_df["tpep_pickup_datetime"] = pd.to_datetime(
                combined_df["tpep_pickup_datetime"]
            )
            combined_df["tpep_dropoff_datetime"] = pd.to_datetime(
                combined_df["tpep_dropoff_datetime"]
            )

            combined_df["trip_duration"] = (
                combined_df["tpep_dropoff_datetime"]
                - combined_df["tpep_pickup_datetime"]
            ).dt.total_seconds() / 60

            average_trip_lengths = (
                combined_df.groupby(["year", "month"])["trip_duration"]
                .mean()
                .reset_index()
            )

            # Convert to CSV file path
            csv_file_path = os.path.join(self.output_folder, "average_trip_lengths.csv")

            # Save to CSV file
            average_trip_lengths.to_csv(csv_file_path, index=False)

            return average_trip_lengths
        except Exception as e:
            logging.error(f"Error occurred while calculating average trip length: {e}")
            return {}

    def calculate_rolling_average_trip_length(self, combined_df, window_size=45):
        try:
            combined_df["tpep_pickup_datetime"] = pd.to_datetime(
                combined_df["tpep_pickup_datetime"]
            )
            combined_df["tpep_dropoff_datetime"] = pd.to_datetime(
                combined_df["tpep_dropoff_datetime"]
            )
            combined_df["trip_duration"] = (
                combined_df["tpep_dropoff_datetime"]
                - combined_df["tpep_pickup_datetime"]
            ).dt.total_seconds() / 60
            daily_avg_trip_lengths = combined_df.groupby(
                combined_df["tpep_pickup_datetime"].dt.date
            )["trip_duration"].mean()
            rolling_avg_trip_lengths = daily_avg_trip_lengths.rolling(
                window=window_size, min_periods=1
            ).mean()
            csv_file_path = os.path.join(
                self.output_folder, "rolling_avg_trip_lengths.csv"
            )
            rolling_avg_trip_lengths.to_csv(csv_file_path, header=True)
            return rolling_avg_trip_lengths
        except Exception as e:
            logging.error(
                f"Error occurred while calculating rolling average trip length: {e}"
            )
            return pd.Series()
