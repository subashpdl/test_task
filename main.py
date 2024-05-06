import os
import pandas as pd
import requests


def download_parquet_files(base_url, output_folder, years):
    """
    Download Parquet data files from the specified base URL for the given years.

    Args:
    - base_url (str): The base URL for the Parquet data files.
    - output_folder (str): The folder where the Parquet files will be saved.
    - years (list): A list of years for which data should be downloaded.

    Returns:
    - list: The list of downloaded Parquet file paths.
    """

    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Function to download and store Parquet file
    def download_and_store_parquet(url, output_folder):
        # Extract filename from URL
        filename = os.path.join(output_folder, url.split("/")[-1])

        # Check if file already exists in the output folder
        if os.path.exists(filename):
            print(f"File already exists: {filename}")
            return filename

        # Download Parquet file
        response = requests.get(url)
        if response.status_code == 200:
            # Write Parquet file to the output folder
            with open(filename, "wb") as f:
                f.write(response.content)
            print(f"Downloaded and stored: {filename}")
            return filename
        else:

            return None

    downloaded_files = []

    # Iterate over years and months
    for year in years:
        for month in range(1, 13):  # January to December
            # Pad month with leading zero if necessary
            month_str = str(month).zfill(2)

            # Construct URL
            url = f"{base_url}{year}-{month_str}.parquet"

            # Call function to download and store Parquet file
            file_path = download_and_store_parquet(url, output_folder)
            if file_path:
                downloaded_files.append(file_path)

    return downloaded_files


def append_parquet_files(parquet_files, output_folder, output_file):
    """
    Append Parquet data files into a single Parquet file.

    Args:
    - parquet_files (list): The list of paths to the Parquet files to be appended.
    - output_folder (str): The folder where the output Parquet file will be saved.
    - output_file (str): The filename of the output Parquet file.

    Returns:
    - str: The path to the combined Parquet file.
    """

    # Initialize an empty list to store DataFrames for each Parquet file
    dfs = []

    # Read data from each Parquet file into a DataFrame and append to the list
    for file_path in parquet_files:
        df = pd.read_parquet(file_path)
        dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)

    # Define the path to the output Parquet file
    output_file_path = os.path.join(output_folder, output_file)

    # Write the combined DataFrame to the output Parquet file
    combined_df.to_parquet(output_file_path, engine="pyarrow", compression="snappy")

    print("Data appended to Parquet file.")
    return output_file_path


def calculate_average_trip_length(combined_df):
    """
    Calculate the average trip length for each month using the combined DataFrame.

    Args:
    - combined_df (DataFrame): The combined DataFrame containing trip data.

    Returns:
    - dict: A dictionary containing the average trip length for each month.
    """

    # Convert pickup and dropoff datetime columns to datetime objects
    combined_df["tpep_pickup_datetime"] = pd.to_datetime(
        combined_df["tpep_pickup_datetime"]
    )
    combined_df["tpep_dropoff_datetime"] = pd.to_datetime(
        combined_df["tpep_dropoff_datetime"]
    )

    # Calculate trip duration in minutes
    combined_df["trip_duration"] = (
        combined_df["tpep_dropoff_datetime"] - combined_df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    # Group by month and calculate average trip length for each month
    average_trip_lengths = (
        combined_df.groupby(combined_df["tpep_pickup_datetime"].dt.to_period("M"))[
            "trip_duration"
        ]
        .mean()
        .to_dict()
    )

    return average_trip_lengths


# Example usage:
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
output_folder = "output_data"
years = [2024, 2025]

# Download Parquet files
parquet_files = download_parquet_files(base_url, output_folder, years)

# Append Parquet files
output_file = "all_yellow_taxi_data.parquet"
combined_parquet_file = append_parquet_files(parquet_files, output_folder, output_file)

# Read the combined Parquet file into a DataFrame
combined_df = pd.read_parquet(combined_parquet_file)

# Calculate average trip length for each month
average_trip_lengths = calculate_average_trip_length(combined_df)
print("Average trip length for each month:")
for month, avg_trip_length in average_trip_lengths.items():
    print(f"{month}: {avg_trip_length:.2f} minutes")


def calculate_rolling_average_trip_length(combined_df, window_size=45):
    """
    Calculate the 45-day rolling average trip length using the combined DataFrame.

    Args:
    - combined_df (DataFrame): The combined DataFrame containing trip data.
    - window_size (int): The size of the rolling window in days (default is 45).

    Returns:
    - DataFrame: A DataFrame containing the 45-day rolling average trip length for each day.
    """

    # Convert pickup and dropoff datetime columns to datetime objects
    combined_df["tpep_pickup_datetime"] = pd.to_datetime(
        combined_df["tpep_pickup_datetime"]
    )
    combined_df["tpep_dropoff_datetime"] = pd.to_datetime(
        combined_df["tpep_dropoff_datetime"]
    )

    # Calculate trip duration in minutes
    combined_df["trip_duration"] = (
        combined_df["tpep_dropoff_datetime"] - combined_df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    # Group by date and calculate average trip length for each day
    daily_avg_trip_lengths = combined_df.groupby(
        combined_df["tpep_pickup_datetime"].dt.date
    )["trip_duration"].mean()

    # Calculate rolling average trip length using the rolling window
    rolling_avg_trip_lengths = daily_avg_trip_lengths.rolling(
        window=window_size, min_periods=1
    ).mean()

    return rolling_avg_trip_lengths


# Calculate 45-day rolling average trip length
rolling_avg_trip_lengths = calculate_rolling_average_trip_length(combined_df)

print("45-day rolling average trip length:")
print(rolling_avg_trip_lengths)
