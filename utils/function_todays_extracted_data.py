import os
import pandas as pd
from datetime import datetime
import logging

def get_today_extracted_data(dataset_name, base_dir):
    """
    Locates today's CSV extracted data and returns it as a pandas DataFrame.

    Args:
    - dataset_name: Name of the dataset to identify the CSV folder.
    - base_dir: The base directory where the extracted data is saved.

    Returns:
    - DataFrame containing the data from today's CSV.
    """
    try:
        # Get today's date and build the path
        today = datetime.today()
        year = today.strftime('%Y')
        month = today.strftime('%m')
        day = today.strftime('%d')

        # Construct the folder path to today's data
        folder_path = os.path.join(base_dir, year, month, day, dataset_name)

        # Get all CSV files in the folder (assuming there is only one CSV per day)
        csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

        if not csv_files:
            logging.error(f"No CSV files found for {dataset_name} on {today.strftime('%Y-%m-%d')}")
            return None

        # Assuming the first CSV file is the one to be uploaded (you can adjust this if needed)
        csv_file_path = os.path.join(folder_path, csv_files[0])

        # Load the CSV into a pandas DataFrame
        df = pd.read_csv(csv_file_path)

        # Log the number of records in the CSV
        logging.info(f"Loaded {len(df)} records from {csv_file_path}")
        if 'timestamp_fetched' in df.columns:
            df['timestamp_fetched'] = pd.to_datetime(df['timestamp_fetched'], errors='coerce')
        return df

    except Exception as e:
        logging.error(f"Error while getting data for {dataset_name}: {str(e)}")
        return None
