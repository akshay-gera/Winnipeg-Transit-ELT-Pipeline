import os
import pandas as pd

def save_df_to_csv(df, dataset_name, base_dir):
    """
    Save a pandas DataFrame to a timestamped CSV file in a nested directory structure.

    Parameters:
        - df: The pandas DataFrame to be saved.
        - dataset_name: The name of the dataset (used in directory and file naming).
        - base_dir: The base directory where the files will be saved. Default is '/usr/local/airflow/extracted_data'.
    
    Returns:
        - file_path: The full path to the saved CSV file.
    """
    try:
        # Get the current time for folder structure and file naming
        current_time = pd.Timestamp.now()
        year = current_time.strftime('%Y')
        month = current_time.strftime('%m')
        day = current_time.strftime('%d')
        dataset_name = f"Raw_{dataset_name}"
        # Create the nested directory structure
        dataset_dir = os.path.join(base_dir, year, month, day, dataset_name)
        os.makedirs(dataset_dir, exist_ok=True)
        
        # Create a timestamp-based filename
        timestamp_str = current_time.strftime('%Y%m%d_%H%M%S')
        file_name = f"{dataset_name}_{timestamp_str}.csv"
        file_path = os.path.join(dataset_dir, file_name)
        
        # Save the DataFrame to CSV
        df.to_csv(file_path, index=False)
        
        print(f"Data saved to: {file_path}")
        return file_path  # Return the file path for reference
    
    except Exception as e:
        print(f"Error saving DataFrame to CSV: {e}")
        return None