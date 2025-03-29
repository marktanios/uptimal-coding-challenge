import os
import yaml
import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path

base_path = Path(__file__).resolve().parent.parent.parent


def download_csv(url: str, save_path: str, overwrite: bool = False):
    """
    Downloads a CSV file from the given URL and saves it to the specified path.

    Args:
        url (str): The URL of the CSV file.
        save_path (str): The local file path to save the downloaded CSV.
        overwrite (bool): If False, skip download if the file already exists.
    """
    try:
        save_path = Path(save_path)

        # check if file exists
        if save_path.exists() and not overwrite:
            print(f"File already exists: {save_path}")
            return  # Skip download

        response = requests.get(url, stream=True)
        response.raise_for_status()

        # get the total file size from headers (if available)
        total_size = int(response.headers.get("content-length", 0))
        chunk_size = 8192

        with open(save_path, "wb") as file, tqdm(
                desc="Downloading",
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
        ) as progress_bar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                file.write(chunk)
                progress_bar.update(len(chunk))

        print(f"CSV file downloaded successfully: {save_path}")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading CSV: {e}")

def load_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    """
    Reads data from a CSV file and loads it into a Pandas DataFrame.

    :param file_path: Path to the CSV file.
    :return: Pandas DataFrame containing the CSV data.
    """
    try:
        file_path = base_path / file_path
        df = pd.read_csv(
            file_path,
            parse_dates=["CRASH DATE"],
            dtype={"ZIP CODE": str},  # Ensure ZIP CODE is read as a string
            dayfirst=False
        )
        print(f"Successfully loaded {len(df)} rows from {file_path}")
        return df
    except FileNotFoundError:
        print(f"Error: File not found - {file_path}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def load_config():
    try:
        config_path = base_path / "config" / "settings.yaml"
        with open(config_path, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        print(f"Unexpected error happened during reading configuration: {e}")

def read_collision_dataset():
    # loading configuration
    config = load_config()
    collision_dataset_path = config["data_sources"]["collision_dataset_path"]
    collision_dataset_path = os.path.join(base_path, collision_dataset_path)
    collision_dataset_url = config["data_sources"]["collision_dataset_url"]

    # downloading the csv file
    download_csv(collision_dataset_url, collision_dataset_path)

    # loading the csv to dataframe
    raw_collisions_df = load_csv_to_dataframe(collision_dataset_path)

    # filter for only 2025 data
    filtered_collisions_df = raw_collisions_df[raw_collisions_df["CRASH DATE"].dt.year == 2025]
    print(f"Filtered 2025 data rows: {len(filtered_collisions_df)}.")

    # free the memory from raw data
    del raw_collisions_df

    return filtered_collisions_df
