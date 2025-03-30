import os
import json
import yaml
import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from datetime import datetime, timedelta

base_path = Path(__file__).resolve().parent.parent.parent

def load_config():
    try:
        config_path = base_path / "config" / "settings.yaml"
        with open(config_path, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        print(f"Unexpected error happened during reading configuration: {e}")

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
        full_save_path = base_path / save_path

        # check if file exists
        if full_save_path.exists() and not overwrite:
            print(f"Download file already exists: {full_save_path}")
            print("Skipping download...")
            return  # Skip download

        response = requests.get(url, stream=True)
        response.raise_for_status()

        # get the total file size from headers (if available)
        total_size = int(response.headers.get("content-length", 0))
        chunk_size = 8192

        with open(full_save_path, "wb") as file, tqdm(
                desc="Downloading",
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
        ) as progress_bar:
            for chunk in response.iter_content(chunk_size=chunk_size):
                file.write(chunk)
                progress_bar.update(len(chunk))

        print(f"CSV file downloaded successfully: {full_save_path}")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading CSV: {e}")

def fetch_and_download_json_data_url(url, json_save_path, overwrite: bool = False):
    """
        Makes a get request to url and saves it to the specified path.

        Args:
            url (str): The URL of the CSV file.
            json_save_path (str): The local file path to save the downloaded JSON response.
            overwrite (bool): If False, skip download if the file already exists.
        """
    try:
        save_path = Path(json_save_path)
        full_save_path = base_path / save_path

        # check if file exists
        if full_save_path.exists() and not overwrite:
            print(f"Response file already exists: {full_save_path}")
            print("Skipping API call...")
            return  # Skip download

        # make the GET request
        print(f"Making get request to url: {url}")
        response = requests.get(url)

        # check if the request was successful (status code 200)
        if response.status_code == 200:
            print("Request was successful.")

            holiday_json_file_path = Path(json_save_path)
            save_path = base_path / holiday_json_file_path
            with open(save_path, "w", encoding="utf-8") as file:
                holidays = response.json()
                json.dump(holidays, file, indent=4)

            print(f"JSON data saved to {save_path}")
        else:
            print(f"Error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Unexpected error happened fetching holiday data: {e}")


def generate_date_dataframe(year: int):
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)

    all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    df = pd.DataFrame({
        'date': all_dates.strftime('%m/%d/%Y'),  # Format as MM/DD/YYYY
        'is_holiday_bool': False,
        'global': False,
        'states': None
    })
    return df

def merge_holiday_data(df: pd.DataFrame, holiday_json: str):
    holidays = json.loads(holiday_json)
    holiday_dict = {h['date']: h for h in holidays}

    for idx, row in df.iterrows():
        date_iso = datetime.strptime(row['date'], '%m/%d/%Y').strftime('%Y-%m-%d')
        if date_iso in holiday_dict:
            df.at[idx, 'is_holiday_bool'] = True
            df.at[idx, 'global_across_us'] = holiday_dict[date_iso]['global']
            df.at[idx, 'states'] = holiday_dict[date_iso]['counties']

    return df

def load_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    """
    Reads data from a CSV file and loads it into a Pandas DataFrame.

    :param file_path: Path to the CSV file.
    :return: Pandas DataFrame containing the CSV data.
    """
    try:
        full_file_path = base_path / file_path
        print(f"Reading {file_path} file as pandas dataframe...")
        df = pd.read_csv(
            full_file_path,
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

def get_collision_dataset():
    # loading configuration
    config = load_config()
    filtration_year = config["filtration"]["year"]
    collision_dataset_path = config["output"]["save_collision_dataset_path"]
    collision_dataset_path = collision_dataset_path.format(year=filtration_year)
    collision_dataset_url = config["data_sources"]["collision_dataset_url"]

    # downloading the csv file
    download_csv(collision_dataset_url, collision_dataset_path)

    # loading the csv to dataframe
    raw_collisions_df = load_csv_to_dataframe(collision_dataset_path)

    # filter for only 2025 data
    filtered_collisions_df = raw_collisions_df[raw_collisions_df["CRASH DATE"].dt.year == filtration_year]
    print(f"Filtered collisions for {filtration_year} year are: {len(filtered_collisions_df)} rows.")

    # free the memory from raw data
    del raw_collisions_df

    return filtered_collisions_df

def get_holiday_data():
    print(f"Getting holidays data...")

    # loading configuration
    config = load_config()
    filtration_year = config["filtration"]["year"]
    holiday_endpoint = config["data_sources"]["holidays_api"]
    holiday_json_file_path = config["output"]["save_holiday_path"]
    holiday_json_file_path = holiday_json_file_path.format(year=filtration_year)
    holiday_api_url = holiday_endpoint.format(year=filtration_year)

    # make the holiday data from url and save it to json file
    fetch_and_download_json_data_url(holiday_api_url, holiday_json_file_path)

    date_df = generate_date_dataframe(int(filtration_year))

    # load holiday JSON
    holiday_json_file_path = Path(holiday_json_file_path)
    json_path = base_path / holiday_json_file_path
    with open(json_path, 'r', encoding='utf-8') as file:
        holiday_json = file.read()

    holiday_df = merge_holiday_data(date_df, holiday_json)
    return holiday_df

