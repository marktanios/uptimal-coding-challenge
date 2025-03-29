import yaml
from pathlib import Path
import pandas as pd

base_path = Path(__file__).resolve().parent.parent.parent

def load_csv_to_dataframe(file_path: str) -> pd.DataFrame:
    """
    Reads data from a CSV file and loads it into a Pandas DataFrame.

    :param file_path: Path to the CSV file.
    :return: Pandas DataFrame containing the CSV data.
    """
    try:
        file_path = base_path / file_path
        df = pd.read_csv(file_path, parse_dates=["CRASH DATE"], dayfirst=False)
        print(f"Successfully loaded {len(df)} rows from {file_path}")
        return df
    except FileNotFoundError:
        print(f"Error: File not found - {file_path}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def load_config():
    config_path = base_path / "config" / "settings.yaml"
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def read_collision_dataset():
    config = load_config()
    collision_dataset_path = config["data_sources"]["collision_dataset_path"]
    raw_collisions_df = load_csv_to_dataframe(collision_dataset_path)

    # Now, filter for only 2025 data
    filtered_collisions_df = raw_collisions_df[raw_collisions_df["CRASH DATE"].dt.year == 2025]

    # free the memory from raw data
    del raw_collisions_df

    print(f"Filtered 2025 data len: {len(filtered_collisions_df)}")

    return filtered_collisions_df
