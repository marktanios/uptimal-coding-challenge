import yaml
import pandas as pd
from pathlib import Path

base_path = Path(__file__).resolve().parent.parent.parent

def load_config():
    try:
        config_path = base_path / "config" / "settings.yaml"
        with open(config_path, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        print(f"Unexpected error happened during reading configuration: {e}")

def save_to_parquet(df: pd.DataFrame):
    """
        Saves a pandas dataframe to parquet

        :param df: pandas dataframe to save
    """
    try:
        config = load_config()
        path_str = config["output"]["final_parquet_path"]
        path = Path(path_str)
        full_path = base_path / path
        df.to_parquet(path=full_path)
        print(f"File saved successfully to {path_str}")
    except Exception as e:
        print(f"Error saving file: {e}")
        return None