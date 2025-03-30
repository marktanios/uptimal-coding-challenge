from lib.utils import save_to_parquet
from lib.fetch_data import get_collision_dataset, get_holiday_data
from lib.transform import normalize_column_name, merge_collision_and_holiday

if __name__ == '__main__':
    # STEP 1: download and read the collision dataset csv as pandas dataframe
    collisions_df = get_collision_dataset()

    # STEP 2: normalize column names: lowercase, strip spaces, replace spaces & hyphens with underscores
    collisions_df.columns = [normalize_column_name(col) for col in collisions_df.columns]

    # STEP 3: fetch holiday data from holiday API and merge it with the year calendar
    holiday_data_df = get_holiday_data()

    # STEP 4: merge collision and holiday dataframes
    merged_df = merge_collision_and_holiday(collisions_df, holiday_data_df)

    # STEP 5: save output to parquet
    save_to_parquet(merged_df)
