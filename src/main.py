from lib.transform import normalize_column_name
from lib.fetch_data import get_collision_dataset, get_holiday_data

if __name__ == '__main__':
    # STEP 1: download and read the collision dataset csv
    # collisions_df = get_collision_dataset()

    # STEP 2: normalize column names: lowercase, strip spaces, replace spaces & hyphens with underscores
    # collisions_df.columns = [normalize_column_name(col) for col in collisions_df.columns]

    # STEP 3: fetch holiday data from holiday API
    get_holiday_data()

