from lib.transform import normalize_column_name
from lib.fetch_data import read_collision_dataset

if __name__ == '__main__':
    # STEP 1: read the collision dataset csv
    collisions_df = read_collision_dataset()

    # STEP 2: normalize column names: lowercase, strip spaces, replace spaces & hyphens with underscores
    collisions_df.columns = [normalize_column_name(col) for col in collisions_df.columns]

