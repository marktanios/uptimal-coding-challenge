import pandas as pd

def normalize_column_name(col_name):
    normalized_col = col_name.strip().lower().replace(" ", "_").replace("-", "_")
    if normalized_col == 'crash_date':
        normalized_col = 'date'
    return normalized_col

def merge_collision_and_holiday(collision_df: pd.DataFrame, holiday_df: pd.DataFrame) -> pd.DataFrame:
    # convert 'date' column to datetime in both DataFrames
    collision_df['date'] = pd.to_datetime(collision_df['date'])
    holiday_df['date'] = pd.to_datetime(holiday_df['date'])

    # merge on date
    merged_df = collision_df.merge(holiday_df, on='date', how='left')
    merged_df['is_holiday'] = merged_df.apply(
        lambda row: True if row['is_holiday_bool'] and row['global_across_us'] else (
            "US-NY" in row['states'] if row['states'] is not None else False
        ),
        axis=1
    )
    return merged_df
