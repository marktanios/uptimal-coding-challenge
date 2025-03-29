
def normalize_column_name(col_name):
    normalized_col = col_name.strip().lower().replace(" ", "_").replace("-", "_")
    return normalized_col