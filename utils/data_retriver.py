import pandas as pd
import json
import hashlib


def detect_primary_key(df):
    """
    Attempt to detect primary key column

    Args:
        df: DataFrame to analyze

    Returns:
        str: Name of likely primary key column or None
    """
    # Look for columns with 'id' in the name
    id_columns = [col for col in df.columns if 'id' in col.lower() and col not in ['__schema__', '__table__']]

    if id_columns:
        # Check if any of these have unique values
        for col in id_columns:
            if df[col].nunique() == len(df):
                return col

    # If no ID column found, check other columns for uniqueness
    for col in df.columns:
        if col not in ['__schema__', '__table__'] and df[col].nunique() == len(df):
            return col

    return None


def calculate_row_hash(row):
    """
    Calculate a hash for a row based on its values

    Args:
        row: Pandas Series representing a row

    Returns:
        str: Hash value for the row
    """
    # Drop metadata columns if they exist
    if "__schema__" in row.index:
        row = row.drop(["__schema__", "__table__"])

    # Convert row to string and hash
    row_str = json.dumps(row.to_dict(), sort_keys=True)
    return hashlib.md5(row_str.encode()).hexdigest()


def find_matching_rows(df1, df2, primary_key=None):
    """
    Find matching rows between two dataframes

    Args:
        df1: First DataFrame
        df2: Second DataFrame
        primary_key: Column name for primary key

    Returns:
        dict: Dictionary mapping rows from df1 to df2
    """
    matches = {}

    # Drop metadata columns if they exist
    if "__schema__" in df1.columns:
        df1_clean = df1.drop(["__schema__", "__table__"], axis=1)
    else:
        df1_clean = df1.copy()

    if "__schema__" in df2.columns:
        df2_clean = df2.drop(["__schema__", "__table__"], axis=1)
    else:
        df2_clean = df2.copy()

    # If primary key is specified, use it for matching
    if primary_key and primary_key in df1_clean.columns and primary_key in df2_clean.columns:
        for idx1, row1 in df1_clean.iterrows():
            pk_value = row1[primary_key]
            matching_rows = df2_clean[df2_clean[primary_key] == pk_value]

            if not matching_rows.empty:
                matches[idx1] = matching_rows.index[0]
    else:
        # Otherwise, try to match based on row hash
        df1_hashes = df1_clean.apply(calculate_row_hash, axis=1)
        df2_hashes = df2_clean.apply(calculate_row_hash, axis=1)

        for idx1, hash1 in df1_hashes.items():
            matches_in_df2 = df2_hashes[df2_hashes == hash1].index

            if not matches_in_df2.empty:
                matches[idx1] = matches_in_df2[0]

    return matches


def get_common_tables(schema1_data, schema2_data):
    """
    Find common tables between two schemas

    Args:
        schema1_data: Dictionary of tables for first schema
        schema2_data: Dictionary of tables for second schema

    Returns:
        set: Set of common table names
    """
    return set(schema1_data.keys()) & set(schema2_data.keys())