import datetime
import copy
import pandas as pd


def generate_data_comparison_report(schema1_data, schema2_data, schema1_name, schema2_name):
    """
    Generate a data comparison report between two schemas

    Args:
        schema1_data (dict): Dictionary of DataFrames for schema 1
        schema2_data (dict): Dictionary of DataFrames for schema 2
        schema1_name (str): Name of schema 1
        schema2_name (str): Name of schema 2

    Returns:
        dict: Data comparison report
    """
    report = {
        "meta": {
            "source_schema": schema1_name,
            "destination_schema": schema2_name,
            "timestamp": datetime.datetime.now().isoformat(),
            "tables_compared": len(set(schema1_data.keys()) & set(schema2_data.keys()))
        },
        "table_comparisons": {},
        "summary": {
            "total_tables": 0,
            "total_rows_source": 0,
            "total_rows_destination": 0,
            "total_matching_rows": 0,
            "total_missing_rows": 0,
            "total_different_rows": 0,
            "total_extra_rows": 0,
            "all_matched": True
        }
    }

    # Get common tables
    common_tables = set(schema1_data.keys()) & set(schema2_data.keys())

    # Compare each table
    for table in common_tables:
        df1 = schema1_data[table].copy()
        df2 = schema2_data[table].copy()

        # Preserve original column names
        df1_columns = list(df1.columns)
        df2_columns = list(df2.columns)

        # Debug print
        print(f"Comparing table {table} with columns:")
        print(f"  Source: {df1_columns}")
        print(f"  Destination: {df2_columns}")

        # Convert all columns to string for comparison
        for df in [df1, df2]:
            for col in df.columns:
                df[col] = df[col].astype(str)

        # Create comparison result
        table_result = {
            "success": True,
            "meta": {
                "source_schema": schema1_name,
                "destination_schema": schema2_name,
                "table": table,
                "timestamp": datetime.datetime.now().isoformat()
            },
            "summary": {
                "rows_in_source": len(df1),
                "rows_in_destination": len(df2),
                "matching_rows": 0,
                "missing_rows": 0,
                "different_rows": 0,
                "extra_rows": 0
            },
            "details": {
                "matching_rows": [],
                "missing_rows": [],
                "different_rows": [],
                "extra_rows": []
            }
        }

        # Identify key column (assume first column is the key)
        key_column = df1_columns[0]

        # Convert DataFrames to records for easier comparison
        df1_records = df1.to_dict('records')
        df2_records = df2.to_dict('records')

        # Dictionary to track df2 rows that have been matched
        df2_matched_indices = set()

        # Compare each row in df1 with rows in df2
        for row1 in df1_records:
            row1_key = row1[key_column]
            matching_rows_in_df2 = [
                (i, row2) for i, row2 in enumerate(df2_records)
                if row2[key_column] == row1_key
            ]

            if matching_rows_in_df2:
                # Found at least one row with matching key
                idx, row2 = matching_rows_in_df2[0]
                df2_matched_indices.add(idx)

                # Check if the rows are identical
                differences = {}
                for col in set(df1_columns) & set(df2_columns):
                    if row1[col] != row2[col]:
                        differences[col] = {
                            "source": row1[col],
                            "destination": row2[col]
                        }

                if not differences:
                    # Exact match
                    table_result["summary"]["matching_rows"] += 1
                    table_result["details"]["matching_rows"].append(row1)
                else:
                    # Same key but different values
                    table_result["summary"]["different_rows"] += 1
                    table_result["details"]["different_rows"].append({
                        "source_row": row1,
                        "destination_row": row2,
                        "differences": differences
                    })
                    table_result["success"] = False
                    report["summary"]["all_matched"] = False
            else:
                # Row in df1 not found in df2
                table_result["summary"]["missing_rows"] += 1
                table_result["details"]["missing_rows"].append(row1)
                table_result["success"] = False
                report["summary"]["all_matched"] = False

        # Find rows in df2 that weren't matched
        for i, row2 in enumerate(df2_records):
            if i not in df2_matched_indices:
                table_result["summary"]["extra_rows"] += 1
                table_result["details"]["extra_rows"].append(row2)
                table_result["success"] = False
                report["summary"]["all_matched"] = False

        # Add table result to report
        report["table_comparisons"][table] = table_result

        # Update overall summary
        report["summary"]["total_tables"] += 1
        report["summary"]["total_rows_source"] += table_result["summary"]["rows_in_source"]
        report["summary"]["total_rows_destination"] += table_result["summary"]["rows_in_destination"]
        report["summary"]["total_matching_rows"] += table_result["summary"]["matching_rows"]
        report["summary"]["total_missing_rows"] += table_result["summary"]["missing_rows"]
        report["summary"]["total_different_rows"] += table_result["summary"]["different_rows"]
        report["summary"]["total_extra_rows"] += table_result["summary"]["extra_rows"]

    return report