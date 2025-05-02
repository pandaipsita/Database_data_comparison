from docx import Document
import re
import pandas as pd
import json


def extract_insert_statements(docx_path):
    """
    Extract INSERT statements from a Word document

    Args:
        docx_path: Path to Word document

    Returns:
        list: List of dictionaries with INSERT statement details
    """
    doc = Document(docx_path)
    full_text = "\n".join([para.text for para in doc.paragraphs])

    # Extract schema name from document filename
    import os
    schema_name = os.path.basename(docx_path).split('.')[0]

    # Find all INSERT statements
    insert_statements = re.findall(r"(INSERT\s+INTO[\s\S]+?;)", full_text, re.IGNORECASE)

    print(f"Found {len(insert_statements)} INSERT statements in {docx_path}")

    parsed_inserts = []
    for stmt in insert_statements:
        # Extract table name
        table_match = re.search(r"INSERT\s+INTO\s+(?:(\w+)\.)?(\w+)", stmt, re.IGNORECASE)
        if not table_match:
            continue

        schema_prefix = table_match.group(1) or schema_name
        table_name = table_match.group(2)

        # Extract column names if present
        column_match = re.search(r"INSERT\s+INTO\s+[\w\.]+\s*\(([^)]+)\)", stmt, re.IGNORECASE)
        columns = []
        if column_match:
            # Extract and clean column names
            columns = [col.strip().strip('`[]"\' ') for col in column_match.group(1).split(',')]
            # Print the extracted columns for debugging
            print(f"  Extracted columns for {table_name}: {columns}")
        else:
            print(f"  No columns found in INSERT statement for {table_name}")

        # Extract values section
        values_section_match = re.search(r"VALUES\s*(.*?);", stmt, re.IGNORECASE | re.DOTALL)
        if values_section_match:
            values_section = values_section_match.group(1)
            # Find all value sets
            value_sets = re.findall(r"\(([^)]+)\)", values_section)

            for values_str in value_sets:
                # Parse values
                values = []
                in_string = False
                current_value = ""

                for char in values_str:
                    if char == "'" and (len(current_value) == 0 or current_value[-1] != '\\'):
                        in_string = not in_string
                        current_value += char
                    elif char == ',' and not in_string:
                        values.append(current_value.strip())
                        current_value = ""
                    else:
                        current_value += char

                # Add the last value
                if current_value.strip():
                    values.append(current_value.strip())

                # Clean values
                cleaned_values = []
                for val in values:
                    val = val.strip()
                    if val.startswith("'") and val.endswith("'"):
                        val = val[1:-1]  # Remove surrounding quotes
                    elif val.lower() == "null":
                        val = None
                    cleaned_values.append(val)

                # If no columns were specified, create generic ones
                if not columns:
                    columns = [f"column_{i + 1}" for i in range(len(cleaned_values))]

                # Create data entry
                if len(columns) == len(cleaned_values):
                    parsed_inserts.append({
                        "schema_name": schema_prefix,
                        "table_name": table_name,
                        "columns": columns,
                        "values": cleaned_values,
                        "raw_statement": stmt
                    })

    print(f"Extracted {len(parsed_inserts)} rows from INSERT statements in {docx_path}")
    return parsed_inserts


def inserts_to_dataframe(insert_list):
    """
    Convert INSERT statements to a pandas DataFrame using actual column names

    Args:
        insert_list: List of INSERT statement dictionaries

    Returns:
        dict: Dictionary of DataFrames keyed by schema.table
    """
    # Group by schema and table
    grouped_data = {}

    for insert in insert_list:
        schema_name = insert["schema_name"]
        table_name = insert["table_name"]
        columns = insert["columns"]

        key = f"{schema_name}.{table_name}"

        if key not in grouped_data:
            grouped_data[key] = {
                "schema": schema_name,
                "table": table_name,
                "columns": columns,  # Use the actual column names
                "data": []
            }

        # Ensure columns are consistent
        if grouped_data[key]["columns"] != columns:
            print(f"⚠️ Warning: Inconsistent columns for {key}:")
            print(f"  Existing: {grouped_data[key]['columns']}")
            print(f"  New: {columns}")
            # Merge columns to ensure all are included
            grouped_data[key]["columns"] = list(set(grouped_data[key]["columns"] + columns))

        grouped_data[key]["data"].append(insert["values"])

    # Convert to DataFrames using actual column names
    dataframes = {}

    for key, group in grouped_data.items():
        if group["data"]:
            try:
                # Use the actual column names from the INSERT statements
                column_names = group["columns"]
                print(f"Creating DataFrame for {key} with actual columns: {column_names}")
                df = pd.DataFrame(group["data"], columns=column_names)
                df["__schema__"] = group["schema"]
                df["__table__"] = group["table"]
                dataframes[key] = df

                # Verify column names in the created DataFrame
                print(f"  Verification - DataFrame columns: {list(df.columns)}")
            except Exception as e:
                print(f"Error creating DataFrame for {key}: {e}")
                # Fallback to generic column names
                df = pd.DataFrame(group["data"])
                df["__schema__"] = group["schema"]
                df["__table__"] = group["table"]
                dataframes[key] = df
                print(f"  Fallback - Using generic columns: {list(df.columns)}")

    return dataframes


def organize_by_schema(dataframes):
    """
    Organize DataFrames by schema name, preserving actual column names

    Args:
        dataframes: Dictionary of DataFrames keyed by schema.table

    Returns:
        dict: Dictionary of schema -> table -> DataFrame
    """
    organized = {}

    for key, df in dataframes.items():
        schema = df["__schema__"].iloc[0]
        table = df["__table__"].iloc[0]

        if schema not in organized:
            organized[schema] = {}

        # Remove __schema__ and __table__ columns but preserve all other columns
        columns_to_drop = ["__schema__", "__table__"]
        columns_to_keep = [col for col in df.columns if col not in columns_to_drop]

        cleaned_df = df[columns_to_keep]
        print(f"Organizing table {table} in schema {schema} with columns: {list(cleaned_df.columns)}")

        organized[schema][table] = cleaned_df

    return organized