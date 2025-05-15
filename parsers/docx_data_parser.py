from docx import Document
import re
import pandas as pd
import json
import os


def extract_insert_statements_from_text(file_path):
    """
    Extract INSERT statements from a plain text or SQL file
    """
    try:
        # Extract schema name from document filename
        schema_name = os.path.basename(file_path).split('.')[0]
        print(f"DEBUG: Extracting from text file: {file_path}, Schema: {schema_name}")

        # Read file content with different encodings for better compatibility
        encodings = ['utf-8', 'latin-1', 'cp1252']
        content = None

        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as file:
                    content = file.read()
                    print(f"DEBUG: Successfully read file with {encoding} encoding")
                    break
            except UnicodeDecodeError:
                continue

        if content is None:
            print(f"ERROR: Could not read file {file_path} with any encoding")
            return []

        # Find all INSERT statements
        insert_statements = re.findall(
            r"(INSERT\s+INTO[\s\S]+?;)",
            content,
            re.IGNORECASE | re.MULTILINE
        )

        print(f"DEBUG: Found {len(insert_statements)} INSERT statements in {file_path}")

        parsed_inserts = []
        for idx, stmt in enumerate(insert_statements):
            print(f"DEBUG: Processing statement {idx + 1}/{len(insert_statements)}")

            # Extract table name
            table_match = re.search(r"INSERT\s+INTO\s+(?:(\w+)\.)?(\w+)", stmt, re.IGNORECASE)
            if not table_match:
                print(f"DEBUG: No table name found in statement {idx + 1}")
                continue

            schema_prefix = table_match.group(1) or schema_name
            table_name = table_match.group(2)
            print(f"DEBUG: Table: {table_name}, Schema: {schema_prefix}")

            # Extract column names if present
            column_match = re.search(r"INSERT\s+INTO\s+[\w\.]+\s*\(([^)]+)\)", stmt, re.IGNORECASE)
            columns = []
            if column_match:
                columns_str = column_match.group(1)
                columns = [col.strip().strip('`[]"\' ') for col in columns_str.split(',')]
                print(f"DEBUG: Columns found: {columns}")
            else:
                print(f"DEBUG: No columns specified in INSERT statement")

            # Extract values section
            values_section_match = re.search(r"VALUES\s*(.*?)(?:;|$)", stmt, re.IGNORECASE | re.DOTALL)
            if values_section_match:
                values_section = values_section_match.group(1)
                # Find all value sets - improved regex to handle nested parentheses
                value_sets = []
                temp_value = ""
                paren_count = 0
                in_string = False

                for char in values_section:
                    if char in ["'", '"'] and (not temp_value or temp_value[-1] != '\\'):
                        in_string = not in_string

                    if not in_string:
                        if char == '(':
                            paren_count += 1
                        elif char == ')':
                            paren_count -= 1

                    temp_value += char

                    if paren_count == 0 and char == ')':
                        # Extract the content between parentheses
                        value_content = re.search(r'\((.*)\)', temp_value)
                        if value_content:
                            value_sets.append(value_content.group(1))
                        temp_value = ""

                print(f"DEBUG: Found {len(value_sets)} value sets")

                for values_str in value_sets:
                    # Parse values more robustly
                    values = []
                    in_string = False
                    current_value = ""
                    quote_char = None

                    i = 0
                    while i < len(values_str):
                        char = values_str[i]

                        if not in_string:
                            if char in ["'", '"']:
                                in_string = True
                                quote_char = char
                                current_value = ""
                            elif char == ',':
                                if current_value.strip():
                                    values.append(current_value.strip())
                                current_value = ""
                            else:
                                current_value += char
                        else:
                            if char == quote_char and (i + 1 >= len(values_str) or values_str[i + 1] != quote_char):
                                in_string = False
                                values.append(current_value)
                                current_value = ""
                                quote_char = None
                            elif char == quote_char and i + 1 < len(values_str) and values_str[i + 1] == quote_char:
                                current_value += char
                                i += 1  # Skip the escaped quote
                            else:
                                current_value += char

                        i += 1

                    # Add the last value
                    if current_value.strip():
                        values.append(current_value.strip())

                    # Clean values
                    cleaned_values = []
                    for val in values:
                        val = val.strip()
                        if val.lower() == "null":
                            val = None
                        elif val.startswith("'") and val.endswith("'"):
                            val = val[1:-1].replace("''", "'")  # Handle escaped quotes
                        elif val.startswith('"') and val.endswith('"'):
                            val = val[1:-1].replace('""', '"')  # Handle escaped quotes
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
                            "raw_statement": stmt[:100] + "..." if len(stmt) > 100 else stmt
                        })
                    else:
                        print(
                            f"WARNING: Column count ({len(columns)}) doesn't match value count ({len(cleaned_values)})")

        print(f"DEBUG: Successfully extracted {len(parsed_inserts)} rows from {file_path}")
        return parsed_inserts

    except Exception as e:
        print(f"ERROR: Failed to process text file {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return []


def extract_insert_statements(file_path):
    """
    Extract INSERT statements from a document, automatically detecting file type
    """
    try:
        # Determine file type based on extension
        file_extension = file_path.lower().split('.')[-1]
        print(f"DEBUG: Processing file {file_path} with extension: {file_extension}")

        # Extract schema name from document filename
        schema_name = os.path.basename(file_path).split('.')[0]

        if file_extension == 'docx':
            print(f"DEBUG: Processing as DOCX file")
            # Process Word document
            doc = Document(file_path)
            full_text = "\n".join([para.text for para in doc.paragraphs])

            # Find all INSERT statements
            insert_statements = re.findall(r"(INSERT\s+INTO[\s\S]+?;)", full_text, re.IGNORECASE)

            print(f"Found {len(insert_statements)} INSERT statements in {file_path}")

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
                    columns = [col.strip().strip('`[]"\' ') for col in column_match.group(1).split(',')]
                    print(f"  Extracted columns for {table_name}: {columns}")
                else:
                    print(f"  No columns found in INSERT statement for {table_name}")

                # Extract values section
                values_section_match = re.search(r"VALUES\s*(.*?);", stmt, re.IGNORECASE | re.DOTALL)
                if values_section_match:
                    values_section = values_section_match.group(1)
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
                                val = val[1:-1]
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

            print(f"Extracted {len(parsed_inserts)} rows from INSERT statements in {file_path}")
            return parsed_inserts

        elif file_extension in ['txt', 'sql']:
            print(f"DEBUG: Processing as {file_extension.upper()} file")
            result = extract_insert_statements_from_text(file_path)
            print(f"DEBUG: Extracted {len(result)} statements from {file_extension} file")
            return result
        else:
            print(f"ERROR: Unsupported file type: {file_extension}")
            return []

    except Exception as e:
        print(f"ERROR: Failed to process file {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return []


def inserts_to_dataframe(insert_list):
    """
    Convert INSERT statements to a pandas DataFrame using actual column names
    """
    if not insert_list:
        print("DEBUG: No inserts to convert to dataframe")
        return {}

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
                "columns": columns,
                "data": []
            }

        # Ensure columns are consistent
        if grouped_data[key]["columns"] != columns:
            print(f"⚠️ Warning: Inconsistent columns for {key}:")
            print(f"  Existing: {grouped_data[key]['columns']}")
            print(f"  New: {columns}")
            # Use the longer column list
            if len(columns) > len(grouped_data[key]["columns"]):
                grouped_data[key]["columns"] = columns

        grouped_data[key]["data"].append(insert["values"])

    # Convert to DataFrames using actual column names
    dataframes = {}

    for key, group in grouped_data.items():
        if group["data"]:
            try:
                column_names = group["columns"]
                print(f"DEBUG: Creating DataFrame for {key} with columns: {column_names}")

                # Ensure all rows have the same number of values as columns
                cleaned_data = []
                for row in group["data"]:
                    if len(row) == len(column_names):
                        cleaned_data.append(row)
                    elif len(row) < len(column_names):
                        # Pad with None values
                        cleaned_data.append(row + [None] * (len(column_names) - len(row)))
                    else:
                        # Truncate extra values
                        cleaned_data.append(row[:len(column_names)])

                df = pd.DataFrame(cleaned_data, columns=column_names)
                df["__schema__"] = group["schema"]
                df["__table__"] = group["table"]
                dataframes[key] = df

                print(f"DEBUG: Created DataFrame for {key} with shape: {df.shape}")
            except Exception as e:
                print(f"ERROR: Failed to create DataFrame for {key}: {e}")
                import traceback
                traceback.print_exc()

    print(f"DEBUG: Created {len(dataframes)} dataframes from inserts")
    return dataframes


def organize_by_schema(dataframes):
    """
    Organize DataFrames by schema name, preserving actual column names
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
        print(f"DEBUG: Organizing table {table} in schema {schema} with columns: {list(cleaned_df.columns)}")

        organized[schema][table] = cleaned_df

    return organized