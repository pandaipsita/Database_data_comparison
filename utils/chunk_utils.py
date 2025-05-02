def chunk_data(dataframes):
    """
    Process dataframes into chunks with metadata for vector storage

    Args:
        dataframes: Dictionary of DataFrames keyed by schema.table

    Returns:
        list: List of dictionaries with content and metadata
    """
    chunks = []

    for key, df in dataframes.items():
        schema = df["__schema__"].iloc[0]
        table = df["__table__"].iloc[0]

        # Process each row
        for idx, row in df.iterrows():
            # Convert row to string representation (exclude metadata columns)
            row_data = row.drop(["__schema__", "__table__"], errors='ignore')
            row_content = row_data.to_json()

            chunks.append({
                "content": row_content,
                "metadata": {
                    "schema": schema,
                    "table": table,
                    "row_id": idx
                }
            })

    # Count chunks per schema
    schema_counts = {}
    for chunk in chunks:
        schema_name = chunk["metadata"]["schema"]
        schema_counts[schema_name] = schema_counts.get(schema_name, 0) + 1

    print("Data chunk counts by schema:")
    for schema, count in schema_counts.items():
        print(f"  {schema}: {count} chunks")

    return chunks