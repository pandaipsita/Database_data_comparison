import os
import sys
import yaml

# Add the current directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from parsers.docx_data_parser import extract_insert_statements, inserts_to_dataframe
from database.chroma_store import store_data
from utils.chunk_utils import chunk_data


def main():
    # Load configuration
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    data_dir = config.get("data_directory", "./data_validation")
    files = sorted([f for f in os.listdir(data_dir) if f.endswith(".docx")])

    all_inserts = []

    for file in files:
        file_path = os.path.join(data_dir, file)
        print(f"📄 Parsing file: {file_path}")

        # Extract INSERT statements
        inserts = extract_insert_statements(file_path)
        all_inserts.extend(inserts)

    print(f"🧠 Total rows extracted from INSERT statements: {len(all_inserts)}")

    # Convert INSERT statements to DataFrames
    data_dataframes = inserts_to_dataframe(all_inserts)
    print(f"📊 Created DataFrames for {len(data_dataframes)} tables")

    # Print summary of data loaded
    for key, df in data_dataframes.items():
        print(f"  - {key}: {len(df)} rows")

    # Generate and count data chunks
    data_chunks = chunk_data(data_dataframes)

    # Store data in ChromaDB
    store_data(data_chunks, config)
    print(f"✅ Data loaded and stored successfully")


if __name__ == "__main__":
    main()