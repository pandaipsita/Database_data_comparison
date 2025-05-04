import os
import sys
import yaml
import json
import datetime
import great_expectations as ge

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Now import modules using absolute imports
from parsers.docx_data_parser import extract_insert_statements, inserts_to_dataframe, organize_by_schema
from utils.data_retriver import get_common_tables
from utils.chunk_utils import chunk_data
from database.chroma_store import store_data
from validators.data_comparator import generate_data_comparison_report
from validators.ge_validator import compare_data_with_ge
from langchain_ollama import OllamaLLM
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain


def main():
    print("Starting report generation process...")

    # Load configuration
    print("Loading configuration from config.yaml...")
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
        print("✅ Configuration loaded successfully")
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        return

    # Create output directories
    print("Creating validation_reports directory if it doesn't exist...")
    try:
        os.makedirs("validation_reports", exist_ok=True)
        print("✅ Directory structure verified")
    except Exception as e:
        print(f"❌ Error creating directories: {e}")
        return

    # Initialize Great Expectations context
    try:
        print("Initializing Great Expectations context...")
        context = ge.data_context.DataContext(config.get("ge_dir", "./great_expectations"))
        print("✅ Great Expectations context loaded")
        use_ge = True
    except Exception as e:
        print(f"⚠️ Could not load Great Expectations context: {e}")
        print("Continuing without Great Expectations...")
        use_ge = False

    # Step 1: Load data from docx files
    print("\nStep 1: Loading data from Word documents...")
    data_dir = config["data_directory"]

    try:
        files = [f for f in os.listdir(data_dir) if f.endswith(".docx")]
        print(f"Found {len(files)} Word documents in {data_dir}: {files}")
    except Exception as e:
        print(f"❌ Error accessing data directory {data_dir}: {e}")
        return

    all_inserts = []

    for file in files:
        file_path = os.path.join(data_dir, file)
        print(f"📄 Parsing file: {file_path}")

        try:
            # Extract INSERT statements
            inserts = extract_insert_statements(file_path)
            all_inserts.extend(inserts)
        except Exception as e:
            print(f"❌ Error parsing file {file_path}: {e}")
            continue

    print(f"🧠 Total rows extracted from INSERT statements: {len(all_inserts)}")

    if len(all_inserts) == 0:
        print("❌ No data extracted from documents. Cannot proceed.")
        return

    # Process INSERT statements to DataFrames
    print("\nStep 2: Converting data to DataFrames...")
    try:
        all_dataframes = inserts_to_dataframe(all_inserts)
        print(f"📊 Created DataFrames for {len(all_dataframes)} tables")

        # Print summary of data loaded
        for key, df in all_dataframes.items():
            print(f"  - {key}: {len(df)} rows, columns: {list(df.columns)}")
    except Exception as e:
        print(f"❌ Error converting to DataFrames: {e}")
        import traceback
        traceback.print_exc()
        return

    # Organize data by schema
    print("\nStep 3: Organizing data by schema...")
    try:
        schema_data = organize_by_schema(all_dataframes)
        print(f"Found {len(schema_data)} schemas: {list(schema_data.keys())}")
    except Exception as e:
        print(f"❌ Error organizing data by schema: {e}")
        return

    # Load schema names from config
    schema_names = config.get("schemas", [])
    print(f"Schemas specified in config: {schema_names}")

    if len(schema_names) != 2:
        print(f"⚠️ Missing one or both of the required schemas in config.yaml")
        available_schemas = list(schema_data.keys())
        if len(available_schemas) >= 2:
            print(f"Using available schemas instead: {available_schemas[0]}, {available_schemas[1]}")
            schema_names = available_schemas[:2]
        else:
            print(f"❌ Need at least two schemas to generate comparison report")
            return

    schema1, schema2 = schema_names
    print(f"Comparing data between schemas: {schema1} (source) and {schema2} (destination)")

    # Step 4: Generate data chunks and store in vector database
    print("\nStep 4: Storing data in vector database...")
    try:
        data_chunks = chunk_data(all_dataframes)
        store_data(data_chunks, config)
        print("✅ Data stored in vector database")
    except Exception as e:
        print(f"⚠️ Error storing data: {e}")
        print("Continuing without vector storage...")

    # Step 5: Generate data comparison report
    print("\nStep 5: Generating data comparison report...")
    if schema1 in schema_data and schema2 in schema_data:
        schema1_data = schema_data[schema1]
        schema2_data = schema_data[schema2]

        # Get common tables
        try:
            print("Finding common tables between schemas...")
            common_tables = get_common_tables(schema1_data, schema2_data)
        except Exception as e:
            print(f"⚠️ Error using get_common_tables function: {e}")
            # Fallback implementation if the function isn't available
            common_tables = set(schema1_data.keys()) & set(schema2_data.keys())

        # Find tables that exist in only one schema
        schema1_only_tables = set(schema1_data.keys()) - set(schema2_data.keys())
        schema2_only_tables = set(schema2_data.keys()) - set(schema1_data.keys())
        mismatched_tables = schema1_only_tables.union(schema2_only_tables)

        print(f"Found {len(common_tables)} common tables to compare: {common_tables}")
        print(f"Found {len(mismatched_tables)} mismatched tables: {mismatched_tables}")

        # Specific check for TransactionHistory tables
        transaction_tables = [table for table in mismatched_tables if "TransactionHistory" in table]
        if transaction_tables:
            print(f"Note: The following transaction history tables have different names: {transaction_tables}")
            print("This may indicate a naming convention difference rather than a structural difference.")

        if len(common_tables) == 0:
            print("❌ No common tables found between schemas. Cannot generate comparison report.")
            return

        # Generate data comparison report
        print("Comparing data between schemas...")
        try:
            data_comparison = generate_data_comparison_report(
                schema1_data,
                schema2_data,
                schema1,
                schema2
            )

            # Add information about mismatched tables to the report
            data_comparison["mismatched_tables"] = {
                f"{schema1}_only": list(schema1_only_tables),
                f"{schema2}_only": list(schema2_only_tables)
            }

            print("✅ Data comparison completed")

            # Debugging: log a sample of the report structure
            if "table_comparisons" in data_comparison:
                sample_table_name = next(iter(data_comparison["table_comparisons"].keys()))
                sample_table = data_comparison["table_comparisons"][sample_table_name]
                if "details" in sample_table and "matching_rows" in sample_table["details"] and sample_table["details"][
                    "matching_rows"]:
                    print(f"\nSample row structure from {sample_table_name}:")
                    print(sample_table["details"]["matching_rows"][0])
        except Exception as e:
            print(f"❌ Error generating comparison report: {e}")
            import traceback
            traceback.print_exc()
            return

        # Optional: Use Great Expectations for additional validation
        if use_ge:
            print("\nPerforming Great Expectations validations...")
            ge_results = {}

            for table in common_tables:
                try:
                    df1 = schema1_data[table]
                    df2 = schema2_data[table]

                    result = compare_data_with_ge(df1, df2, table, context)
                    ge_results[table] = {
                        "success": result.success,
                        "statistics": result.statistics,
                        "results": [r.to_json_dict() for r in result.results]
                    }
                    print(f"✅ GE validation for table {table} completed")
                except Exception as e:
                    print(f"⚠️ Error in GE validation for table {table}: {e}")

            # Add GE results to the comparison report
            data_comparison["great_expectations"] = ge_results

        # Save data comparison report
        print("\nStep 6: Saving comparison report...")
        report_path = os.path.join("validation_reports", "validation_report.json")
        print(f"Attempting to save report to {report_path}")
        try:
            with open(report_path, "w") as f:
                json.dump(data_comparison, f, indent=2)
            print(f"✅ Data comparison report saved to {report_path}")
        except Exception as e:
            print(f"❌ Error saving comparison report: {e}")
            # Try with a different path as a fallback
            alt_path = "validation_report.json"
            print(f"Trying to save to {alt_path} instead...")
            try:
                with open(alt_path, "w") as f:
                    json.dump(data_comparison, f, indent=2)
                print(f"✅ Data comparison report saved to {alt_path}")
                report_path = alt_path
            except Exception as e2:
                print(f"❌ Error saving to alternate path: {e2}")
                print("Unable to save report to disk. Continuing with in-memory report...")

        # Step 7: Generate natural language summary with LLM
        print("\nStep 7: Generating natural language summary...")
        try:
            # First check if Ollama is available
            print("Initializing LLaMA model...")
            llm = OllamaLLM(model="llama3")

            print("Creating summary prompt...")
            prompt = PromptTemplate(
                template="""
                You are a database expert. Analyze this data comparison report and provide a concise summary:

                {report}

                Provide a clear summary of:
                1. Overall statistics - how many rows were compared, matched, differed
                2. Key differences by table - highlight the most significant variations
                3. Tables that exist in only one schema but not the other
                4. Potential data quality issues or inconsistencies
                5. Recommendations for data migration or alignment

                Format your response as a structured report with clear headings and bullet points.
                """,
                input_variables=["report"]
            )

            print("Running LLM chain to generate summary...")
            chain = LLMChain(llm=llm, prompt=prompt)
            result = chain.run(report=json.dumps(data_comparison, indent=2))

            # Save the summary
            summary_path = os.path.join("validation_reports", "validation_report.txt")
            print(f"Attempting to save summary to {summary_path}")

            try:
                with open(summary_path, "w") as f:
                    f.write(result)
                print(f"✅ Natural language summary saved to {summary_path}")
            except Exception as e:
                print(f"❌ Error saving summary: {e}")
                # Try alternative path
                alt_summary_path = "validation_report_summary.txt"
                try:
                    with open(alt_summary_path, "w") as f:
                        f.write(result)
                    print(f"✅ Summary saved to alternative path: {alt_summary_path}")
                except Exception as e2:
                    print(f"❌ Error saving to alternate path: {e2}")

        except Exception as e:
            print(f"⚠️ Error generating summary: {e}")

        # Step 8: Print overall statistics
        print("\nStep 8: Displaying comparison summary...")
        try:
            summary = data_comparison["summary"]
            print("\n=== Data Comparison Summary ===")
            print(f"Tables compared: {summary['total_tables']}")
            print(f"Tables in {schema1} only: {len(schema1_only_tables)}")
            print(f"Tables in {schema2} only: {len(schema2_only_tables)}")
            print(f"Total rows in source ({schema1}): {summary['total_rows_source']}")
            print(f"Total rows in destination ({schema2}): {summary['total_rows_destination']}")
            print(f"Matching rows: {summary['total_matching_rows']}")
            print(f"Rows with differences: {summary['total_different_rows']}")
            print(f"Rows missing in destination: {summary['total_missing_rows']}")
            print(f"Extra rows in destination: {summary['total_extra_rows']}")

            if summary["all_matched"] and len(mismatched_tables) == 0:
                print("\n✅ All data matches perfectly between the two schemas!")
            elif summary["all_matched"]:
                print("\n⚠️ All common tables match, but some tables exist in only one schema")
            else:
                print("\n⚠️ Data differences detected between the schemas")
        except Exception as e:
            print(f"❌ Error displaying summary: {e}")

        # Step 9: Optional - Ask for additional analysis
        print("\nStep 9: Interactive Q&A...")
        answer = input("\nWould you like to ask questions about the data comparison? (y/n): ")
        if answer.lower() == "y":
            print("\nDatabase Expert is ready! Ask questions about the data comparison, or type 'exit' to quit.")

            while True:
                question = input("\nQuestion: ")
                if question.lower() in ["exit", "quit"]:
                    break

                try:
                    llm = OllamaLLM(model="llama3", temperature=0.1)
                    prompt = PromptTemplate(
                        template="""
                        You are a database expert with access to the following data comparison report:

                        {report}

                        Answer the following question based only on the information in the report:
                        {question}

                        Be specific, clear, and concise in your answer.
                        """,
                        input_variables=["report", "question"]
                    )

                    chain = LLMChain(llm=llm, prompt=prompt)
                    result = chain.run(
                        report=json.dumps(data_comparison, indent=2),
                        question=question
                    )

                    print("\nAnswer:", result)
                except Exception as e:
                    print(f"⚠️ Error answering question: {e}")
    else:
        print(f"❌ One or both schemas not found in the data. Available schemas: {list(schema_data.keys())}")

    print("\nReport generation process completed.")


if __name__ == "__main__":
    main()