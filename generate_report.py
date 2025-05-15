

import glob
import os
import re
import sys

import yaml
import json
import datetime
import great_expectations as ge
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

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


# Helper function to extract inserts from a single file (for parallel processing)
def process_single_file(file_path):
    try:
        print(f"📄 Processing file: {file_path}")
        file_extension = file_path.lower().split('.')[-1]
        print(f"  File type: {file_extension}")

        from parsers.docx_data_parser import extract_insert_statements
        results = extract_insert_statements(file_path)

        print(f"  Extracted {len(results)} INSERT statements")
        if results and len(results) > 0:
            # Show sample of first result for debugging
            first = results[0]
            print(f"  Sample - Table: {first.get('table_name')}, Columns: {first.get('columns', [])[:3]}...")

        return results
    except Exception as e:
        print(f"❌ Error processing file {file_path}: {e}")
        import traceback
        traceback.print_exc()
        return []


# Helper function for parallel Great Expectations validation
def validate_table_with_ge(args):
    table, df1, df2, context = args
    try:
        result = compare_data_with_ge(df1, df2, table, context)
        return table, {
            "success": result.success,
            "statistics": result.statistics,
            "results": [r.to_json_dict() for r in result.results]
        }
    except Exception as e:
        print(f"⚠️ Error in GE validation for table {table}: {e}")
        return table, {"error": str(e)}


# Function to generate HTML report from comparison data
def generate_html_report(data_comparison):
    """
    Generate an HTML report from the data comparison results
    """
    print("\nGenerating HTML report...")
    # Extract report ID from meta data
    report_id = data_comparison.get("meta", {}).get("report_id",
                                                    datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))

    # Save the HTML report with a consistent naming pattern
    html_path = os.path.join("validation_reports", f"validation_report_{report_id}.html")

    # Extract necessary data
    meta = data_comparison.get("meta", {})
    summary = data_comparison.get("summary", {})
    table_comparisons = data_comparison.get("table_comparisons", {})
    mismatched_tables = data_comparison.get("mismatched_tables", {})

    # Calculate match percentage
    match_percentage = 0
    if summary.get("total_rows_source", 0) > 0:
        match_percentage = round((summary.get("total_matching_rows", 0) / summary.get("total_rows_source", 0)) * 100, 1)

    # Format current timestamp
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")

    # Create HTML content - the HTML template (same as in your original code)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Database/Data Validation Report</title>

    <style>
        :root {{
            --primary-color: #3498db;
            --secondary-color: #2c3e50;
            --success-color: #2ecc71;
            --warning-color: #f39c12;
            --danger-color: #e74c3c;
            --info-color: #3498db;
            --light-color: #ecf0f1;
            --dark-color: #2c3e50;
            --border-color: #bdc3c7;
        }}

        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: var(--dark-color);
            background-color: #f5f8fa;
            padding: 20px;
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: #ffffff;
            border-radius: 8px;
            box-shadow: 0 2px 15px rgba(0, 0, 0, 0.1);
            overflow: hidden;
        }}

        header {{
            background-color: var(--secondary-color);
            color: white;
            padding: 25px;
            text-align: center;
        }}

        header h1 {{
            margin-bottom: 10px;
            font-weight: 600;
        }}

        .meta-info {{
            background-color: var(--light-color);
            padding: 15px 25px;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
        }}

        .meta-info div {{
            margin: 5px 15px 5px 0;
        }}

        .meta-info p {{
            margin-bottom: 5px;
        }}

        .summary {{
            padding: 25px;
            border-bottom: 1px solid var(--border-color);
        }}

        .summary h2 {{
            margin-bottom: 20px;
            color: var(--secondary-color);
            font-weight: 600;
        }}

        .stats {{
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
        }}

        .stat-card {{
            flex: 1;
            min-width: 200px;
            padding: 20px;
            border-radius: 8px;
            background-color: white;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
            text-align: center;
            transition: transform 0.2s ease;
        }}

        .stat-card:hover {{
            transform: translateY(-5px);
        }}

        .stat-card h3 {{
            font-size: 2.2rem;
            margin-bottom: 8px;
            color: var(--primary-color);
        }}

        .stat-card p {{
            color: var(--dark-color);
            font-size: 1rem;
        }}

        .stat-card.error-stat h3 {{
            color: var(--danger-color);
        }}

        .stat-card.success-stat h3 {{
            color: var(--success-color);
        }}

        .tables {{
            padding: 25px;
        }}

        .tables h2 {{
            margin-bottom: 20px;
            color: var(--secondary-color);
            font-weight: 600;
        }}

        .accordion {{
            margin-bottom: 15px;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            overflow: hidden;
            transition: box-shadow 0.3s ease;
        }}

        .accordion:hover {{
            box-shadow: 0 4px 10px rgba(0, 0, 0, 0.1);
        }}

        .accordion-header {{
            background-color: var(--light-color);
            padding: 15px 20px;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        .accordion-header h3 {{
            margin: 0;
            font-size: 1.2rem;
            color: var(--secondary-color);
        }}

        .accordion-content {{
            padding: 0;
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-out, padding 0.3s ease;
            background-color: white;
        }}

        .accordion-content.active {{
            max-height: 2000px;
            padding: 20px;
            border-top: 1px solid var(--border-color);
        }}

        .table-summary {{
            margin-bottom: 20px;
            padding: 10px 15px;
            background-color: var(--light-color);
            border-radius: 6px;
        }}

        .badges {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
        }}

        .badge {{
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: 500;
            display: inline-block;
        }}

        .badge-success {{
            background-color: var(--success-color);
            color: white;
        }}

        .badge-warning {{
            background-color: var(--warning-color);
            color: white;
        }}

        .badge-danger {{
            background-color: var(--danger-color);
            color: white;
        }}

        .badge-info {{
            background-color: var(--info-color);
            color: white;
        }}

        .diff-table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
            font-size: 0.95rem;
        }}

        .diff-table th {{
            background-color: var(--light-color);
            padding: 12px 15px;
            text-align: left;
            border-bottom: 2px solid var(--border-color);
            position: sticky;
            top: 0;
        }}

        .diff-table td {{
            padding: 12px 15px;
            border-bottom: 1px solid var(--border-color);
        }}

        .diff-table tr:hover {{
            background-color: rgba(236, 240, 241, 0.5);
        }}

        .diff-table .no-differences {{
            text-align: center;
            padding: 20px;
            color: var(--dark-color);
        }}

        .diff-highlight {{
            background-color: #ffecb3;
            padding: 3px 6px;
            border-radius: 4px;
            font-weight: 500;
        }}

        .mismatched-tables {{
            margin-top: 30px;
            padding: 20px;
            background-color: var(--light-color);
            border-radius: 8px;
        }}

        .mismatched-tables h3 {{
            margin-bottom: 15px;
            color: var(--secondary-color);
        }}

        .mismatched-tables ul {{
            list-style-type: none;
            margin-left: 20px;
            margin-bottom: 20px;
        }}

        .mismatched-tables li {{
            padding: 8px 0;
            border-bottom: 1px solid rgba(189, 195, 199, 0.5);
        }}

        .mismatched-tables li:last-child {{
            border-bottom: none;
        }}

        .schema-status {{
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            margin-top: 25px;
        }}

        .schema-card {{
            flex: 1;
            min-width: 300px;
            padding: 20px;
            border-radius: 8px;
            background-color: white;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        }}

        .schema-card h3 {{
            margin-bottom: 15px;
            color: var(--primary-color);
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 10px;
        }}

        .progress-container {{
            margin-top: 25px;
        }}

        .progress-bar {{
            height: 20px;
            background-color: #ecf0f1;
            border-radius: 10px;
            margin-bottom: 15px;
            overflow: hidden;
        }}

        .progress {{
            height: 100%;
            background-color: var(--primary-color);
            border-radius: 10px;
        }}

        .progress-label {{
            display: flex;
            justify-content: space-between;
            font-size: 0.9rem;
            color: var(--dark-color);
        }}

        footer {{
            text-align: center;
            padding: 20px;
            color: var(--dark-color);
            font-size: 0.9rem;
            border-top: 1px solid var(--border-color);
            background-color: var(--light-color);
        }}

        .chart-container {{
            display: flex;
            justify-content: space-between;
            margin-top: 25px;
            flex-wrap: wrap;
            gap: 20px;
        }}

        .chart {{
            flex: 1;
            min-width: 300px;
            height: 300px;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        }}

        @media (max-width: 768px) {{
            .stats, .schema-status, .chart-container {{
                flex-direction: column;
            }}

            .stat-card, .schema-card, .chart {{
                width: 100%;
            }}

            .meta-info {{
                flex-direction: column;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Database/Data Validation Report</h1>
            <p>Comparing data between {meta.get('source_schema', 'source')} and {meta.get('destination_schema', 'destination')} database schemas</p>
        </header>

        <div class="meta-info">
            <div>
                <p><strong>Source Schema:</strong> {meta.get('source_schema', 'N/A')}</p>
                <p><strong>Destination Schema:</strong> {meta.get('destination_schema', 'N/A')}</p>
            </div>
            <div>
                <p><strong>Timestamp:</strong> {timestamp}</p>
                <p><strong>Tables Compared:</strong> {meta.get('tables_compared', summary.get('total_tables', 0))}</p>
            </div>
        </div>

        <div class="summary">
            <h2>Overall Summary</h2>
            <div class="stats">
                <div class="stat-card">
                    <h3>{summary.get('total_tables', 0)}</h3>
                    <p>Tables Compared</p>
                </div>
                <div class="stat-card">
                    <h3>{summary.get('total_rows_source', 0)}</h3>
                    <p>Total Source Rows</p>
                </div>
                <div class="stat-card">
                    <h3>{summary.get('total_rows_destination', 0)}</h3>
                    <p>Total Destination Rows</p>
                </div>
                <div class="stat-card success-stat">
                    <h3>{summary.get('total_matching_rows', 0)}</h3>
                    <p>Matching Rows</p>
                </div>
            </div>
            <div class="stats">
                <div class="stat-card error-stat">
                    <h3>{summary.get('total_different_rows', 0)}</h3>
                    <p>Different Rows</p>
                </div>
                <div class="stat-card error-stat">
                    <h3>{summary.get('total_missing_rows', 0)}</h3>
                    <p>Missing Rows</p>
                </div>
                <div class="stat-card error-stat">
                    <h3>{summary.get('total_extra_rows', 0)}</h3>
                    <p>Extra Rows</p>
                </div>
            </div>

            <div class="progress-container">
                <h3>Data Match Progress</h3>
                <div class="progress-bar">
                    <div class="progress" style="width: {match_percentage}%;"></div>
                </div>
                <div class="progress-label">
                    <span>0%</span>
                    <span>{match_percentage}% Matched</span>
                    <span>100%</span>
                </div>
            </div>
        </div>

        <div class="tables">
            <h2>Table Comparisons</h2>
"""

    # Add table comparisons
    for table_name, table_data in table_comparisons.items():
        table_summary = table_data.get('summary', {})
        rows_matching = table_summary.get('matching_rows', 0)
        rows_different = table_summary.get('different_rows', 0)
        rows_missing = table_summary.get('missing_rows', 0)

        html += f"""
            <div class="accordion">
                <div class="accordion-header">
                    <h3>{table_name.capitalize()}</h3>
                    <div class="badges">
                        <span class="badge badge-success">{rows_matching} Matching</span>
                        <span class="badge badge-danger">{rows_different} Different</span>
                        <span class="badge badge-warning">{rows_missing} Missing</span>
                    </div>
                </div>
                <div class="accordion-content">
                    <div class="table-summary">
                        <p><strong>Summary:</strong> {table_summary.get('rows_in_source', 0)} rows in source, {table_summary.get('rows_in_destination', 0)} rows in destination, {rows_matching} matching, {rows_different} with differences, {rows_missing} missing</p>
                    </div>
        """

        # Add differences section if there are differences
        if rows_different > 0 and 'details' in table_data and 'different_rows' in table_data['details']:
            html += """
                    <h4>Differences</h4>
                    <table class="diff-table">
                        <thead>
                            <tr>
            """

            # Get the ID field name (try to find something like payroll_id, contact_id, etc.)
            id_field = None
            if table_data['details']['different_rows']:
                source_row = table_data['details']['different_rows'][0]['source_row']
                for key in source_row.keys():
                    if key.endswith('_id') and key != 'employee_id' and key != 'contractor_id':
                        id_field = key
                        break

                # If no specific ID found, use the first field
                if id_field is None:
                    id_field = list(source_row.keys())[0]

                # Create header row
                html += f"""
                                <th>{id_field.replace('_', ' ').title()}</th>
                                <th>Field</th>
                                <th>Source Value</th>
                                <th>Destination Value</th>
                            </tr>
                        </thead>
                        <tbody>
                """

                # Add difference rows
                for diff_row in table_data['details']['different_rows']:
                    source_row = diff_row.get('source_row', {})
                    differences = diff_row.get('differences', {})

                    row_id = source_row.get(id_field, 'N/A')

                    for field, values in differences.items():
                        html += f"""
                            <tr>
                                <td>{row_id}</td>
                                <td>{field}</td>
                                <td>{values.get('source', 'N/A')}</td>
                                <td class="diff-highlight">{values.get('destination', 'N/A')}</td>
                            </tr>
                        """

                html += """
                        </tbody>
                    </table>
                """

        # Add missing rows section if there are missing rows
        if rows_missing > 0 and 'details' in table_data and 'missing_rows' in table_data['details'] and \
                table_data['details']['missing_rows']:
            html += """
                    <h4>Missing Rows</h4>
                    <table class="diff-table">
                        <thead>
                            <tr>
            """

            # Get field names from the first missing row
            if table_data['details']['missing_rows']:
                field_names = list(table_data['details']['missing_rows'][0].keys())

                # Add table headers
                for field in field_names:
                    html += f"""
                                <th>{field.replace('_', ' ').title()}</th>
                    """

                html += """
                            </tr>
                        </thead>
                        <tbody>
                """

                # Add missing rows
                for missing_row in table_data['details']['missing_rows']:
                    html += """
                            <tr>
                    """

                    for field in field_names:
                        html += f"""
                                <td>{missing_row.get(field, 'N/A')}</td>
                        """

                    html += """
                            </tr>
                    """

                html += """
                        </tbody>
                    </table>
                """

        html += """
                </div>
            </div>
        """

    # Add mismatched tables section
    source_schema = meta.get('source_schema', 'source')
    dest_schema = meta.get('destination_schema', 'destination')

    html += """
        </div>

        <div class="mismatched-tables">
            <h3>Tables that exist in only one schema</h3>
            <div class="schema-status">
    """

    # Add tables that exist only in source schema
    source_only_key = f"{source_schema}_only"
    if source_only_key in mismatched_tables:
        source_only_tables = mismatched_tables[source_only_key]

        html += f"""
                <div class="schema-card">
                    <h3>Tables in {source_schema} Only</h3>
                    <ul>
        """

        for table in source_only_tables:
            html += f"""
                        <li>{table}</li>
            """

        html += """
                    </ul>
                </div>
        """

    # Add tables that exist only in destination schema
    dest_only_key = f"{dest_schema}_only"
    if dest_only_key in mismatched_tables:
        dest_only_tables = mismatched_tables[dest_only_key]

        html += f"""
                <div class="schema-card">
                    <h3>Tables in {dest_schema} Only</h3>
                    <ul>
        """

        for table in dest_only_tables:
            html += f"""
                        <li>{table}</li>
            """

        html += """
                    </ul>
                </div>
        """

    html += """
            </div>
        </div>

        <footer>
            <p>Generated on """ + datetime.datetime.now().strftime("%B %d, %Y") + """ | Database/Data Validation Report</p>
        </footer>
    </div>

    <script>
        // JavaScript to toggle accordion
        document.addEventListener('DOMContentLoaded', function() {
            const accordions = document.querySelectorAll('.accordion-header');

            accordions.forEach(accordion => {
                accordion.addEventListener('click', function() {
                    const content = this.nextElementSibling;

                    // Close all other accordion contents
                    document.querySelectorAll('.accordion-content').forEach(item => {
                        if (item !== content) {
                            item.classList.remove('active');
                        }
                    });

                    // Toggle current accordion content
                    content.classList.toggle('active');
                });
            });
        });
    </script>
</body>
</html>
    """

    # Create a report ID based on timestamp
    report_id = meta.get('report_id', datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))

    # Save the HTML report
    try:
        # Make sure validation_reports directory exists
        os.makedirs("validation_reports", exist_ok=True)

        # Save the report with a unique ID
        report_path = os.path.join("validation_reports", f"validation_report_{report_id}.html")
        with open(report_path, "w") as f:
            f.write(html)
        print(f"✅ HTML report saved to {report_path}")
        return report_path, report_id
    except Exception as e:
        print(f"❌ Error saving HTML report: {e}")
        # Try alternative path
        try:
            alt_path = f"validation_report_{report_id}.html"
            with open(alt_path, "w") as f:
                f.write(html)
            print(f"✅ HTML report saved to alternative path: {alt_path}")
            return alt_path, report_id
        except Exception as e2:
            print(f"❌ Error saving to alternate path: {e2}")
            return None, None


def get_common_table_list(schema1, schema2, config):
    """
    Get list of common tables between schemas by discovering tables in all supported files.
    """
    try:
        # Check if schema_paths is defined in config
        schema_paths = config.get("schema_paths", {})

        # Get schema directories from schema_paths or fall back to data_dir + schema
        if schema1 in schema_paths:
            schema1_dir = schema_paths[schema1]
        else:
            # Fall back to original logic
            data_dir = config.get("data_directory", "")
            if data_dir.startswith("./"):
                project_dir = os.path.dirname(os.path.abspath(__file__))
                data_dir = os.path.join(project_dir, data_dir[2:])
            schema1_dir = os.path.join(data_dir, schema1)

        if schema2 in schema_paths:
            schema2_dir = schema_paths[schema2]
        else:
            # Fall back to original logic
            data_dir = config.get("data_directory", "")
            if data_dir.startswith("./"):
                project_dir = os.path.dirname(os.path.abspath(__file__))
                data_dir = os.path.join(project_dir, data_dir[2:])
            schema2_dir = os.path.join(data_dir, schema2)

        print(f"Looking for files in: {schema1_dir} and {schema2_dir}")

        # Import parser
        from parsers.docx_data_parser import extract_insert_statements

        # Find tables in schema1
        schema1_tables = set()
        if os.path.exists(schema1_dir) and os.path.isdir(schema1_dir):
            # Find all supported files in schema directory - UPDATED TO INCLUDE SQL AND TXT
            supported_files = []
            for ext in ['docx', 'sql', 'txt']:
                supported_files.extend(glob.glob(os.path.join(schema1_dir, f"**/*.{ext}"), recursive=True))

            print(f"Found {len(supported_files)} supported files in {schema1_dir}")

            for file_path in supported_files:
                try:
                    print(f"Processing {file_path}")
                    inserts = extract_insert_statements(file_path)
                    for insert in inserts:
                        # Extract table name from the dictionary
                        table_name = insert.get("table_name")
                        if table_name:
                            schema1_tables.add(table_name)
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

        # Find tables in schema2
        schema2_tables = set()
        if os.path.exists(schema2_dir) and os.path.isdir(schema2_dir):
            # Find all supported files in schema directory - UPDATED TO INCLUDE SQL AND TXT
            supported_files = []
            for ext in ['docx', 'sql', 'txt']:
                supported_files.extend(glob.glob(os.path.join(schema2_dir, f"**/*.{ext}"), recursive=True))

            print(f"Found {len(supported_files)} supported files in {schema2_dir}")

            for file_path in supported_files:
                try:
                    print(f"Processing {file_path}")
                    inserts = extract_insert_statements(file_path)
                    for insert in inserts:
                        # Extract table name from the dictionary
                        table_name = insert.get("table_name")
                        if table_name:
                            schema2_tables.add(table_name)
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

        print(f"Tables found in {schema1}: {schema1_tables}")
        print(f"Tables found in {schema2}: {schema2_tables}")

        # Find common tables
        common_tables = schema1_tables.intersection(schema2_tables)

        if common_tables:
            print(f"✅ Discovered {len(common_tables)} common tables: {common_tables}")
            return list(common_tables)
        else:
            print(f"⚠️ No common tables found between {schema1} and {schema2}")
            return []  # Return empty list instead of defaults
    except Exception as e:
        print(f"⚠️ Error discovering common tables: {e}")
        import traceback
        traceback.print_exc()
        return [] # Return empty list on error


def get_schema_only_tables(schema1, schema2, config):
    """
    Get tables that exist only in schema1 but not in schema2.
    """
    try:
        # Check if schema_paths is defined in config
        schema_paths = config.get("schema_paths", {})

        # Get schema directories from schema_paths or fall back to data_dir + schema
        if schema1 in schema_paths:
            schema1_dir = schema_paths[schema1]
        else:
            # Fall back to original logic
            data_dir = config.get("data_directory", "")
            if data_dir.startswith("./"):
                project_dir = os.path.dirname(os.path.abspath(__file__))
                data_dir = os.path.join(project_dir, data_dir[2:])
            schema1_dir = os.path.join(data_dir, schema1)

        if schema2 in schema_paths:
            schema2_dir = schema_paths[schema2]
        else:
            # Fall back to original logic
            data_dir = config.get("data_directory", "")
            if data_dir.startswith("./"):
                project_dir = os.path.dirname(os.path.abspath(__file__))
                data_dir = os.path.join(project_dir, data_dir[2:])
            schema2_dir = os.path.join(data_dir, schema2)

        print(f"Looking for schema1 ({schema1}) files in: {schema1_dir}")
        print(f"Looking for schema2 ({schema2}) files in: {schema2_dir}")

        # Import parser
        from parsers.docx_data_parser import extract_insert_statements

        # Find tables in schema1
        schema1_tables = set()
        if os.path.exists(schema1_dir) and os.path.isdir(schema1_dir):
            # Find all supported files in schema directory - UPDATED TO INCLUDE SQL AND TXT
            supported_files = []
            for ext in ['docx', 'sql', 'txt']:
                supported_files.extend(glob.glob(os.path.join(schema1_dir, f"**/*.{ext}"), recursive=True))

            print(f"Found {len(supported_files)} supported files in {schema1_dir}")

            for file_path in supported_files:
                try:
                    inserts = extract_insert_statements(file_path)
                    for insert in inserts:
                        # Extract table name from the dictionary
                        table_name = insert.get("table_name")
                        if table_name:
                            schema1_tables.add(table_name)
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

        # Find tables in schema2
        schema2_tables = set()
        if os.path.exists(schema2_dir) and os.path.isdir(schema2_dir):
            # Find all supported files in schema directory - UPDATED TO INCLUDE SQL AND TXT
            supported_files = []
            for ext in ['docx', 'sql', 'txt']:
                supported_files.extend(glob.glob(os.path.join(schema2_dir, f"**/*.{ext}"), recursive=True))

            print(f"Found {len(supported_files)} supported files in {schema2_dir}")

            for file_path in supported_files:
                try:
                    inserts = extract_insert_statements(file_path)
                    for insert in inserts:
                        # Extract table name from the dictionary
                        table_name = insert.get("table_name")
                        if table_name:
                            schema2_tables.add(table_name)
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

        print(f"Tables found in {schema1}: {schema1_tables}")
        print(f"Tables found in {schema2}: {schema2_tables}")

        # Find tables only in schema1
        only_in_schema1 = schema1_tables - schema2_tables

        if only_in_schema1:
            print(f"✅ Discovered {len(only_in_schema1)} tables only in {schema1}: {only_in_schema1}")
            return list(only_in_schema1)
        else:
            print(f"No tables found exclusively in {schema1}")
            return []
    except Exception as e:
        print(f"⚠️ Error discovering schema-only tables: {e}")
        import traceback
        traceback.print_exc()
        return []

def compare_table_in_chunks(schema1, schema2, table, chunk_size=1000, config=None):
            """
            Compare data between two schemas for a single table, processing in chunks
            to minimize memory usage.
            """
            print(f"\nComparing data for table {table} in chunks of {chunk_size} rows...")

            # Initialize counters with minimal memory footprint
            rows_in_source = 0
            rows_in_destination = 0
            matching_rows = 0
            different_rows = 0
            missing_rows = 0
            extra_rows = 0

            # Track differences (keep only a limited number for reporting to save memory)
            max_differences_to_track = min(100, config.get("max_differences", 100)) if config else 100
            different_rows_details = []
            missing_rows_details = []
            extra_rows_details = []
            matching_rows_details = []

            # Get actual data from the config's loaded data
            source_data = []
            dest_data = []

            try:
                # Import required modules
                from parsers.docx_data_parser import extract_insert_statements

                # If uploaded files are available in config, use them directly
                if config.get('source_files'):
                    print("Loading from uploaded source files...")
                    for file_path in config['source_files']:
                        print(f"Trying file: {file_path}")
                        try:
                            # Extract all INSERT statements from the file
                            all_inserts = extract_insert_statements(file_path)
                            print(f"Found {len(all_inserts)} total INSERT statements")

                            # Filter inserts for this specific table
                            for insert_dict in all_inserts:
                                if insert_dict.get("table_name") == table:
                                    # Each insert_dict contains one row
                                    columns = insert_dict.get("columns", [])
                                    values = insert_dict.get("values", [])

                                    # Convert to dictionary with column names as keys
                                    if len(values) == len(columns):
                                        row_dict = {}
                                        for i, col_name in enumerate(columns):
                                            row_dict[col_name] = values[i]
                                        source_data.append(row_dict)

                            print(f"Extracted {len(source_data)} rows for table {table} from source")
                            if source_data:
                                print(f"First row: {source_data[0]}")

                        except Exception as e:
                            print(f"Error loading from source file {file_path}: {e}")
                            import traceback
                            traceback.print_exc()

                if config.get('dest_files'):
                    print("Loading from uploaded destination files...")
                    for file_path in config['dest_files']:
                        print(f"Trying file: {file_path}")
                        try:
                            # Extract all INSERT statements from the file
                            all_inserts = extract_insert_statements(file_path)
                            print(f"Found {len(all_inserts)} total INSERT statements")

                            # Filter inserts for this specific table
                            for insert_dict in all_inserts:
                                if insert_dict.get("table_name") == table:
                                    # Each insert_dict contains one row
                                    columns = insert_dict.get("columns", [])
                                    values = insert_dict.get("values", [])

                                    # Convert to dictionary with column names as keys
                                    if len(values) == len(columns):
                                        row_dict = {}
                                        for i, col_name in enumerate(columns):
                                            row_dict[col_name] = values[i]
                                        dest_data.append(row_dict)

                            print(f"Extracted {len(dest_data)} rows for table {table} from destination")
                            if dest_data:
                                print(f"First row: {dest_data[0]}")

                        except Exception as e:
                            print(f"Error loading from dest file {file_path}: {e}")
                            import traceback
                            traceback.print_exc()

            except Exception as e:
                print(f"Error loading data for table {table}: {e}")
                import traceback
                traceback.print_exc()
                # Use empty lists as fallback
                source_data = []
                dest_data = []

            # Get the actual counts
            source_total = len(source_data)
            dest_total = len(dest_data)

            print(f"Processing table with {source_total} source rows and {dest_total} destination rows")

            # Process source data first to build index
            print("Building source data index...")
            source_index = {}  # Dict mapping primary key to row data
            source_keys = set()

            # Identify the primary key field dynamically
            primary_key = None
            source_id_field = None
            dest_id_field = None

            if source_data and dest_data:
                # Get the first row from each dataset
                first_source_row = source_data[0]
                first_dest_row = dest_data[0]

                # Find the primary key (looking for table-specific ID like asset_id, payroll_id, etc.)
                for key in first_source_row.keys():
                    if key.endswith('_id') and key not in ['employee_id', 'contractor_id', 'department_id']:
                        primary_key = key
                        break

                # If no specific table ID found, use the first field that ends with _id
                if primary_key is None:
                    for key in first_source_row.keys():
                        if key.endswith('_id'):
                            primary_key = key
                            break

                # Check for entity-specific ID fields
                if 'employee_id' in first_source_row:
                    source_id_field = 'employee_id'
                if 'contractor_id' in first_dest_row:
                    dest_id_field = 'contractor_id'

                print(f"Using primary key: {primary_key}")
                print(f"Source ID field: {source_id_field}")
                print(f"Destination ID field: {dest_id_field}")

            # Index source data - keep values exactly as they are
            for row in source_data:
                if primary_key and primary_key in row:
                    key_value = str(row[primary_key]).strip()  # Convert to string and strip whitespace
                    source_index[key_value] = row
                    source_keys.add(key_value)

            rows_in_source = len(source_data)

            # Now process destination data and compare
            print("Processing destination data and comparing...")
            dest_keys = set()

            for row in dest_data:
                if primary_key and primary_key in row:
                    key_value = str(row[primary_key]).strip()  # Convert to string and strip whitespace
                    dest_keys.add(key_value)

                    if key_value in source_index:
                        # Row exists in both source and destination
                        source_row = source_index[key_value]
                        row_differences = {}

                        # Debug for specific tables
                        if table == 'trainingrecords' and primary_key:
                            print(f"\nDebug - TrainingRecords comparison:")
                            print(f"Primary key field: {primary_key}")
                            print(f"Source row key: {key_value}")
                            print(f"Source row: {source_row}")
                            print(f"Dest row: {row}")

                        # Get all fields from both rows
                        all_fields = set(source_row.keys()).union(set(row.keys()))

                        for field in all_fields:
                            # Skip entity-specific ID fields since they're expected to be different
                            if field in ['employee_id', 'contractor_id']:
                                continue

                            # Get values from both rows
                            source_val = source_row.get(field)
                            dest_val = row.get(field)

                            # Normalize values - strip whitespace and convert to string
                            source_str = str(source_val).strip() if source_val is not None else None
                            dest_str = str(dest_val).strip() if dest_val is not None else None

                            # Debug output for date fields
                            if field == 'completed_date' and source_str and dest_str:
                                print(f"Debug - Comparing {field}: '{source_str}' vs '{dest_str}'")
                                print(f"  Source type: {type(source_val)}, Dest type: {type(dest_val)}")
                                print(f"  String lengths: {len(source_str)} vs {len(dest_str)}")
                                print(f"  Raw bytes: {repr(source_str)} vs {repr(dest_str)}")
                                print(f"  Equal? {source_str == dest_str}")

                                # Check for any non-visible characters
                                import unicodedata
                                source_normalized = unicodedata.normalize('NFKC', source_str)
                                dest_normalized = unicodedata.normalize('NFKC', dest_str)
                                print(f"  Normalized equal? {source_normalized == dest_normalized}")

                            # Direct comparison after normalization
                            if source_str != dest_str:
                                row_differences[field] = {
                                    "source": source_str,
                                    "destination": dest_str
                                }

                        if row_differences:
                            different_rows += 1
                            if len(different_rows_details) < max_differences_to_track:
                                # Create proper details with both source and destination rows
                                dest_row_clean = {}
                                for field, value in row.items():
                                    dest_row_clean[field] = str(value).strip() if value is not None else None

                                source_row_clean = {}
                                for field, value in source_row.items():
                                    source_row_clean[field] = str(value).strip() if value is not None else None

                                different_rows_details.append({
                                    "source_row": source_row_clean,
                                    "destination_row": dest_row_clean,
                                    "differences": row_differences
                                })
                        else:
                            matching_rows += 1
                            if len(matching_rows_details) < max_differences_to_track:
                                # Store matching row
                                matching_row = {}
                                for field, value in source_row.items():
                                    # Skip entity-specific ID field
                                    if field in ['employee_id', 'contractor_id']:
                                        continue
                                    matching_row[field] = str(value).strip() if value is not None else None
                                matching_rows_details.append(matching_row)
                    else:
                        # Row exists only in destination (extra)
                        extra_rows += 1
                        if len(extra_rows_details) < max_differences_to_track:
                            clean_row = {}
                            for field, value in row.items():
                                clean_row[field] = str(value).strip() if value is not None else None
                            extra_rows_details.append(clean_row)

            rows_in_destination = len(dest_data)

            # Identify missing rows (in source but not in destination)
            missing_keys = source_keys - dest_keys
            missing_rows = len(missing_keys)

            # Collect a sample of missing rows for reporting
            for key in list(missing_keys)[:max_differences_to_track]:
                if key in source_index:
                    source_row = source_index[key]
                    clean_row = {}
                    for field, value in source_row.items():
                        clean_row[field] = str(value).strip() if value is not None else None
                    missing_rows_details.append(clean_row)

            # Debug: Print detailed comparison for first few rows
            if source_data and dest_data:
                print(f"\nDebug - First source row: {source_data[0]}")
                print(f"Debug - First dest row: {dest_data[0]}")

                # Check primary key matching
                if primary_key:
                    source_key = str(source_data[0].get(primary_key)).strip()
                    for dest_row in dest_data:
                        dest_key = str(dest_row.get(primary_key)).strip()
                        if source_key == dest_key:
                            print(f"Found matching primary key: {source_key}")
                            break

            # Compute summary for this table
            has_differences = (different_rows > 0 or missing_rows > 0 or extra_rows > 0)

            table_summary = {
                "rows_in_source": rows_in_source,
                "rows_in_destination": rows_in_destination,
                "matching_rows": matching_rows,
                "different_rows": different_rows,
                "missing_rows": missing_rows,
                "extra_rows": extra_rows,
                "has_differences": has_differences
            }

            # Create table comparison result with proper structure
            table_comparison = {
                "success": has_differences == False,  # success is false when there are differences
                "meta": {
                    "source_schema": schema1,
                    "destination_schema": schema2,
                    "table": table,
                    "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
                },
                "summary": table_summary,
                "details": {
                    "matching_rows": matching_rows_details[:max_differences_to_track],
                    "different_rows": different_rows_details[:max_differences_to_track] if different_rows > 0 else [],
                    "missing_rows": missing_rows_details[:max_differences_to_track] if missing_rows > 0 else [],
                    "extra_rows": extra_rows_details[:max_differences_to_track] if extra_rows > 0 else []
                }
            }

            print(
                f"✅ Table {table} comparison completed: {matching_rows} matching, {different_rows} different, {missing_rows} missing, {extra_rows} extra")
            return table_comparison

def process_table_result(table, table_comparison, summary, table_comparisons, temp_dir, config):
            """
            Process the result of a table comparison and update the summary.
            Optionally save large table results to disk to minimize memory usage.
            """
            # Update overall summary with this table's results
            table_summary = table_comparison.get("summary", {})
            summary["total_rows_source"] += table_summary.get("rows_in_source", 0)
            summary["total_rows_destination"] += table_summary.get("rows_in_destination", 0)
            summary["total_matching_rows"] += table_summary.get("matching_rows", 0)
            summary["total_different_rows"] += table_summary.get("different_rows", 0)
            summary["total_missing_rows"] += table_summary.get("missing_rows", 0)
            summary["total_extra_rows"] += table_summary.get("extra_rows", 0)

            if table_summary.get("has_differences", False):
                summary["all_matched"] = False

            # Decide whether to keep in memory or save to disk
            if (config.get("save_tables_to_disk", False) or
                    table_summary.get("rows_in_source", 0) > config.get("large_table_threshold", 10000)):

                # Save large tables to disk to minimize memory usage
                table_path = os.path.join(temp_dir, f"{table}_comparison.json")
                try:
                    with open(table_path, "w") as f:
                        json.dump(table_comparison, f)

                    # Keep only a reference in memory
                    table_comparisons[table] = {
                        "summary": table_summary,
                        "disk_path": table_path
                    }
                    print(f"✅ Table {table} saved to disk ({table_summary.get('rows_in_source', 0)} rows)")
                except Exception as e:
                    print(f"⚠️ Error saving table to disk: {e}")
                    # Fallback to keeping in memory
                    table_comparisons[table] = table_comparison
            else:
                # Small table, keep in memory
                table_comparisons[table] = table_comparison

def run_great_expectations_validation(common_tables, schema1, schema2, context, chunk_size, config):
            """
            Run Great Expectations validation for all tables in chunks to minimize memory usage.
            Returns a dictionary of validation results.
            """
            if context is None:
                print("Great Expectations context not available, skipping validation")
                return {}

            ge_results = {}

            # Process tables in small batches to limit memory usage
            ge_batch_size = config.get("ge_batch_size", 1)  # Process one table at a time for GE to minimize memory

            for i in range(0, len(common_tables), ge_batch_size):
                batch_tables = common_tables[i:i + ge_batch_size]
                print(f"Running GE validation for tables: {batch_tables}")

                for table in batch_tables:
                    try:
                        # Load data for both schemas
                        project_dir = os.path.dirname(os.path.abspath(__file__))
                        data_dir = config.get("data_directory", "")

                        if data_dir.startswith("./"):
                            data_dir = os.path.join(project_dir, data_dir[2:])

                        schema1_dir = os.path.join(data_dir, schema1)
                        schema2_dir = os.path.join(data_dir, schema2)

                        df1 = None
                        df2 = None

                        # Load schema1 data
                        if os.path.exists(schema1_dir):
                            docx_files = glob.glob(os.path.join(schema1_dir, "**/*.docx"), recursive=True)
                            for file_path in docx_files:
                                try:
                                    inserts = extract_insert_statements(file_path)
                                    dataframes = inserts_to_dataframe(inserts)
                                    if table in dataframes:
                                        df1 = dataframes[table]
                                        break
                                except Exception as e:
                                    print(f"Error loading schema1 data: {e}")

                        # Load schema2 data
                        if os.path.exists(schema2_dir):
                            docx_files = glob.glob(os.path.join(schema2_dir, "**/*.docx"), recursive=True)
                            for file_path in docx_files:
                                try:
                                    inserts = extract_insert_statements(file_path)
                                    dataframes = inserts_to_dataframe(inserts)
                                    if table in dataframes:
                                        df2 = dataframes[table]
                                        break
                                except Exception as e:
                                    print(f"Error loading schema2 data: {e}")

                        # Run validation if both dataframes are available
                        if df1 is not None and df2 is not None:
                            print(f"Running GE validation for table {table}...")
                            result = compare_data_with_ge(df1, df2, table, context)
                            ge_results[table] = {
                                "success": result.success,
                                "statistics": result.statistics,
                                "results": [r.to_json_dict() for r in result.results]
                            }
                            print(f"✅ GE validation for table {table} completed")
                        else:
                            print(f"⚠️ Could not load data for table {table}")
                            ge_results[table] = {"error": "Could not load data for validation"}

                    except Exception as e:
                        print(f"⚠️ Error in GE validation for table {table}: {e}")
                        ge_results[table] = {"error": str(e)}

            return ge_results

def process_data_in_batches(config, use_ge, context):
            """
            Process database comparison in batches to reduce memory usage
            """
            # Load schema names from config
            schema_names = config.get("schemas", [])
            print(f"Schemas specified in config: {schema_names}")

            if len(schema_names) != 2:
                print(f"⚠️ Need exactly two schemas for comparison")
                return None

            schema1, schema2 = schema_names
            print(f"Comparing data between schemas: {schema1} (source) and {schema2} (destination)")

            # Step 1: Find common tables between schemas without loading all data
            print("\nStep 1: Finding common tables between schemas...")
            try:
                common_tables = get_common_table_list(schema1, schema2, config)

                # Find tables that exist in only one schema
                schema1_only_tables = get_schema_only_tables(schema1, schema2, config)
                schema2_only_tables = get_schema_only_tables(schema2, schema1, config)

                mismatched_tables = {
                    f"{schema1}_only": list(schema1_only_tables),
                    f"{schema2}_only": list(schema2_only_tables)
                }

                print(f"Found {len(common_tables)} common tables to compare")
                print(f"Found {len(schema1_only_tables)} tables only in {schema1}")
                print(f"Found {len(schema2_only_tables)} tables only in {schema2}")
            except Exception as e:
                print(f"❌ Error finding common tables: {e}")
                return None

            if len(common_tables) == 0:
                print("❌ No common tables found between schemas. Cannot generate comparison report.")
                return None

            # Initialize summary data structure
            summary = {
                "total_tables": len(common_tables),
                "total_rows_source": 0,
                "total_rows_destination": 0,
                "total_matching_rows": 0,
                "total_different_rows": 0,
                "total_missing_rows": 0,
                "total_extra_rows": 0,
                "all_matched": True
            }

            # Initialize result container
            table_comparisons = {}

            # Process tables in batches with configurable batch size
            batch_size = config.get("batch_size", 3)  # Default to 3 tables per batch
            chunk_size = config.get("chunk_size", 1000)  # Default to 1000 rows per chunk

            print(f"\nStep 2: Processing tables in batches of {batch_size} with chunk size {chunk_size}...")

            # Create a directory for temporary storage
            report_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            temp_dir = os.path.join("validation_reports", "temp", report_id)
            os.makedirs(temp_dir, exist_ok=True)

            # Process tables in batches
            for i in range(0, len(common_tables), batch_size):
                batch_tables = common_tables[i:i + batch_size]
                print(
                    f"Processing batch {(i // batch_size) + 1}/{(len(common_tables) + batch_size - 1) // batch_size}: {batch_tables}")

                # Use parallel processing if available
                max_workers = min(len(batch_tables), multiprocessing.cpu_count() - 1, 4)

                if max_workers > 1 and config.get("use_parallel", True):
                    print(f"Using {max_workers} parallel processes for batch processing")
                    with ProcessPoolExecutor(max_workers=max_workers) as executor:
                        # Create tasks for each table
                        future_to_table = {
                            executor.submit(
                                compare_table_in_chunks,
                                schema1,
                                schema2,
                                table,
                                chunk_size,
                                config
                            ): table for table in batch_tables
                        }

                        # Process results as they complete
                        for future in as_completed(future_to_table):
                            table = future_to_table[future]
                            try:
                                table_comparison = future.result()
                                process_table_result(table, table_comparison, summary, table_comparisons,
                                                     temp_dir, config)
                            except Exception as e:
                                print(f"❌ Error processing table {table}: {e}")
                                import traceback
                                traceback.print_exc()
                else:
                    # Sequential processing
                    for table in batch_tables:
                        try:
                            table_comparison = compare_table_in_chunks(schema1, schema2, table, chunk_size,
                                                                       config)
                            process_table_result(table, table_comparison, summary, table_comparisons, temp_dir,
                                                 config)
                        except Exception as e:
                            print(f"❌ Error processing table {table}: {e}")
                            import traceback
                            traceback.print_exc()

                # Force garbage collection after each batch
                import gc
                gc.collect()

            # Run Great Expectations validation if enabled
            if use_ge and context is not None:
                print("\nStep 3: Running Great Expectations validations...")
                ge_results = run_great_expectations_validation(common_tables, schema1, schema2, context,
                                                               chunk_size, config)

                # Store GE results in table_comparisons
                for table, result in ge_results.items():
                    if table in table_comparisons:
                        table_comparisons[table]["ge_validation"] = result

            # Combine results for the final report
            data_comparison = {
                "meta": {
                    "source_schema": schema1,
                    "destination_schema": schema2,
                    "tables_compared": len(common_tables),
                    "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                    "report_id": report_id
                },
                "summary": summary,
                "mismatched_tables": mismatched_tables
            }

            # Handle table_comparisons specially because they might be on disk
            if config.get("save_tables_to_disk", False):
                data_comparison["table_comparisons_location"] = temp_dir
            else:
                data_comparison["table_comparisons"] = table_comparisons

            # Generate and save reports
            result = generate_and_save_reports(data_comparison, config, temp_dir)

            # Return result for UI integration
            return result

def generate_and_save_reports(data_comparison, config, temp_dir):
            """Generate and save HTML and JSON reports with optimized memory usage"""
            # Create report ID for consistent naming
            report_id = data_comparison["meta"]["report_id"]

            # Save JSON report
            print("\nSaving JSON report...")
            report_path = os.path.join("validation_reports", f"validation_report_{report_id}.json")
            try:
                with open(report_path, "w") as f:
                    json.dump(data_comparison, f, indent=2)
                print(f"✅ JSON report saved to {report_path}")
            except Exception as e:
                print(f"❌ Error saving JSON report: {e}")
                alt_path = f"validation_report_{report_id}.json"
                try:
                    with open(alt_path, "w") as f:
                        json.dump(data_comparison, f, indent=2)
                    print(f"✅ JSON report saved to {alt_path}")
                    report_path = alt_path
                except Exception as e2:
                    print(f"❌ Error saving to alternate path: {e2}")
                    return None

            # Load table comparison data from disk if needed
            if "table_comparisons_location" in data_comparison:
                print("Loading table comparison data from disk for HTML report...")
                table_comparisons = {}
                for filename in os.listdir(temp_dir):
                    if filename.endswith("_comparison.json"):
                        table_name = filename.replace("_comparison.json", "")
                        try:
                            with open(os.path.join(temp_dir, filename), "r") as f:
                                table_comparisons[table_name] = json.load(f)
                        except Exception as e:
                            print(f"⚠️ Error loading table {table_name} from disk: {e}")

                # Add to data_comparison for HTML report
                data_comparison["table_comparisons"] = table_comparisons

            # Generate HTML report
            html_report_path, _ = generate_html_report(data_comparison)
            if html_report_path:
                print(f"\n✅ Reports generated successfully:")
                print(f"  - JSON Report: {report_path}")
                print(f"  - HTML Report: {html_report_path}")

                # Optionally open the HTML report in the default browser
                if config.get("open_browser", True):
                    try:
                        import webbrowser
                        webbrowser.open('file://' + os.path.abspath(html_report_path))
                        print("✅ Opened HTML report in your default browser")
                    except Exception as e:
                        print(f"⚠️ Could not open browser automatically: {e}")
                        print(f"  You can open the HTML report at: file://{os.path.abspath(html_report_path)}")

            # Return report information for UI
            return {
                "success": True,
                "report_id": report_id,
                "json_report": report_path,
                "html_report": html_report_path
            }


def process_document_data(config, use_ge, context):
    """Process data using document-based approach and return report information."""
    # Create a report ID for this run
    report_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Step 1: Load data from all supported files with parallel processing
    print("\nStep 1: Loading data from documents (in parallel)...")
    data_dir = config["data_directory"]

    try:
        # Find all supported files - UPDATED TO INCLUDE SQL AND TXT
        files = []
        for ext in ['docx', 'sql', 'txt']:
            files.extend([f for f in os.listdir(data_dir) if f.endswith(f".{ext}")])

        print(f"Found {len(files)} supported documents in {data_dir}: {files}")
    except Exception as e:
        print(f"❌ Error accessing data directory {data_dir}: {e}")
        return {"success": False, "error": f"Failed to access data directory: {str(e)}"}

    # Prepare file paths for parallel processing
    file_paths = [os.path.join(data_dir, file) for file in files]

    # Determine number of workers based on CPU cores
    max_workers = min(multiprocessing.cpu_count(), len(file_paths))
    print(f"Using {max_workers} parallel workers for document processing")

    all_inserts = []
    # Use ProcessPoolExecutor for parallel file processing
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(process_single_file, file_path): file_path for file_path in file_paths}

        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                inserts = future.result()
                all_inserts.extend(inserts)
                print(f"✅ Completed processing: {file_path}")
            except Exception as e:
                print(f"❌ Error processing file {file_path}: {e}")

    print(f"🧠 Total rows extracted from INSERT statements: {len(all_inserts)}")

    if len(all_inserts) == 0:
        print("❌ No data extracted from documents. Cannot proceed.")
        return {"success": False, "error": "No data extracted from documents"}

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
        return {"success": False, "error": f"Error converting to DataFrames: {str(e)}"}

    # Organize data by schema
    print("\nStep 3: Organizing data by schema...")
    try:
        schema_data = organize_by_schema(all_dataframes)
        print(f"Found {len(schema_data)} schemas: {list(schema_data.keys())}")
    except Exception as e:
        print(f"❌ Error organizing data by schema: {e}")
        return {"success": False, "error": f"Error organizing data by schema: {str(e)}"}

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
            return {"success": False,
                    "error": "Need at least two schemas to generate comparison report"}

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
            # Fallback implementation
            common_tables = set(schema1_data.keys()) & set(schema2_data.keys())

        # Find tables that exist in only one schema
        schema1_only_tables = set(schema1_data.keys()) - set(schema2_data.keys())
        schema2_only_tables = set(schema2_data.keys()) - set(schema1_data.keys())
        mismatched_tables = schema1_only_tables.union(schema2_only_tables)

        print(f"Found {len(common_tables)} common tables to compare: {common_tables}")
        print(f"Found {len(mismatched_tables)} mismatched tables: {mismatched_tables}")

        if len(common_tables) == 0:
            print("❌ No common tables found between schemas. Cannot generate comparison report.")
            return {"success": False, "error": "No common tables found between schemas"}

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

            # Add report ID
            data_comparison["meta"]["report_id"] = report_id

            print("✅ Data comparison completed")

        except Exception as e:
            print(f"❌ Error generating comparison report: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": f"Error generating comparison report: {str(e)}"}

        # Optional: Use Great Expectations for additional validation
        if use_ge and context is not None:
            print("\nPerforming Great Expectations validations (in parallel)...")
            ge_results = {}

            # Prepare tasks for parallel processing
            validation_tasks = []
            for table in common_tables:
                if table in schema1_data and table in schema2_data:
                    df1 = schema1_data[table]
                    df2 = schema2_data[table]
                    validation_tasks.append((table, df1, df2, context))

            # Use parallel processing for Great Expectations validation
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                for table, result in executor.map(validate_table_with_ge, validation_tasks):
                    ge_results[table] = result
                    print(f"✅ GE validation for table {table} completed")

            # Add GE results to the comparison report
            data_comparison["great_expectations"] = ge_results

        # Step 6: Save JSON report
        print("\nStep 6: Saving JSON report...")
        json_report_path = os.path.join("validation_reports", f"validation_report_{report_id}.json")
        try:
            os.makedirs(os.path.dirname(json_report_path), exist_ok=True)
            with open(json_report_path, "w") as f:
                json.dump(data_comparison, f, indent=2)
            print(f"✅ JSON report saved to {json_report_path}")
        except Exception as e:
            print(f"❌ Error saving JSON report: {e}")
            # Try with a different path as a fallback
            alt_path = f"validation_report_{report_id}.json"
            print(f"Trying to save to {alt_path} instead...")
            try:
                with open(alt_path, "w") as f:
                    json.dump(data_comparison, f, indent=2)
                print(f"✅ JSON report saved to {alt_path}")
                json_report_path = alt_path
            except Exception as e2:
                print(f"❌ Error saving to alternate path: {e2}")
                return {"success": False, "error": f"Error saving report: {str(e)} and {str(e2)}"}

        # Step 7: Generate HTML report
        html_report_path, _ = generate_html_report(data_comparison)
        if not html_report_path:
            print("⚠️ Could not generate HTML report")
            html_report_path = None

        # Return report information
        return {
            "success": True,
            "report_id": report_id,
            "json_report": json_report_path,
            "html_report": html_report_path
        }
    else:
        print(
            f"❌ One or both schemas not found in the data. Available schemas: {list(schema_data.keys())}")
        return {"success": False,
                "error": f"One or both schemas not found. Available: {list(schema_data.keys())}"}


def get_validation_status(report_id):
    """
    Get the status of a validation process by checking if the report files exist.
    Returns a dictionary with status information.
    """
    json_report_path = os.path.join("validation_reports", f"validation_report_{report_id}.json")
    html_report_path = os.path.join("validation_reports", f"validation_report_{report_id}.html")

    if os.path.exists(json_report_path) and os.path.exists(html_report_path):
        return {
            "status": "completed",
            "report_id": report_id,
            "json_report": json_report_path,
            "html_report": html_report_path
        }
    elif os.path.exists(json_report_path):
        return {
            "status": "partially_completed",
            "report_id": report_id,
            "json_report": json_report_path,
            "html_report": None
        }
    else:
        # Check if temporary files exist to indicate progress
        temp_dir = os.path.join("validation_reports", "temp", report_id)
        if os.path.exists(temp_dir) and len(os.listdir(temp_dir)) > 0:
            return {
                "status": "in_progress",
                "report_id": report_id,
                "progress": len(os.listdir(temp_dir))
            }
        else:
            return {
                "status": "not_found",
                "report_id": report_id
            }


def list_available_reports():
    """
    Get a list of all available validation reports.
    Returns a list of report metadata.
    """
    reports = []
    report_dir = "validation_reports"

    if not os.path.exists(report_dir):
        return reports

    for filename in os.listdir(report_dir):
        if filename.endswith(".json") and filename.startswith("validation_report_"):
            try:
                report_path = os.path.join(report_dir, filename)
                with open(report_path, 'r') as f:
                    report_data = json.load(f)

                    # Extract report ID from filename
                    report_id = filename.replace("validation_report_", "").replace(".json", "")

                    # Get metadata
                    meta = report_data.get("meta", {})
                    summary = report_data.get("summary", {})

                    # Calculate match percentage
                    match_percentage = 0
                    if summary.get("total_rows_source", 0) > 0:
                        match_percentage = round((summary.get("total_matching_rows", 0) /
                                                  summary.get("total_rows_source", 0)) * 100, 1)

                    # Create report metadata
                    reports.append({
                        "id": report_id,
                        "timestamp": meta.get("timestamp", ""),
                        "source_schema": meta.get("source_schema", ""),
                        "destination_schema": meta.get("destination_schema", ""),
                        "tables_compared": meta.get("tables_compared", 0),
                        "total_rows": summary.get("total_rows_source", 0),
                        "matching_rows": summary.get("total_matching_rows", 0),
                        "different_rows": summary.get("total_different_rows", 0),
                        "missing_rows": summary.get("total_missing_rows", 0),
                        "match_percentage": match_percentage,
                        "json_report": os.path.join(report_dir, filename),
                        "html_report": os.path.join(report_dir, filename.replace(".json", ".html"))
                    })
            except Exception as e:
                print(f"Error loading report {filename}: {e}")

    # Sort by timestamp (newest first)
    reports.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return reports


def get_report_by_id(report_id):
    """
    Get a specific report by ID.
    Returns the report data if found, None otherwise.
    """
    report_path = os.path.join("validation_reports", f"validation_report_{report_id}.json")

    if not os.path.exists(report_path):
        return None

    try:
        with open(report_path, 'r') as f:
            report_data = json.load(f)
        return report_data
    except Exception as e:
        print(f"Error loading report {report_id}: {e}")
        return None


def clean_up_temporary_files(report_id=None):
    """
    Clean up temporary files for a specific report or all reports.
    Returns True if successful, False otherwise.
    """
    temp_dir = os.path.join("validation_reports", "temp")

    if not os.path.exists(temp_dir):
        return True

    try:
        if report_id:
            # Clean up specific report
            report_temp_dir = os.path.join(temp_dir, report_id)
            if os.path.exists(report_temp_dir):
                import shutil
                shutil.rmtree(report_temp_dir)
                print(f"✅ Temporary files for report {report_id} removed")
        else:
            # Clean up all temporary files
            import shutil
            shutil.rmtree(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)
            print("✅ All temporary files removed")
        return True
    except Exception as e:
        print(f"⚠️ Error cleaning up temporary files: {e}")
        return False


def main(config=None):
    """
    Run the validation process with the provided config.
    If config is a string, it's treated as a path to a YAML file.
    If config is a dictionary, it's used directly.

    Returns a dictionary with the report results.
    """
    print("Starting report generation process...")

    # Load configuration
    if isinstance(config, str):
        # Load from file path
        print(f"Loading configuration from {config}...")
        try:
            with open(config, "r") as f:
                config = yaml.safe_load(f)
            print("✅ Configuration loaded successfully")
        except Exception as e:
            print(f"❌ Error loading configuration: {e}")
            return {"success": False, "error": f"Failed to load config: {str(e)}"}
    elif config is None:
        # Use default config.yaml
        print("Loading configuration from config.yaml...")
        try:
            with open("config.yaml", "r") as f:
                config = yaml.safe_load(f)
            print("✅ Configuration loaded successfully")
        except Exception as e:
            print(f"❌ Error loading configuration: {e}")
            return {"success": False, "error": f"Failed to load default config: {str(e)}"}
    elif not isinstance(config, dict):
        print(f"❌ Invalid config type: {type(config)}")
        return {"success": False,
                "error": "Config must be a dictionary, path to YAML file, or None"}

    # Create output directories
    print("Creating validation_reports directory if it doesn't exist...")
    try:
        os.makedirs("validation_reports", exist_ok=True)
        print("✅ Directory structure verified")
    except Exception as e:
        print(f"❌ Error creating directories: {e}")
        return {"success": False, "error": f"Failed to create directories: {str(e)}"}

    # Initialize Great Expectations context
    context = None  # Initialize as None to handle failure case
    try:
        print("Initializing Great Expectations context...")
        context = ge.data_context.DataContext(config.get("ge_dir", "./great_expectations"))
        print("✅ Great Expectations context loaded")
        use_ge = True
    except Exception as e:
        print(f"⚠️ Could not load Great Expectations context: {e}")
        print("Continuing without Great Expectations...")
        use_ge = False

    # Process data based on chosen approach
    if config.get("use_direct_comparison", False):
        print("\nUsing direct database-to-database comparison for better space efficiency...")
        # Process tables in batches to reduce memory usage
        result = process_data_in_batches(config, use_ge, context)
        if result is None:
            return {"success": False, "error": "Failed to process data in batches"}
        return result
    else:
        # Use the original document-based approach
        try:
            result = process_document_data(config, use_ge, context)
            return result
        except Exception as e:
            import traceback
            print(f"❌ Error in document processing: {e}")
            traceback.print_exc()
            return {"success": False, "error": f"Error in document processing: {str(e)}"}


if __name__ == "__main__":
    # When run directly, use the default config.yaml
    result = main()
    print("\nValidation process completed with result:", result)