from flask import Flask, render_template, request, jsonify, send_file
import os
import sys
import json
import yaml
import threading
import datetime
import traceback
from werkzeug.utils import secure_filename

# Debug imports
print(f"Current working directory: {os.getcwd()}")
print(f"Python path: {sys.path}")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    # Import the entire module instead of just the main function
    import generate_report

    print("Successfully imported generate_report module")
except Exception as e:
    print(f"Failed to import generate_report: {e}")
    traceback.print_exc()

app = Flask(__name__)
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'uploads')
ALLOWED_EXTENSIONS = {'docx', 'sql', 'txt'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Create separate folders for source and destination files
SOURCE_UPLOAD_FOLDER = os.path.join(UPLOAD_FOLDER, 'source')
DEST_UPLOAD_FOLDER = os.path.join(UPLOAD_FOLDER, 'destination')
os.makedirs(SOURCE_UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DEST_UPLOAD_FOLDER, exist_ok=True)

# Track validation status
validation_status = {}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def load_config():
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def extract_schema_from_filename(filename):
    """Extract schema name from filename"""
    # Remove file extension
    name_without_ext = os.path.splitext(filename)[0]

    # Handle common patterns
    if 'employee_management' in name_without_ext.lower():
        return 'employee_management'
    elif 'contractor_management' in name_without_ext.lower():
        return 'contractor_management'
    else:
        # Return the full name without extension
        return name_without_ext


def discover_tables_from_files(file_paths):
    """Discover tables from uploaded files"""
    tables = set()

    # Import parser
    try:
        from parsers.docx_data_parser import extract_insert_statements
    except ImportError as e:
        print(f"Error importing parser: {e}")
        return list(tables)

    # Extract tables from files
    for file_path in file_paths:
        try:
            print(f"Processing file: {os.path.basename(file_path)}")
            inserts = extract_insert_statements(file_path)
            for insert in inserts:
                table_name = insert.get("table_name")
                if table_name:
                    tables.add(table_name)
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

    print(f"Found {len(tables)} tables")
    return sorted(list(tables))


@app.route('/')
def index():
    return render_template('index.html')


@ app.route('/api/run-validation', methods=['POST'])


def run_validation():
    try:
        # Handle source file uploads
        source_files = []
        source_file_paths = []
        if 'source_files' in request.files:
            files = request.files.getlist('source_files')
            for file in files:
                if file and allowed_file(file.filename):
                    filename = secure_filename(file.filename)
                    filepath = os.path.join(SOURCE_UPLOAD_FOLDER, filename)
                    file.save(filepath)
                    source_files.append(filename)
                    source_file_paths.append(filepath)

        # Handle destination file uploads
        dest_files = []
        dest_file_paths = []
        if 'dest_files' in request.files:
            files = request.files.getlist('dest_files')
            for file in files:
                if file and allowed_file(file.filename):
                    filename = secure_filename(file.filename)
                    filepath = os.path.join(DEST_UPLOAD_FOLDER, filename)
                    file.save(filepath)
                    dest_files.append(filename)
                    dest_file_paths.append(filepath)

        # Validate files were uploaded
        if not source_files or not dest_files:
            return jsonify({
                'success': False,
                'error': 'Both source and destination files are required'
            })

        # Get form data
        batch_size = int(request.form.get('batch_size', 100))
        chunk_size = int(request.form.get('chunk_size', 1000))

        # Create run ID
        run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        # Initialize status
        validation_status[run_id] = {
            'status': 'running',
            'progress': 0,
            'current_table': 'Initializing...',
            'error': None,
            'results': []
        }

        # Load base config
        config = load_config()

        # Extract schema names from filenames
        source_schema = None
        dest_schema = None

        for filename in source_files:
            extracted = extract_schema_from_filename(filename)
            if extracted in config.get('schemas', []):
                source_schema = extracted
                break

        for filename in dest_files:
            extracted = extract_schema_from_filename(filename)
            if extracted in config.get('schemas', []):
                dest_schema = extracted
                break

        # Default to config values if not found
        source_schema = source_schema or config.get('source-schema', 'employee_management')
        dest_schema = dest_schema or config.get('dest-schema', 'contractor_management')

        print(f"Using source schema: {source_schema}")
        print(f"Using destination schema: {dest_schema}")

        # Discover tables from uploaded files
        try:
            print(f"Discovering tables from uploaded files")
            source_tables = set(discover_tables_from_files(source_file_paths))
            dest_tables = set(discover_tables_from_files(dest_file_paths))
            selected_tables = sorted(list(source_tables.intersection(dest_tables)))

            print(f"Source tables: {source_tables}")
            print(f"Destination tables: {dest_tables}")
            print(f"Common tables: {selected_tables}")

            if not selected_tables:
                validation_status[run_id]['status'] = 'failed'
                validation_status[run_id]['error'] = f'No common tables found between source and destination files'
                return jsonify({
                    'success': True,
                    'run_id': run_id
                })
        except Exception as e:
            error_msg = f'Error discovering tables: {str(e)}'
            print(error_msg)
            validation_status[run_id]['status'] = 'failed'
            validation_status[run_id]['error'] = error_msg
            return jsonify({
                'success': True,
                'run_id': run_id
            })

        # Extract file types from uploaded files
        file_types = set()
        for filename in source_files + dest_files:
            ext = filename.rsplit('.', 1)[1].lower()
            file_types.add(ext)

        # Create temporary directories for processing using schema names
        temp_source_dir = os.path.join(UPLOAD_FOLDER, f'{source_schema}_{run_id}')
        temp_dest_dir = os.path.join(UPLOAD_FOLDER, f'{dest_schema}_{run_id}')
        os.makedirs(temp_source_dir, exist_ok=True)
        os.makedirs(temp_dest_dir, exist_ok=True)

        # Copy uploaded files to temporary directories
        import shutil
        for src_file in source_file_paths:
            shutil.copy2(src_file, temp_source_dir)
        for dest_file in dest_file_paths:
            shutil.copy2(dest_file, temp_dest_dir)

        # Create a modified config with correct paths
        runtime_config = config.copy()

        # Update schemas in config
        runtime_config['schemas'] = [source_schema, dest_schema]

        # Update schema paths to point to uploaded files (temporary directories)
        runtime_config['schema_paths'] = {
            source_schema: temp_source_dir,
            dest_schema: temp_dest_dir
        }

        # Update data directory to upload folder
        runtime_config['data_directory'] = UPLOAD_FOLDER

        # Add additional runtime values
        runtime_config.update({
            'batch_size': batch_size,
            'chunk_size': chunk_size,
            'use_direct_comparison': True,
            'selected_tables': selected_tables,
            'file_types': list(file_types),
            'source_files': source_file_paths,  # Keep original paths
            'dest_files': dest_file_paths,  # Keep original paths
            'uploaded_files': source_file_paths + dest_file_paths,
            'temp_dirs': {
                'source': temp_source_dir,
                'dest': temp_dest_dir
            }
        })

        # Run validation in background thread
        def run_validation_thread():
            try:
                # Update progress
                validation_status[run_id]['current_table'] = f'Found {len(selected_tables)} common tables'
                validation_status[run_id]['progress'] = 10

                print(f"Starting report generation with config:")
                print(f"  - Source files: {runtime_config['source_files']}")
                print(f"  - Dest files: {runtime_config['dest_files']}")
                print(f"  - Schema paths: {runtime_config['schema_paths']}")
                print(f"  - Selected tables: {runtime_config['selected_tables']}")

                # Call the main function from generate_report module with modified config
                result = generate_report.main(runtime_config)

                print(f"Report generation result: {result}")

                # Clean up temporary directories
                try:
                    shutil.rmtree(temp_source_dir)
                    shutil.rmtree(temp_dest_dir)
                except Exception as e:
                    print(f"Error cleaning up temp directories: {e}")

                if result and result.get('success'):
                    validation_status[run_id]['status'] = 'completed'
                    validation_status[run_id]['progress'] = 100
                    validation_status[run_id]['report_id'] = result.get('report_id')
                    validation_status[run_id]['html_report'] = result.get('html_report')
                    validation_status[run_id]['json_report'] = result.get('json_report')

                    # Extract results from report data
                    if result.get('json_report'):
                        try:
                            with open(result['json_report'], 'r') as f:
                                report_data = json.load(f)

                            table_comparisons = report_data.get('table_comparisons', {})
                            results = []

                            for table_name, table_data in table_comparisons.items():
                                summary = table_data.get('summary', {})
                                results.append({
                                    'table_name': table_name,
                                    'source_count': summary.get('rows_in_source', 0),
                                    'destination_count': summary.get('rows_in_destination', 0),
                                    'match': summary.get('has_differences', False) == False,
                                    'matching_rows': summary.get('matching_rows', 0),
                                    'different_rows': summary.get('different_rows', 0),
                                    'missing_rows': summary.get('missing_rows', 0),
                                    'processing_time': 1.0
                                })

                            validation_status[run_id]['results'] = results
                            validation_status[run_id]['report_data'] = report_data
                        except Exception as e:
                            print(f"Error reading report data: {e}")
                else:
                    validation_status[run_id]['status'] = 'failed'
                    validation_status[run_id]['error'] = result.get('error',
                                                                    'Unknown error') if result else 'Report generation returned None'
            except Exception as e:
                error_msg = f"Error during validation: {str(e)}\n{traceback.format_exc()}"
                print(error_msg)
                validation_status[run_id]['status'] = 'failed'
                validation_status[run_id]['error'] = str(e)

                # Clean up temp directories on error
                try:
                    if os.path.exists(temp_source_dir):
                        shutil.rmtree(temp_source_dir)
                    if os.path.exists(temp_dest_dir):
                        shutil.rmtree(temp_dest_dir)
                except Exception as cleanup_error:
                    print(f"Error cleaning up temp directories: {cleanup_error}")

        thread = threading.Thread(target=run_validation_thread)
        thread.start()

        return jsonify({
            'success': True,
            'run_id': run_id
        })

    except Exception as e:
        error_msg = f"Error in run_validation: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return jsonify({
            'success': False,
            'error': str(e)
        })


@app.route('/reports/<report_id>')
def view_report(report_id):
    try:
        report_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'validation_reports',
            f'validation_report_{report_id}.html'
        )

        if os.path.exists(report_path):
            return send_file(report_path)
        else:
            return "Report not found", 404
    except Exception as e:
        return str(e), 500


if __name__ == '__main__':
    app.run(debug=True, port=5002)