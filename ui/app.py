from flask import Flask, render_template, request, jsonify, send_file
import os
import sys
import json
import yaml
import threading
import datetime
import traceback
from werkzeug.utils import secure_filename
import shutil

print(f"Current working directory: {os.getcwd()}")
print(f"Python path: {sys.path}")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
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

SOURCE_UPLOAD_FOLDER = os.path.join(UPLOAD_FOLDER, 'source')
DEST_UPLOAD_FOLDER = os.path.join(UPLOAD_FOLDER, 'destination')
os.makedirs(SOURCE_UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DEST_UPLOAD_FOLDER, exist_ok=True)

validation_status = {}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def load_config():
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def extract_schema_from_filename(filename):
    name_without_ext = os.path.splitext(filename)[0].lower()
    config = load_config()
    for schema in config.get('schemas', []):
        if schema.lower() in name_without_ext:
            return schema
    return name_without_ext

def discover_tables_from_files(file_paths):
    tables = set()
    try:
        from parsers.docx_data_parser import extract_insert_statements
    except ImportError as e:
        print(f"Error importing parser: {e}")
        return list(tables)

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


@app.route('/api/status/<run_id>', methods=['GET'])
def get_validation_status(run_id):
    """Return current status and progress for a given run_id."""
    status = validation_status.get(run_id)
    if status:
        return jsonify({
            'success': True,
            'run_id': run_id,
            'status': status.get('status'),
            'progress': status.get('progress'),
            'current_table': status.get('current_table'),
            'error': status.get('error'),
            'results': status.get('results', [])
        })
    else:
        return jsonify({
            'success': False,
            'error': f'No validation run found with run_id: {run_id}'
        }), 404

@app.route('/api/run-validation', methods=['POST'])
def run_validation():
        try:
            # Load the config first so we can access the schema paths
            config = load_config()

            source_files = []
            source_file_paths = []
            source_unsupported = []
            if 'source_files' in request.files:
                files = request.files.getlist('source_files')
                for file in files:
                    if file and allowed_file(file.filename):
                        filename = secure_filename(file.filename)
                        source_files.append(filename)
                    elif file:
                        source_unsupported.append(file.filename)

            dest_files = []
            dest_file_paths = []
            dest_unsupported = []
            if 'dest_files' in request.files:
                files = request.files.getlist('dest_files')
                for file in files:
                    if file and allowed_file(file.filename):
                        filename = secure_filename(file.filename)
                        dest_files.append(filename)
                    elif file:
                        dest_unsupported.append(file.filename)

            if source_unsupported or dest_unsupported:
                unsupported_msg = []
                if source_unsupported:
                    unsupported_msg.append(f"source files: {', '.join(source_unsupported)}")
                if dest_unsupported:
                    unsupported_msg.append(f"destination files: {', '.join(dest_unsupported)}")
                return jsonify({
                    'success': False,
                    'error': f"Unsupported file types detected in {' and '.join(unsupported_msg)}. Allowed types are: {', '.join(f'.{ext}' for ext in ALLOWED_EXTENSIONS)}"
                })

            if not source_files or not dest_files:
                return jsonify({
                    'success': False,
                    'error': 'Both source and destination files are required'
                })

            batch_size = int(request.form.get('batch_size', 100))
            chunk_size = int(request.form.get('chunk_size', 1000))
            run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

            validation_status[run_id] = {
                'status': 'running',
                'progress': 0,
                'current_table': 'Initializing...',
                'error': None,
                'results': []
            }

            # Determine source and destination schemas from config
            schema_names = config.get('schemas', [])
            if len(schema_names) != 2:
                return jsonify({
                    'success': False,
                    'error': 'Need exactly two schemas in configuration'
                })

            source_schema = schema_names[0]
            dest_schema = schema_names[1]

            print(f"Using source schema: {source_schema}")
            print(f"Using destination schema: {dest_schema}")

            # Get the exact same paths from config that would be used in direct execution
            source_dir = config['schema_paths'][source_schema]
            dest_dir = config['schema_paths'][dest_schema]

            print(f"Using source directory: {source_dir}")
            print(f"Using destination directory: {dest_dir}")

            # Ensure directories exist
            os.makedirs(source_dir, exist_ok=True)
            os.makedirs(dest_dir, exist_ok=True)

            # Save the uploaded files directly to these directories
            for file in request.files.getlist('source_files'):
                if file and allowed_file(file.filename):
                    filepath = os.path.join(source_dir, secure_filename(file.filename))
                    file.save(filepath)
                    source_file_paths.append(filepath)

            for file in request.files.getlist('dest_files'):
                if file and allowed_file(file.filename):
                    filepath = os.path.join(dest_dir, secure_filename(file.filename))
                    file.save(filepath)
                    dest_file_paths.append(filepath)

            # Debug - print directory contents
            print("\n=== Source Directory Contents ===")
            for file in os.listdir(source_dir):
                print(f"  - {file} ({os.path.getsize(os.path.join(source_dir, file))} bytes)")

            print("\n=== Destination Directory Contents ===")
            for file in os.listdir(dest_dir):
                print(f"  - {file} ({os.path.getsize(os.path.join(dest_dir, file))} bytes)")

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
                    return jsonify({'success': True, 'run_id': run_id})
            except Exception as e:
                error_msg = f'Error discovering tables: {str(e)}'
                print(error_msg)
                validation_status[run_id]['status'] = 'failed'
                validation_status[run_id]['error'] = error_msg
                return jsonify({'success': True, 'run_id': run_id})

            file_types = {filename.rsplit('.', 1)[1].lower() for filename in source_files + dest_files}

            # Use the config directly without modifying schema paths
            # This ensures we use exactly the same paths as in direct execution
            runtime_config = config.copy()
            runtime_config.update({
                'batch_size': batch_size,
                'chunk_size': chunk_size,
                'use_direct_comparison': True,
                'selected_tables': selected_tables,
                'file_types': list(file_types),
                'source_files': source_file_paths,
                'dest_files': dest_file_paths
            })

            # Print the final config for debugging
            print("\n=== Runtime Configuration ===")
            for key, value in runtime_config.items():
                if key not in ['source_files', 'dest_files']:  # Skip long lists
                    print(f"{key}: {value}")
            print(f"source_files: {len(runtime_config.get('source_files', []))} files")
            print(f"dest_files: {len(runtime_config.get('dest_files', []))} files")

            def run_validation_thread():
                try:
                    validation_status[run_id]['current_table'] = f'Found {len(selected_tables)} common tables'
                    validation_status[run_id]['progress'] = 10

                    print(f"Starting report generation with config:")
                    print(f"  - Schema paths: {runtime_config['schema_paths']}")
                    print(f"  - Selected tables: {runtime_config['selected_tables']}")

                    result = generate_report.main(runtime_config)

                    if result and result.get('success'):
                        validation_status[run_id]['status'] = 'completed'
                        validation_status[run_id]['progress'] = 100
                        validation_status[run_id]['report_id'] = result.get('report_id')
                        validation_status[run_id]['html_report'] = result.get('html_report')
                        validation_status[run_id]['json_report'] = result.get('json_report')

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
                                        'match': not summary.get('has_differences', False),
                                        'matching_rows': summary.get('matching_rows', 0),
                                        'different_rows': summary.get('different_rows', 0),
                                        'missing_rows': summary.get('missing_rows', 0),
                                        'extra_rows': summary.get('extra_rows', 0),
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

            thread = threading.Thread(target=run_validation_thread)
            thread.start()

            return jsonify({'success': True, 'run_id': run_id})

        except Exception as e:
            error_msg = f"Error in run_validation: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            return jsonify({'success': False, 'error': str(e)})
        except Exception as e:
         error_msg = f"Error in run_validation: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return jsonify({'success': False, 'error': str(e)})

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
    app.run(debug=True, port=5003)