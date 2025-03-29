import json
import io
import boto3
import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename

app = Flask(__name__)

def decode_bytes(obj):
    """
    Recursively decode all bytes in a given object (dict, list, etc.) to strings.
    """
    if isinstance(obj, dict):
        return {k: decode_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [decode_bytes(item) for item in obj]
    elif isinstance(obj, bytes):
        return obj.decode('utf-8', errors='ignore')  # Decode bytes to string (ignore errors if any)
    else:
        return obj

def parse_avro_file(file):
    """
    Parse an Avro file and return its schema, metadata, and records.
    
    Args:
        file (file-like object): Avro file to parse.
    """
    try:
        # Read the Avro file using DataFileReader
        reader = DataFileReader(file, DatumReader())

        # Extract schema and decode it from bytes to string
        schema_str = reader.meta.get('avro.schema').decode('utf-8')
        schema = json.loads(schema_str)  # Parse schema as JSON for better formatting

        # Extract metadata and decode from bytes to strings
        metadata = {
            'codec': reader.meta.get('avro.codec', b'null').decode('utf-8', errors='ignore'),
            'sync_marker': reader.sync_marker.hex() if reader.sync_marker else 'None'
        }

        # Extract and collect records, also decoding any bytes to strings
        records = []
        for record in reader:
            records.append(decode_bytes(record))  # Ensure all bytes in records are decoded

        # Return parsed data with the schema, metadata, and records
        return {
            'schema': decode_bytes(schema),  # Decode any bytes in the schema if necessary
            'metadata': metadata,
            'records': records,
            'total_records': len(records)
        }

    except Exception as e:
        return {'error': f"An error occurred: {str(e)}"}
@app.route('/upload', methods=['POST'])
def upload_avro_file():
    # Check if a file was part of the request
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    
    # Ensure the file is an Avro file
    if file and file.filename.endswith('.avro'):
        try:
            # Parse the file
            parsed_data = parse_avro_file(file)
            return jsonify(parsed_data)
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Invalid file type. Please upload an Avro file.'}), 400

if __name__ == '__main__':
    # Run the app
    app.run(debug=True)
