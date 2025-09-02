from flask import Flask, request, jsonify, send_file
from flask_cors import cross_origin
import os
from functions.perform import perform
from local_connections.db_connect import execute_query

app = Flask(__name__)

instance = None
resources = './resources'
os.makedirs(resources, exist_ok=True)
app.config['UPLOAD_FOLDER'] = resources
app.config['ALLOWED_FILE_TYPES'] = {'csv', 'txt', 'xlsx', 'json', 'zip'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_FILE_TYPES']


@app.route("/sourceFile", methods=["POST"])
@cross_origin()
def handle_source_file_upload():
    source_file = request.files.get('source_file')
    if source_file:
        if allowed_file(source_file.filename):
            source_filename = os.path.join(app.config['UPLOAD_FOLDER'], source_file.filename)
            source_file.save(source_filename)
            return jsonify({"source_fileName": os.path.basename(source_filename).replace("\\", "/")}), 200
        else:
            return jsonify({"error": "Invalid source file format"}), 400
    else:
        return jsonify({"error": "No file uploaded"}), 400


@app.route("/targetFile", methods=["POST"])
@cross_origin()
def handle_target_file_upload():
    target_file = request.files.get('target_file')
    if target_file:
        if allowed_file(target_file.filename):
            target_filename = os.path.join(app.config['UPLOAD_FOLDER'], target_file.filename)
            target_file.save(target_filename)
            return jsonify({"target_fileName": os.path.basename(target_filename).replace("\\", "/")}), 200
        else:
            return jsonify({"error": "Invalid target file format"}), 400
    else:
        return jsonify({"error": "No file uploaded"}), 400


@app.route("/perform_validation", methods=["POST"])
@cross_origin()
def perform_validation():
    try:
        data = request.get_json()
        global instance
        instance = perform(data)
        response = {
            "connection_status": "Connected",
            "source_headers": instance.get_headers("source")
        }
        connection_data = data.get("Connection", {})
        if "target" in connection_data and connection_data["target"]:
            response["target_headers"] = instance.get_headers("target")

        result = instance.execute(data)
        return jsonify(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/connect", methods=["POST"])
@cross_origin()
def connect():
    global instance
    data = request.get_json()
    instance = perform(data)

    response = {
        "connection_status": "Connected",
        "source_headers": instance.get_headers("source")
    }

    if instance.is_connection_type("source", "databricks"):
        response["source_datatypes"] = instance.get_datatypes("source")

    if instance.has_connection("target"):
        response["target_headers"] = instance.get_headers("target")
        if instance.is_connection_type("target", "databricks"):
            response["target_datatypes"] = instance.get_datatypes("target")

    return jsonify(response), 200


@app.route("/reset", methods=["POST"])
@cross_origin()
def reset():
    global instance
    instance = None
    return jsonify({"connection_status": "Reset"}), 200


@app.route("/updateTable", methods=["POST"])
@cross_origin()
def re_connect():
    data = request.get_json()
    if "SELECT" in data.get("source_query", ""):
        instance.update_tables("source", data.get("source_query"))
    if "SELECT" in data.get("target_query", ""):
        instance.update_tables("target", data.get("target_query"))
    return jsonify({"connection_status": "Re-Connected"}), 200


@app.route('/run_query', methods=['POST'])
@cross_origin()
def run_query():
    req_data = request.json
    query = req_data.get("query")
    if not query:
        return jsonify({"error": "No query provided"}), 400
    try:
        result = execute_query(query)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
