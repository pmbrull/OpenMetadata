#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Local webserver for generating hybrid yamls
"""
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os

from metadata.generated.schema.entity.automations.testServiceConnection import TestServiceConnectionRequest

from models import InitServerModel
from repository import LocalIngestionServer

app = Flask(__name__, static_folder=os.path.join('ui', 'build'), static_url_path='')


CACHE = {}

CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}}, 
     supports_credentials=True, 
     allow_headers="*", 
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])

# Route to save connection configuration
@app.route('/save-config', methods=['POST'])
def save_config():
    data = request.json
    ...
    return jsonify({"message": "Configuration saved successfully!"}), 201

@app.route('/')
def serve_react_app():
    """Route to serve the React app"""
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def serve_static_files(path):
    """Server other static assets like JS, CSS, etc."""
    if os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')


@app.route('/init', methods=['POST'])
def init_local_server():
    """Initialize the local server"""
    init_server = InitServerModel.model_validate(request.json)

    CACHE["server"] = LocalIngestionServer(
        server_url=str(init_server.server_url),
        token=init_server.token
    )

    return jsonify(success=True)

@app.route('/api/test', methods=['POST'])
def _test_connection():
    payload = request.json
    test_conn_req = TestServiceConnectionRequest.model_validate(payload)
    res = CACHE["server"]._test_connection(test_conn_req)
    return jsonify(res.model_dump())


# Start the Flask server
if __name__ == '__main__':
    app.run(debug=True, port=8001)