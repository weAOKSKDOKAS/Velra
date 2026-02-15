from flask import Flask, send_from_directory, make_response, jsonify, request
import os
from datetime import datetime

app = Flask(__name__)

def log_request(response):
    ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {ip} - {request.method} {request.path} {response.status_code} - {request.user_agent}")
    return response

@app.after_request
def after_request(response):
    return log_request(response)

@app.route('/')
def index():
    # If using vite build, files are usually in dist/
    # If running simple setup, files are in root
    return send_from_directory('.', 'index.html')


# Serve static files from project root (bg images, icons, etc.)
# Flask's default /static is not used here because this repo keeps assets at root.
@app.route('/<path:filename>')
def static_files(filename):
    # Prevent shadowing API endpoints
    if filename in {"data.json", "health"} or filename.startswith("internal/"):
        return jsonify({"error": "Not Found"}), 404

    # Only serve files that exist in the project directory
    safe_path = os.path.join(os.getcwd(), filename)
    if not os.path.isfile(safe_path):
        return jsonify({"error": "Not Found"}), 404

    return send_from_directory('.', filename)

@app.route('/data.json')
def data():
    path = os.path.join(os.getcwd(), 'data.json')
    if not os.path.exists(path):
        return jsonify({
            "error": "Data file not found on server",
            "missing_file": True,
            "path_checked": path,
            "suggestion": "Wait for backend_worker.py to generate the initial snapshot."
        }), 404
        
    response = make_response(send_from_directory('.', 'data.json'))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

@app.route('/health')
def health():
    return jsonify({"ok": True, "timestamp": datetime.now().isoformat()})

if __name__ == '__main__':
    # Cloud Run provides PORT env var; default to 5000 for local dev
    port = int(os.environ.get("PORT", 5000))
    print(f"Server starting on 0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port)
