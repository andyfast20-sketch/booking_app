from flask import Flask, render_template, request, jsonify
import sys

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/book", methods=["POST"])
def book():
    data = request.json
    print("Received booking:", data)
    return jsonify({"status": "success", "message": "Booking received"})

if __name__ == "__main__":
    # Allow dynamic port from the Server Manager GUI
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5002
    print(f"Running Booking App on port {port}")
    app.run(host="0.0.0.0", port=port, debug=True)

