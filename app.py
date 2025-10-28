from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS

app = Flask(__name__)

# ✅ Allow your sites
CORS(app, resources={r"/*": {"origins": [
    "https://payasyounow71.neocities.org",
    "https://booking-app-p8q8.onrender.com",
    "https://andyfast20-sketch.github.io"
]}})

# --- Booking Form (already handled from GitHub Pages via /book endpoint) ---
@app.route('/')
def home():
    return "✅ Flask Booking API is running! Visit /bookings to view saved bookings."

# --- Handle booking submissions ---
@app.route('/book', methods=['POST'])
def book():
    data = request.json
    name = data.get('name')
    time = data.get('time')

    with open("bookings.txt", "a") as file:
        file.write(f"{name},{time}\n")

    return jsonify({"message": f"✅ Booking confirmed for {name} at {time}!"})

# --- View all bookings (pretty UI) ---
@app.route('/bookings')
def view_bookings():
    try:
        with open("bookings.txt", "r") as file:
            lines = file.readlines()
    except FileNotFoundError:
        lines = []

    bookings = [line.strip().split(",") for line in lines if line.strip()]

    # 🖌️ Inline HTML template with CSS
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>View Bookings</title>
        <style>
            body {
                font-family: 'Segoe UI', sans-serif;
                background: linear-gradient(135deg, #74ABE2, #5563DE);
                color: white;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
                min-height: 100vh;
                margin: 0;
            }
            h1 {
                text-shadow: 2px 2px 10px rgba(0,0,0,0.3);
            }
            table {
                background-color: rgba(255,255,255,0.15);
                border-collapse: collapse;
                border-radius: 15px;
                overflow: hidden;
                width: 80%;
                max-width: 600px;
                box-shadow: 0 4px 10px rgba(0,0,0,0.3);
            }
            th, td {
                padding: 15px;
                text-align: center;
            }
            th {
                background-color: rgba(255,255,255,0.3);
                color: #fff;
                font-size: 1.1rem;
            }
            tr:nth-child(even) {
                background-color: rgba(255,255,255,0.05);
            }
            tr:hover {
                background-color: rgba(0,0,0,0.2);
                transition: 0.3s;
            }
            a {
                color: #00FFB3;
                margin-top: 20px;
                text-decoration: none;
                font-weight: bold;
            }
            a:hover {
                text-decoration: underline;
            }
        </style>
    </head>
    <body>
        <h1>📘 Current Bookings</h1>
        {% if bookings %}
        <table>
            <tr><th>Name</th><th>Time</th></tr>
            {% for name, time in bookings %}
            <tr><td>{{ name }}</td><td>{{ time }}</td></tr>
            {% endfor %}
        </table>
        {% else %}
        <p>No bookings yet 😅</p>
        {% endif %}
        <a href="/">⬅ Back to Home</a>
    </body>
    </html>
    """

    return render_template_string(html_template, bookings=bookings)


# --- Run the Flask app ---
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
