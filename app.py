from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import os
from datetime import datetime

app = Flask(__name__)

# ✅ Allow your connected sites
CORS(app, resources={r"/*": {"origins": [
    "https://payasyounow71.neocities.org",
    "https://booking-app-p8q8.onrender.com",
    "https://andyfast20-sketch.github.io"
]}})

BOOKINGS_FILE = "bookings.txt"
AVAIL_FILE = "availability.txt"


@app.route("/")
def home():
    return "✅ Flask Booking API running. Visit /bookings to manage and /availability to view free slots."


# --- Handle booking submissions ---
@app.route("/book", methods=["POST"])
def book():
    data = request.json
    name = data.get("name")
    time = data.get("time")

    # Save booking
    with open(BOOKINGS_FILE, "a") as f:
        f.write(f"Name: {name}, Time: {time}\n")

    # Remove the booked slot from available times
    if os.path.exists(AVAIL_FILE):
        with open(AVAIL_FILE) as f:
            slots = [line.strip() for line in f if line.strip()]
        if time in slots:
            slots.remove(time)
        with open(AVAIL_FILE, "w") as f:
            f.write("\n".join(slots) + ("\n" if slots else ""))

    return jsonify({"message": f"✅ Booking confirmed for {name} at {time}!"})


# --- Get available times for dropdown ---
@app.route("/availability")
def get_availability():
    if not os.path.exists(AVAIL_FILE):
        return jsonify([])
    with open(AVAIL_FILE) as f:
        times = [line.strip() for line in f if line.strip()]
    return jsonify(times)


# --- Admin page: manage bookings + set available times ---
@app.route("/bookings", methods=["GET", "POST"])
def view_bookings():
    # Add new slot
    if request.method == "POST":
        date = request.form.get("date")
        time = request.form.get("time")
        if date and time:
            try:
                datetime.strptime(date, "%Y-%m-%d")
                slot = f"{date} {time}"
                with open(AVAIL_FILE, "a") as f:
                    f.write(slot + "\n")
            except ValueError:
                pass

    # Read bookings
    bookings = []
    if os.path.exists(BOOKINGS_FILE):
        with open(BOOKINGS_FILE) as f:
            bookings = [line.strip().split(",") for line in f if line.strip()]

    # Read available times
    avail = []
    if os.path.exists(AVAIL_FILE):
        with open(AVAIL_FILE) as f:
            avail = [line.strip() for line in f if line.strip()]

    # --- Pretty Admin Page ---
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <title>Manage Bookings</title>
      <style>
        body {
          font-family: 'Segoe UI', sans-serif;
          background: linear-gradient(135deg, #74ABE2, #5563DE);
          color: white;
          text-align: center;
          padding: 2rem;
        }
        table {
          margin: auto;
          border-collapse: collapse;
          background-color: rgba(255,255,255,0.15);
          box-shadow: 0 4px 10px rgba(0,0,0,0.3);
          border-radius: 12px;
          overflow: hidden;
        }
        th, td { padding: 10px 20px; }
        th { background: rgba(255,255,255,0.25); }
        tr:nth-child(even){ background: rgba(255,255,255,0.1); }
        input, select, button {
          margin-top: 1rem; padding: 10px 15px;
          border: none; border-radius: 6px; font-size: 1rem;
        }
        input[type="date"] { width: 180px; }
        select { width: 100px; }
        button { background: #00C9A7; color: white; cursor: pointer; }
        button:hover { background: #00A387; }
        .section { margin-top: 2rem; }
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

      <div class="section">
        <h2>🗓️ Set Available Times</h2>
        <form method="POST">
          <input type="date" name="date" required>
          <select name="time" required>
            <option value="18:00">6:00 PM</option>
            <option value="20:00">8:00 PM</option>
          </select>
          <button type="submit">Add Slot</button>
        </form>

        {% if avail %}
          <h3>Current Free Slots</h3>
          <table>
            <tr><th>Time</th></tr>
            {% for t in avail %}
            <tr><td>{{ t }}</td></tr>
            {% endfor %}
          </table>
        {% else %}
          <p>No free times set yet</p>
        {% endif %}
      </div>
    </body>
    </html>
    """
    return render_template_string(html, bookings=bookings, avail=avail)


# --- NEW: JSON endpoint for bookings dashboard ---
@app.route("/bookings_json", methods=["GET"])
def get_bookings_json():
    bookings = []
    try:
        with open(BOOKINGS_FILE, "r") as file:
            for line in file:
                line = line.strip()
                if line:
                    # Example: "Name: Fred, Time: 12.44"
                    parts = line.split(",")
                    name = parts[0].split(":")[1].strip() if len(parts) > 0 else ""
                    time = parts[1].split(":")[1].strip() if len(parts) > 1 else ""
                    bookings.append({"name": name, "time": time})
    except FileNotFoundError:
        return jsonify({"bookings": []})
    return jsonify({"bookings": bookings})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
