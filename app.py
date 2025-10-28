from flask import Flask, request, jsonify, render_template
from flask_cors import CORS   # ✅ Import stays the same

app = Flask(__name__)

# ✅ Updated CORS rule: only allow requests from your Neocities site
CORS(app, resources={r"/*": {"origins": ["https://payasyounow71.neocities.org", "https://booking-app-p8q8.onrender.com"]}})



# --- Route 1: Show the booking form ---
@app.route('/')
def home():
    return render_template('booking.html')

# --- Route 2: Handle booking submissions ---
@app.route('/book', methods=['POST'])
def book():
    data = request.json
    name = data.get('name')
    time = data.get('time')

    # Save booking details to a text file
    with open("bookings.txt", "a") as file:
        file.write(f"Name: {name}, Time: {time}\n")

    # Send a message back to the web page
    return jsonify({"message": f"✅ Booking confirmed for {name} at {time}!"})

# --- Run the Flask app ---
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)








