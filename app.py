from flask import Flask, request, jsonify, render_template
from flask_cors import CORS   # ✅ Add this line

app = Flask(__name__)
CORS(app)  # ✅ Add this line

# --- Route 1: Show the booking form ---
@app.route('/')
def home():
    # This displays your booking.html page
    return render_template('booking.html')


# --- Route 2: Handle booking submissions ---
@app.route('/book', methods=['POST'])
def book():
    # Get the data from the form (sent as JSON)
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
    # host="0.0.0.0" lets it work online later too
    app.run(host="0.0.0.0", port=5000, debug=True)
