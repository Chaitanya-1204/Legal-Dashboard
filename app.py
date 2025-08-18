# app.py
# Import the Flask class from the flask library, and the render_template function.
from flask import Flask, render_template
import os

# Create an instance of the Flask class. 
# __name__ is a special Python variable that gets the name of the current module.
# Flask uses this to know where to look for resources like templates and static files.
app = Flask(__name__)

# --- Database Configuration (Placeholder for future use) ---
# In a real application, you would connect to your MongoDB database here.
# For now, this part is commented out but shows where you would add it.
# from pymongo import MongoClient
# MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
# client = MongoClient(MONGO_URI)
# db = client.legal_dashboard_db


# --- Flask Routes ---

# Use the @app.route decorator to tell Flask what URL should trigger our function.
# The '/' URL is the root or homepage of the site.
@app.route('/')
def home():
    """
    This function will run when a user navigates to the homepage.
    It uses render_template to find and return the 'index.html' file.
    Flask will automatically look for this file in a folder named 'templates'.
    """
    return render_template('index.html')

# --- Main Entry Point ---

# This is a standard Python construct. 
# The code inside this 'if' block will only run when the script is executed directly
# (not when it's imported as a module into another script).
if __name__ == '__main__':
    # The app.run() method starts Flask's built-in development web server.
    # debug=True enables debug mode, which provides helpful error messages
    # and automatically reloads the server whenever you save a change to the file.
    # port=5000 specifies which port the server should listen on.
    app.run(debug=True, port=5001)
