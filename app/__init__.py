# app/__init__.py
# This file contains the application factory. It sets up the core app,
# connects to the database, and registers all the blueprints.
from dotenv import load_dotenv
from flask import Flask, render_template
from pymongo import MongoClient
import os

# This global 'db' variable will be accessible to our blueprints.
db = None

def create_app():
    """
    Creates and configures an instance of the Flask application.
    This pattern is called the 'Application Factory'.
    """
    global db
    
    app = Flask(__name__, instance_relative_config=True)
    

    ENV_PATH = os.path.join(os.path.dirname(__file__), "services", ".env")
    load_dotenv(ENV_PATH) 
    # --- Database Configuration ---
    # It's best practice to use environment variables for sensitive data.
    MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
    client = MongoClient(MONGO_URI)
    # Assign the database connection to our global 'db' variable.
    db = client.legal_dashboard_db

    # The app_context is necessary for Flask to work correctly.
    with app.app_context():
        # --- Import and Register Blueprints ---
        
        # Import the 'acts_bp' blueprint from our acts routes file.
        from .acts import routes as acts_routes
        app.register_blueprint(acts_routes.acts_bp, url_prefix='/acts')

       
        from .judgments.routes import judgments_bp
        app.register_blueprint(judgments_bp, url_prefix="/sc")

        from .tribunals import routes as tribunals_routes
        app.register_blueprint(tribunals_routes.tribunals_bp, url_prefix='/tribunals')

        from .districtcourt import routes as districtcourt_routes
        app.register_blueprint(districtcourt_routes.districtcourt_bp, url_prefix='/districtcourt')

        # app/__init__.py
        # app/__init__.py  (inside create_app, after you construct app)
        #from app.services.summarize import summary_bp
        #app.register_blueprint(summary_bp, url_prefix="/summary")

        from app.services.ner import ner_bp
        app.register_blueprint(ner_bp, url_prefix="/services/ner")

        from .high_courts.routes import high_courts_bp
        app.register_blueprint(high_courts_bp, url_prefix="/high_courts")


        from .resolver import resolver_bp
        app.register_blueprint(resolver_bp)

        @app.route('/')
        def home():
            """Renders the main homepage."""
            return render_template('index.html')
        

    return app
