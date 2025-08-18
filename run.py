# run.py
# This is the main entry point for your application.
# To start the server, you will run 'python run.py' in your terminal.

from app import create_app

# Create an instance of the Flask application using our factory function
# defined in app/__init__.py.
app = create_app()

if __name__ == '__main__':
    # Run the app using Flask's built-in development server.
    # debug=True enables auto-reloading when you save changes and provides
    # detailed error pages in the browser.
    app.run(debug=True, port=5001)
