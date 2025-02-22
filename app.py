import logging
from flask import Flask

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)

@app.route('/')
def home():
    logging.info("Home endpoint accessed")
    return 'Diamond Data Application is running.'

@app.errorhandler(Exception)
def handle_error(error):
    logging.error(f"An error occurred: {str(error)}")
    return str(error), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
