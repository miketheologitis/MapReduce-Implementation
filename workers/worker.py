from flask import Flask, request, jsonify

app = Flask(__name__)


# Define the routes and handlers for the worker
@app.route('/map', methods=['POST'])
def map_task():
    # Handle the mapper task
    pass


@app.route('/reduce', methods=['POST'])
def reduce_task():
    # Handle the reducer task
    pass


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
