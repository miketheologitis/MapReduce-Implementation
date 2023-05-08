from flask import Flask, request
import os
import pickle
import dill
import base64

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAP_DIR = os.path.join(BASE_DIR, "data/map_results")
REDUCE_DIR = os.path.join(BASE_DIR, "data/reduce_results")

app = Flask(__name__)


def save_results_as_pickle(directory, data):
    if not os.path.exists(directory):
        os.makedirs(directory)

    file_name = f"{len(os.listdir(directory))}.pickle"
    file_path = os.path.join(directory, file_name)

    # Save the data to the JSON file
    with open(file_path, "wb") as f:
        pickle.dump(data, f)

    return file_path


# Define the routes and handlers for the worker
@app.route('/map', methods=['POST'])
def map_task():
    data = request.get_json()

    encoded_map_func = data['map_func']
    serialized_map_func = base64.b64decode(encoded_map_func)
    map_func = dill.loads(serialized_map_func)

    input_data = data['data']

    # Process the input data using the mapper function
    output_data = []
    for key, value in input_data:
        # we use `extend` instead of `append` because the map function is expected to return
        # a list of key-value pairs for each input key-value pair it processes.
        output_data.extend(map_func(key, value))

    file_path = save_results_as_pickle(MAP_DIR, output_data)

    return file_path


@app.route('/reduce', methods=['POST'])
def reduce_task():
    # Handle the reducer task
    pass


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
