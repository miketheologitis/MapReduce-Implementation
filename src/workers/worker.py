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
    """
    Save the results as a pickle file in the specified directory.

    :param directory: The directory where the pickle file will be saved.
    :param data: The data to be saved in the pickle file.
    :return: The file path of the saved pickle file.
    """

    # Create the director(y/ies) if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Generate a new unique file name based on the number of existing files in the directory
    file_name = f"{len(os.listdir(directory))}.pickle"
    file_path = os.path.join(directory, file_name)

    # Save the data to the pickle file
    with open(file_path, "wb") as f:
        pickle.dump(data, f)

    # Return the file path of the saved pickle file
    return file_path


# Define the routes and handlers for the worker
@app.route('/map', methods=['POST'])
def map_task():
    """
    Map task route. Receives a map function and input data in JSON format,
    processes the input data using the map function, and saves the output
    data as a pickle file.

    :return: The file path of the saved pickle file.
    """
    data = request.get_json()

    # Extract the encoded map function from the request data
    encoded_map_func = data['map_func']

    # Decode the encoded map function from base64 to bytes
    serialized_map_func = base64.b64decode(encoded_map_func)

    # Deserialize the map function using `dill`
    map_func = dill.loads(serialized_map_func)

    # Extract the input data from the POST request data
    input_data = data['data']

    # Process the input data using the mapper function
    output_data = []
    for key, value in input_data:
        # we use `extend` instead of `append` because the map function is expected to return
        # a list of key-value pairs for each input key-value pair it processes.
        output_data.extend(map_func(key, value))

    # Save the output data as a pickle file
    file_path = save_results_as_pickle(MAP_DIR, output_data)

    # Return the file path of the saved pickle file
    return file_path


@app.route('/reduce', methods=['POST'])
def reduce_task():
    # Handle the reducer task
    pass


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
