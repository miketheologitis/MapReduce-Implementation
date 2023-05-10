from flask import Flask, request
import os
import pickle
import dill
import base64
from flask import send_file
import io
import requests
from operator import itemgetter
from itertools import groupby, chain
from functools import reduce

from ..zookeeper.zookeeper_client import ZookeeperClient

# Try to find the 1st parameter in env variables which will be set up by docker-compose
# (see the .yaml file), else default to the second.
HOSTNAME = os.getenv('HOSTNAME', 'localhost')
ZK_HOSTS = os.getenv('ZK_HOSTS', 'localhost:2181')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAP_DIR = os.path.join(BASE_DIR, "map_results")
REDUCE_DIR = os.path.join(BASE_DIR, "reduce_results")

# initialization
zk_client = ZookeeperClient(ZK_HOSTS)
app = Flask(__name__)


def save_results_as_pickle(directory, data):
    """
    Save the data as a pickle file in the specified directory.

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


def deserialize_func(encoded_func):
    """
    Deserialize a base64-encoded function.

    :param encoded_func: A base64-encoded representation of a Python function.
    :returns: The deserialized Python function.
    """
    # Convert from Base64-encoded string back to binary
    serialized_func = base64.b64decode(encoded_func.encode('utf-8'))

    # Deserialize the binary data to get the function back
    func = dill.loads(serialized_func)

    return func


def fetch_data_from_workers(locations):
    """
    Fetches intermediate data from the specified workers.

    :param: locations: A list of ('IP:PORT', file_path) tuples representing the
            addresses of the workers and the file paths of the intermediate data.

    :returns: The fetched data as a single list of key-value pairs.
    """
    data = []
    for worker_address, file_path in locations:
        response = requests.get(f'http://{worker_address}/fetch-data', params={'file_path': file_path})

        # Use a BytesIO object as a "file-like" object to load the pickle data
        tmp_data = pickle.loads(io.BytesIO(response.content).read())
        data.extend(tmp_data)

    return data


@app.route('/fetch-data', methods=['GET'])
def fetch_data():
    """
    Endpoint to fetch data stored in a file. This is used by other workers to fetch intermediate data for reduce tasks.
    This endpoint expects a GET request with the following query parameters:

    file_path: str
        The path to the file that is to be fetched.

    An example request might look like this: /fetch-data?file_path=/path/to/file

    :return: The contents of the specified file as a download.
    """

    file_path = request.args.get('file_path')
    return send_file(file_path, as_attachment=True)


# Define the routes and handlers for the worker
@app.route('/map', methods=['POST'])
def map_task():
    """
    Map task route. Receives a map function and input data in JSON format,
    processes the input data using the map function, and saves the output
    data as a pickle file.

    POST request data should be in the following JSON format:
    {
        "map_func": "<serialized_map_func>",
        "data": [
            ("key1", "value1"),
            ("key2", "value2"),
            ...
        ]
    }

    `map_func`: Serialized map function. str (base64 encoded serialized function)

    `data`: Input data on which map function is to be applied.

    :return: The file path of the saved pickle file.
    """

    zk_client.update_worker_state(HOSTNAME, 'in-progress')

    data = request.get_json()

    # Deserialize the reduce function
    map_func = deserialize_func(data['map_func'])

    # Extract the input data from the POST request data
    input_data = data['data']

    # Apply the map function to each input key-value pair, and flatten the results into a single list
    output_data = list(chain.from_iterable(map(lambda x: map_func(*x), input_data)))

    # Save the output data as a pickle file
    file_path = save_results_as_pickle(MAP_DIR, output_data)

    zk_client.update_worker_state(HOSTNAME, 'completed', file_path)

    # Return the file path of the saved pickle file
    return file_path


@app.route('/reduce', methods=['POST'])
def reduce_task():
    """
    Reduce task route.

    POST request data should be in the following JSON format:
    {
        "reduce_func": "<serialized_reduce_func>",
        "file_locations": [
            ("<IP1>:<PORT1>", "file1"),
            ("<IP2>:<PORT2>", "file2"),
            ...
        ]
    }

    `reduce_func`: Serialized reduce function, str (base64 encoded serialized function)

    `file_locations`: List of locations where the intermediate data files are stored.

    :return: The file path of the saved pickle file.
    """
    zk_client.update_worker_state(HOSTNAME, 'in-progress')

    # Load the request data
    data = request.get_json()

    # Deserialize the worker_id reduce function
    reduce_func = deserialize_func(data['reduce_func'])

    # Fetch the data from the specified workers
    data = fetch_data_from_workers(data['file_locations'])

    # Group the data by key
    grouped_data = groupby(data, key=itemgetter(0))

    # Apply the reduce function to each group and collect the results
    # Iterate over each group and apply the reduce function to the values of that group
    # The result of reduce function applied to the values of each group is stored as a tuple of (key, result)
    reduce_results = [
        (key, reduce(reduce_func, map(itemgetter(1), group)))
        for key, group in grouped_data
    ]

    # Save the results to a file and return the file path
    file_path = save_results_as_pickle(REDUCE_DIR, reduce_results)

    zk_client.update_worker_state(HOSTNAME, 'completed', file_path)

    return file_path


if __name__ == '__main__':

    # Create the worker z-node in Zookeeper
    zk_client.register_worker(HOSTNAME)

    app.run(host=HOSTNAME, port=5000)
