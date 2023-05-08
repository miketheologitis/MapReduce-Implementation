from flask import Flask, request, jsonify
import dill


app = Flask(__name__)


# Define the routes and handlers for the worker
@app.route('/map', methods=['POST'])
def map_task():
    data = request.get_json()
    serialized_mapper = data['mapper']
    input_data = data['data']

    # Deserialize the mapper function
    mapper = dill.loads(serialized_mapper)

    # Process the input data using the mapper function
    output_data = []
    for key, value in input_data:
        # we use `extend` instead of `append` because the mapper function is expected to return
        # a list of key-value pairs for each input key-value pair it processes.
        output_data.extend(mapper(key, value))

    # Return the result as JSON
    return jsonify(output_data)


@app.route('/reduce', methods=['POST'])
def reduce_task():
    # Handle the reducer task
    pass


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
