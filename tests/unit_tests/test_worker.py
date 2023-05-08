import unittest
from src.workers.worker import app, save_results_as_json, MAP_DIR
import os
import json
import dill
import unittest
import tempfile
import base64


class TestWorker(unittest.TestCase):

    def setUp(self):
        app.testing = True
        self.client = app.test_client()

    def test_map_task(self):
        # Test data and mapper function
        input_data = [("key1", 1), ("key2", 2), ("key3", 3)]

        def map_func(key, value):
            return [(key, value * 2)]

        # Serialize the mapper function using `dill`
        serialized_mapper = dill.dumps(map_func)  # Binary data

        # Convert to Base64-encoded format. Base64 encoding is a technique
        # that takes binary data and represents it as an ASCII string. Also,
        # using decode() method by default converts the bytes to a string using
        # the UTF-8 encoding.
        encoded_mapper = base64.b64encode(serialized_mapper).decode("utf-8")

        # Send the request to the worker
        response = self.client.post('/map', json={
            'map_func': encoded_mapper,
            'data': input_data
        })

        print(response)

    def tearDown(self):
        # Clean up the temporary JSON files created during the tests
        for file in os.listdir(MAP_DIR):
            os.remove(os.path.join(MAP_DIR, file))


if __name__ == '__main__':
    unittest.main()
