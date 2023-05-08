import unittest
import os
import dill
import unittest
import base64
import pickle
from src.workers.worker import app, save_results_as_pickle, MAP_DIR


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

        # Check response status code
        self.assertEqual(response.status_code, 200)

        # Check if the output file was created
        output_file_path = response.get_data(as_text=True)
        self.assertTrue(os.path.exists(output_file_path))

        # Check if the content of the output file matches the expected result
        expected_output = [
            ("key1", 2),
            ("key2", 4),
            ("key3", 6)
        ]

        with open(output_file_path, 'rb') as f:
            output_data = pickle.load(f)

        self.assertEqual(output_data, expected_output)

    def tearDown(self):
        # Clean up the temporary JSON files created during the tests
        for file in os.listdir(MAP_DIR):
            os.remove(os.path.join(MAP_DIR, file))


if __name__ == '__main__':
    unittest.main()
