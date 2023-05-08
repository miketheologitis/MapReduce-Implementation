import unittest
import os
import dill
import unittest
import base64
import pickle
import random
from src.workers.worker import app, MAP_DIR


class TestWorker(unittest.TestCase):
    """
    This class contains unit tests for the `worker` module.
    """

    def setUp(self):
        """
        Set up the test client for the Flask app.
        """
        app.testing = True
        self.client = app.test_client()

    def test_map_task_creates_files(self):
        """
        Test that the `map_task` creates the expected output files.
        """
        input_data = [("key1", 1), ("key2", 2), ("key3", 3)]

        # Serialize the mapper function using `dill`
        serialized_map_func = dill.dumps(lambda key, value: [(key, value)])  # Binary

        # Convert to Base64-encoded format. Base64 encoding is a technique
        # that takes binary data and represents it as an ASCII string. Also,
        # using decode() method by default converts the bytes to a string using
        # the UTF-8 encoding.
        encoded_map_func = base64.b64encode(serialized_map_func).decode("utf-8")

        for _ in range(100):
            # Send the request to the worker
            response = self.client.post('/map', json={
                'map_func': encoded_map_func,
                'data': input_data
            })
            # Check the response status
            self.assertEqual(response.status_code, 200)

            # Get the output file path from the response
            output_file_path = response.data.decode('utf-8')

            # Assert that the output file exists
            self.assertTrue(os.path.exists(output_file_path))

    def test_map_task_1(self):
        """
        Test that the `map_task` works correctly with a simple map function.
        """
        # Test data and mapper function
        input_data = [("key1", 1), ("key2", 2), ("key3", 3)]

        def map_func(key, value):
            return [(key, value * 2)]

        self._test_map_task_helper(input_data, map_func)

    def test_map_task_2(self):
        """
        Test that the `map_task` works correctly with a lambda function as the map function.
        """
        input_data = [("key1", 1), ("key2", 2), ("key3", 3)]
        self._test_map_task_helper(input_data, lambda key, value: [(key, value * 2)])

    def test_map_task_3(self):
        """
        Test that the `map_task` works correctly with a more complex map function.
        """
        input_data = [("key1", 1), ("key2", 2), ("key3", 3)]

        def map_func(key, value):
            result = []
            for i in range(value):
                result.append((key, i * 2))
            return result

        self._test_map_task_helper(input_data, map_func)

    def test_map_task_4(self):
        """
        Test the map_task method with a large amount of input data.

        This test generates a large list of input data where each key-value pair
        has a key that starts with "key" and a random list of integers as the value.
        """

        def random_number_list(n):
            """Generate a list of n random integers between 0 and 10000."""
            return [random.randint(0, 10000) for _ in range(n)]

        # Generate 1000 key-value pairs where the value is a random list of integers
        input_data = [(f"key{i}", random_number_list(random.randint(0, 100))) for i in range(1000)]

        def map_func(key, value):
            """Map function that sums a list of integers and generates two additional key-value pairs."""
            return [
                (key, sum(value)),
                (key, [type(value), len(value)]),
                ([type(value), len(value)], [type(value), len(value)])
            ]

        # Call the _test_map_task_helper method with the input data and mapper function
        self._test_map_task_helper(input_data, map_func)

    def _test_map_task_helper(self, input_data, map_func):
        """
        Helper method to test the map task of the worker.

        :param input_data: A list of key-value pairs representing the input data to be processed by the map function.
        :param map_func: The map function to be applied to the input data.
        :return: None.
        """

        # Serialize the mapper function using `dill`
        serialized_map_func = dill.dumps(map_func)  # Binary data

        # Convert to Base64-encoded format. Base64 encoding is a technique
        # that takes binary data and represents it as an ASCII string. Also,
        # using decode() method by default converts the bytes to a string using
        # the UTF-8 encoding.
        encoded_map_func = base64.b64encode(serialized_map_func).decode("utf-8")

        # Send the request to the worker
        response = self.client.post('/map', json={
            'map_func': encoded_map_func,
            'data': input_data
        })

        # Check the response status
        self.assertEqual(response.status_code, 200)

        # Get the output file path from the response
        output_file_path = response.data.decode('utf-8')

        # Check if the content of the output file matches the expected result
        expected_output = [pair for key, value in input_data for pair in map_func(key, value)]
        with open(output_file_path, 'rb') as f:
            output_data = pickle.load(f)

        self.assertEqual(output_data, expected_output)

    def tearDown(self):
        # Clean up the temporary `.pickle` files created during the tests
        for file in os.listdir(MAP_DIR):
            os.remove(os.path.join(MAP_DIR, file))


if __name__ == '__main__':
    unittest.main()
