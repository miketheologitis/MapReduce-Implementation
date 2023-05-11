import unittest
import os
import dill
import unittest
import base64
import pickle
import random
from unittest.mock import patch, call
import io
import subprocess
import time
from src.workers.worker import save_results_as_pickle, fetch_data_from_workers,\
    app, MAP_DIR, REDUCE_DIR


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

    def tearDown(self):
        # Clean up the temporary `.pickle` files created during the tests
        for file in os.listdir(MAP_DIR):
            os.remove(os.path.join(MAP_DIR, file))
        for file in os.listdir(REDUCE_DIR):
            os.remove(os.path.join(REDUCE_DIR, file))

    def test_fetch_data(self):
        """
        Unit test for the `fetch_data` endpoint in the `worker` module.
        This function tests the case where a GET request is sent to the
        `fetch_data` endpoint with a valid file path.
        """
        # Define the test data and save it to a file using `save_results_as_pickle`
        test_data = [("key1", "value1"), ("key2", "value2"), ("key3", [{"hi": 2}, 2, 3])]
        temp_file_path = save_results_as_pickle(MAP_DIR, test_data)

        # Send a GET request to the `fetch_data` endpoint with the
        # path of the temporary file as a parameter
        response = self.client.get('/fetch-data', query_string={'file_path': temp_file_path})

        # Check the response status code
        self.assertEqual(response.status_code, 200)

        # Load the data from the response
        response_data = pickle.loads(io.BytesIO(response.data).read())

        # Close the response to avoid a ResourceWarning
        response.close()

        # Check that the data matches the test data
        self.assertEqual(response_data, test_data)

    @patch('src.workers.worker.zk_client.update_worker_state')
    def test_map_task_creates_files(self, mock_update_worker_state):
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

    @patch('src.workers.worker.zk_client.update_worker_state')
    def _test_map_task_helper(self, input_data, map_func, mock_update_worker_state):
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

    def test_reduce_task1(self):
        # Mock fetch_data_from_workers to return the list of key-value pairs
        fetched_data_from_workers = [('key1', 1), ('key1', 2), ('key2', 1)]

        # Data after performing reduce with `reduce_func`
        correct_result_data = [('key1', 3), ('key2', 1)]

        self._test_reduce_task_helper(fetched_data_from_workers, lambda x, y: x+y, correct_result_data)

    def test_reduce_task2(self):
        # Mock fetch_data_from_workers to return the list of key-value pairs

        val1 = [[1], 5, '2', {'x': 5}]
        val2 = [['mike'], 5, '2', {5: '5'}]
        val3 = [1, 2, 3]
        val4 = ['?', '*']

        fetched_data_from_workers = [
            ('key1', val1), ('key1', val2), ('key2', val3), ('key2', val4)
        ]

        def reduce_func(x, y):
            # concat lists
            return [*x, *y]

        correct_result_data = [('key1', [*val1, *val2]), ('key2', [*val3, *val4])]
        self._test_reduce_task_helper(fetched_data_from_workers, reduce_func, correct_result_data)

    @patch('src.workers.worker.zk_client.update_worker_state')
    @patch('src.workers.worker.fetch_data_from_workers')
    def _test_reduce_task_helper(self, fetched_data_from_workers, reduce_func, correct_result_data,
                                 mock_fetch, mock_update_worker_state):

        # Mock fetch_data_from_workers to return the list of key-value pairs
        mock_fetch.return_value = fetched_data_from_workers

        # Serialize the reduce function
        serialized_reduce_func = base64.b64encode(dill.dumps(reduce_func)).decode('utf-8')

        # Define the file locations
        file_locations = [('localhost:5000', 'file1.pickle'), ('localhost:5001', 'file2.pickle')]

        # Send the POST request
        response = self.client.post('/reduce', json={
            'reduce_func': serialized_reduce_func,
            'file_locations': file_locations
        })

        # Check the response
        self.assertEqual(response.status_code, 200)

        # The response data is the path of the result file, so we can load it and check the results
        result_file_path = response.data.decode()

        with open(result_file_path, 'rb') as f:
            result_data = pickle.load(f)

        # Check that the result data is correct
        self.assertEqual(result_data, correct_result_data)

    @patch('requests.get')
    def test_fetch_data_from_workers(self, mock_get):
        """
        Unit test for the `fetch_data_from_workers` function in the `worker` module.
        This function tests the case where two different locations are provided and
        the corresponding workers return some data.

        The `patch` decorator is used to mock the `requests.get` function. This allows
        us to simulate the behavior of `requests.get` in a controlled way without
        actually making HTTP requests.

        :param mock_get: A mock object which replaces `requests.get` for the duration
                         of the test. This object is automatically provided by the
                         `patch` decorator.
        """
        # Define the mock response
        # `mock_get.return_value` represents the response that `requests.get` would
        # normally return. We set its `content` attribute to the pickled version of
        # our test data, simulating a worker that returns this data when requested.
        mock_response = mock_get.return_value
        mock_response.content = pickle.dumps([("key1", "value1"), ("key2", "value2")])

        # Call the function with sample data
        # We provide two file locations to `fetch_data_from_workers`. In a real-world
        # scenario, these would represent the addresses and file paths of two different
        # workers.
        file_locations = [("localhost:5000", "path1"), ("localhost:5001", "path2")]
        result = fetch_data_from_workers(file_locations)

        # Check the result
        # Because we simulate two workers that both return `mock_response.content` the
        # expected result is the following
        expected_result = [('key1', 'value1'), ('key2', 'value2'), ('key1', 'value1'), ('key2', 'value2')]
        self.assertEqual(result, expected_result)

        # Check that requests.get was called with the correct arguments
        # We expect `requests.get` to be called twice, once for each worker.
        # We use `assert_has_calls` to ensure that these calls were made with the
        # correct arguments. The `any_order` parameter allows the calls to occur
        # in any order.
        expected_calls = [call('http://localhost:5000/fetch-data', params={'file_path': 'path1'}),
                          call('http://localhost:5001/fetch-data', params={'file_path': 'path2'})]
        mock_get.assert_has_calls(expected_calls, any_order=True)


if __name__ == '__main__':
    unittest.main()
