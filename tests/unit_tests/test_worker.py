import unittest
import random
from unittest.mock import patch, call, create_autospec
from src.zookeeper.zookeeper_client import ZookeeperClient
from src.hadoop.hdfs_client import HdfsClient
from hdfs import InsecureClient
from src.workers.worker import app, worker


class TestWorker(unittest.TestCase):
    """
    This class contains unit tests for the `worker` module.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.mock_zk_client = create_autospec(ZookeeperClient)
        cls.mock_hdfs_client = create_autospec(HdfsClient)
        cls.mock_hdfs = create_autospec(InsecureClient)
        cls.mock_hdfs_client.hdfs = cls.mock_hdfs

        # Mock the update_worker_state method on the ZookeeperClient
        cls.mock_zk_client.update_worker.return_value = None
        cls.mock_zk_client.update_task.return_value = None

        cls.mock_hdfs_client.save_data.return_value = None

        app.testing = True
        cls.client = app.test_client()
        worker.get_zk_client = lambda: cls.mock_zk_client
        worker.get_hdfs_client = lambda: cls.mock_hdfs_client

    def test_map_task_1(self):
        """
        Test that the `map_task` works correctly with a simple map function.
        """
        # Test data and mapper function
        input_data = [("key1", 1), ("key2", 2), ("key3", 3)]

        def map_func(data):
            return [(key, value * 2) for key, value in data]

        self._test_map_task_helper(input_data, map_func)

    def test_map_task_2(self):
        """
        Test that the `map_task` works correctly with a lambda function as the map function.
        """
        input_data = [("key1", 1), ("key2", 2), ("key3", 3)]
        self._test_map_task_helper(input_data, lambda data: [(key, value * 2) for key, value in data])

    def test_map_task_3(self):
        """
        Test that the `map_task` works correctly with a more complex map function.
        """
        input_data = [("key1", 1), ("key2", 2), ("key3", 3)]

        def map_func(data):
            result = []
            for key, value in data:
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

        def map_func(data):
            """Map function that sums a list of integers and generates two additional key-value pairs."""
            result = []

            for key, value in data:
                result.extend([
                    (key, sum(value)),
                    (key, [type(value), len(value)]),
                    ([type(value), len(value)], [type(value), len(value)])
                ])
            return result

        # Call the _test_map_task_helper method with the input data and mapper function
        self._test_map_task_helper(input_data, map_func)

    def test_map_task_5(self):
        """
        Test that the map_task method can handle single elements

        Input:
            [("mike",), ("george",), ("123",)]
        Result:
            [('m', 1), ('i', 1), ('k', 1), ('e', 1), ('g', 1), ('e', 1),
            ('o', 1), ('r', 1), ('g', 1), ('e', 1), ('1', 1), ('2', 1),
            ('3', 1)]

        """
        input_data = [("mike",), ("george",), ("123",)]

        def map_func(data):
            tmp_func = lambda x: map(lambda letter: (letter, 1), x)
            result = []
            for x in data:
                result.extend(tmp_func(x))
        self._test_map_task_helper(input_data, map_func)

    def _test_map_task_helper(self, input_data, map_func):
        """
        Helper method to test the map task of the worker.

        :param input_data: A list of key-value pairs representing the input data to be processed by the map function.
        :param map_func: The map function to be applied to the input data.
        :return: None.
        """

        self.mock_hdfs_client.get_data.return_value = input_data
        self.mock_hdfs_client.get_func.return_value = map_func

        # Send the request to the worker
        response = self.client.post('/map-task', json={'job_id': 1, 'task_id': 1})

        # Check the response status
        self.assertEqual(response.status_code, 200)

        args, _ = self.mock_hdfs_client.save_data.call_args

        # Retrieve the second argument
        output_data = args[1]

        # Check if the content of the output file matches the expected result
        expected_output = map_func(input_data)

        self.assertEqual(output_data, expected_output)

    def test_shuffle_task1(self):
        data = [
            ('key1', 1), ('key1', 2), ('key1', {12: '3'}), ('key1', 4), ('key1', 5), ('key1', 6), ('key1', 7),
            ('key2', 8), ('key2', 9), ('key2', 10), ('key2', {11: '5'}), ('key2', 12), ('key2', 13),
            ('key2', 14), ('key2', [21, 22, 23, 25, 26, 27]), ('key6', 1)
        ]
        correct_output = [
            ('key1', [1, 2, {12: '3'}, 4, 5, 6, 7]),
            ('key2', [8, 9, 10, {11: '5'}, 12, 13, 14, [21, 22, 23, 25, 26, 27]]),
            ('key6', [1])
        ]

        self._test_shuffle_task(data, correct_output)

    def _test_shuffle_task(self, input_data, correct_output):
        self.mock_hdfs_client.get_data.return_value = input_data
        self.mock_hdfs.list.return_value = [1]

        # Send the POST request
        response = self.client.post('/shuffle-task', json={'job_id': 1})

        # Check the response
        self.assertEqual(response.status_code, 200)

        call_args_list = self.mock_hdfs_client.save_data.call_args_list[-2:]

        output_data = []
        for call_args in call_args_list:
            args, _ = call_args
            output_data.append(args[1])

        # Check that the result data is correct
        self.assertEqual(output_data, correct_output)

    def test_reduce_task1(self):
        # Mock fetch_data_from_workers to return the list of key-value pairs
        input_data = ('key1', [1, 2, 3, 4, 5, 6])

        # Data after performing reduce with `reduce_func`
        correct_result_data = [('key1', 21)]

        self._test_reduce_task_helper(input_data, sum, correct_result_data)

    def _test_reduce_task_helper(self, input_data, reduce_func, correct_result_data):

        self.mock_hdfs_client.get_data.return_value = input_data

        self.mock_hdfs_client.get_func.return_value = reduce_func

        # Send the POST request
        response = self.client.post('/reduce-task', json={'job_id': 1, 'task_ids': [1]})

        # Check the response
        self.assertEqual(response.status_code, 200)

        args, _ = self.mock_hdfs_client.save_data.call_args

        # Retrieve the second argument
        output_data = args[1]

        # Check that the result data is correct
        self.assertEqual(output_data, correct_result_data)


if __name__ == '__main__':
    unittest.main()
