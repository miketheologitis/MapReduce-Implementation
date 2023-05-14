
from flask import Flask, request
import os
import pickle
import dill
import base64
from flask import send_file
import io
import time
import requests
from operator import itemgetter
from itertools import groupby, chain
from functools import reduce

from ..zookeeper.zookeeper_client import ZookeeperClient
from ..hadoop.hdfs_client import HdfsClient

# Try to find the 1st parameter in env variables which will be set up by docker-compose
# (see the .yaml file), else default to the second.
HOSTNAME = os.getenv('HOSTNAME', 'localhost')
ZK_HOSTS = os.getenv('ZK_HOSTS', '127.0.0.1:2181')
HDFS_HOST = os.getenv('HDFS_HOST', 'localhost:9870')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAP_DIR = os.path.join(BASE_DIR, "map_results")
REDUCE_DIR = os.path.join(BASE_DIR, "reduce_results")

app = Flask(__name__)


class Worker:
    def __init__(self):
        self.zk_client = None
        self.hdfs_client = None

    def get_zk_client(self):
        if self.zk_client is None:
            retries = 5
            for _ in range(retries):
                try:
                    self.zk_client = ZookeeperClient(ZK_HOSTS)
                except Exception as e:
                    time.sleep(5)
                    continue
        return self.zk_client

    def get_hdfs_client(self):
        if self.hdfs_client is None:
            retries = 10
            for _ in range(retries):
                try:
                    self.hdfs_client = HdfsClient(HDFS_HOST)
                    self.hdfs_client.hdfs.status('')  # Raises exception
                except Exception as e:
                    time.sleep(5)
                    continue
        return self.hdfs_client

    def map_task(self):
        """
        Map Task. Given `job_id` and `task_id` we can retrieve the task information from HDFS.

        POST request data should be in the following JSON format:
        {
            "job_id": <int>
            "task_id": <int>
        }

        :return: status `ok`
        """
        req_data = request.get_json()
        job_id, task_id = req_data['job_id'], req_data['task_id']

        hdfs_client = self.get_hdfs_client()

        # Retrieve the data/func for this task from HDFS (using `job_id` and `task_id`)
        data = hdfs_client.get_data(f'jobs/job_{job_id}/map_tasks/{task_id}.pickle')
        map_func = hdfs_client.get_func(f'jobs/job_{job_id}/map_func.pickle')

        # Apply the map function to each input key-value pair, and flatten the results into a single list
        output_data = list(chain.from_iterable(map(lambda x: map_func(*x), data)))

        # Save the results to HDFS (using `job_id` and `task_id`)
        hdfs_client.save_data(f'jobs/job_{job_id}/map_results/{task_id}.pickle', output_data)

        # Return OK
        return '', 200

    def shuffle_task(self):
        """
        Shuffle Task. We have a single shuffle task in a MapReduce job so given `job_id`
        we can retrieve the task information from HDFS.

        POST request data should be in the following JSON format:
        {
            "job_id": <int>
        }

        :return: status `ok`
        """
        def shuffle_generator(input_data):
            """
            Generate shuffled key-value pairs from input data.

            :param input_data: List of key-value pairs to be shuffled.
            :type input_data: list(tuple)
            :yield: Shuffled key-value pairs.
            :rtype: tuple
            """
            # Sort input data by key
            input_data.sort(key=itemgetter(0))

            # Group data by key
            grouped_data = groupby(input_data, key=itemgetter(0))

            # Generate shuffled key-value pairs
            for key, group in grouped_data:
                values = [item[1] for item in group]
                yield key, values

        job_id = request.get_json()['job_id']

        hdfs_client = self.get_hdfs_client()

        # Retrieve data from map_results directory
        data = []
        for file in hdfs_client.hdfs.list(f'/jobs/job_{job_id}/map_results'):
            data.extend(hdfs_client.get_data(f'jobs/job_{job_id}/map_results/{file}'))

        # Perform shuffling and save the results
        for i, key_values_tuple in enumerate(shuffle_generator(data)):
            hdfs_client.save_data(f'jobs/job_{job_id}/shuffle_results/{i}.pickle', key_values_tuple)

        # Return OK
        return '', 200

    def reduce_task(self):
        """
        Map Task. Given `job_id` and `task_id` we can retrieve the task information from HDFS.

        POST request data should be in the following JSON format:
        {
            "job_id": <int>
            "task_ids": list(int)
        }

        `task_ids`: A list of integer indices corresponding to the `shuffle_results` files.
            For example, [2,3] --> `shuffle_results/2.pickle` , `shuffle_results/3.pickle`

        :return: status `ok`
        """
        req_data = request.get_json()
        job_id, task_ids = req_data['job_id'], req_data['task_ids']

        hdfs_client = self.get_hdfs_client()

        data = []
        for task_id in task_ids:
            # Retrieve shuffle results for each `task_id`
            # Remember that shuffle results are tuples -> (key, values)
            data.append(hdfs_client.get_data(f'jobs/job_{job_id}/shuffle_results/{task_id}.pickle'))

        reduce_func = hdfs_client.get_func(f'jobs/job_{job_id}/reduce_func.pickle')

        # Apply reduction to the shuffled data
        reduce_results = [
            (key, reduce(reduce_func, values))
            for key, values in data
        ]

        # [1,2,3] -> '1_2_3'
        prefix = '_'.join(map(str, task_ids))

        # Save the reduce results with the corresponding prefix
        hdfs_client.save_data(f'jobs/job_{job_id}/reduce_results/{prefix}.pickle', reduce_results)

        return '', 200

    def run(self):
        zk_client = self.get_zk_client()
        zk_client.register_worker(HOSTNAME)
        app.run(host='0.0.0.0', port=5000)


worker = Worker()


@app.route('/map-task', methods=['POST'])
def map_task():
    return worker.map_task()


@app.route('/reduce-task', methods=['POST'])
def reduce_task():
    return worker.reduce_task()


if __name__ == '__main__':
    worker.run()
