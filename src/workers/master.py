from typing import NamedTuple, List
from flask import Flask, request
import os
import time

from ..zookeeper.zookeeper_client import ZookeeperClient
from ..hadoop.hdfs_client import HdfsClient


# Try to find the 1st parameter in env variables which will be set up by docker-compose
# (see the .yaml file), else default to the second.
HOSTNAME = os.getenv('HOSTNAME', 'localhost')
ZK_HOSTS = os.getenv('ZK_HOSTS', '127.0.0.1:2181')
HDFS_HOST = os.getenv('HDFS_HOST', 'localhost:9870')

app = Flask(__name__)


class Task(NamedTuple):
    id: int
    type: str  # 'reduce', 'map', 'reduce'


class Job(NamedTuple):
    id: int
    tasks: List[Task]


class Master:
    def __init__(self):
        self.zk_client = None
        self.hdfs_client = None
        self.jobs = []

    def get_zk_client(self):
        if self.zk_client is None:
            self.zk_client = ZookeeperClient(ZK_HOSTS)
        return self.zk_client

    def get_hdfs_client(self):
        if self.hdfs_client is None:
            self.hdfs_client = HdfsClient(HDFS_HOST)
        return self.hdfs_client

    def get_unique_job_id(self):
        return 0 if not self.jobs else max(job.id for job in self.jobs) + 1

    def add_map_reduce_job(self):
        """
        Map task route. Receives a map function and input data in JSON format,
        processes the input data using the map function, and saves the output
        data as a pickle file.

        POST request data should be in the following JSON format:
        {
            "map_func": "<serialized_map_func>",
            "reduce_func": "<serialized_reduce_func>",
            "hdfs_filename": containing data : [ (X,...,...), (Y, ... , ...), ... ],
            "requested_workers": int
        }

        `map_func`: Serialized map function. str (base64 encoded serialized function)
        `reduce_func`: Serialized reduce function, str (base64 encoded serialized function)
        `hdfs_filename`: The HDFS path of the input data on which map function is to be applied.
        `num_workers`: Requested of workers in the map task
        :return: status `ok`
        """
        #zk_client = self.get_zk_client()
        #zk_client.update_master_state(HOSTNAME, 'in-job')

        hdfs_client = self.get_hdfs_client()
        job_id = self.get_unique_job_id()
        hdfs_client.initialize_job_dirs(job_id)

        req_data = request.get_json()

        map_func = req_data['reduce_func']
        reduce_func = req_data['reduce_func']
        data = req_data['data']
        num_workers = req_data['requested_workers']

        # Continue from here

    def run(self):
        tries = 5
        for _ in range(tries):
            try:
                #zk_client = self.get_zk_client()
                #zk_client.register_master(HOSTNAME)

                hdfs_client = self.get_hdfs_client()
                hdfs_client.status('/')

                break
            except Exception as e:
                time.sleep(5)
                continue

        app.run(host='0.0.0.0', port=5000)

    @staticmethod
    def split_data(data, n):
        """
        Split data into roughly `n` chunks.

        :param data: The list to be split.
        :param n: The number of chunks to create.
        :returns: A generator that yields approximately equal chunks of data from the original list.
        """
        chunk_size = len(data) // n
        remainder = len(data) % n
        start = 0
        for i in range(n):
            chunk_end = start + chunk_size + (1 if i < remainder else 0)
            yield data[start:chunk_end]
            start = chunk_end


# Create a singleton instance of Master
master = Master()


@app.route('/submit-map-reduce-job', methods=['POST'])
def add_map_reduce_job():
    return master.add_map_reduce_job()


if __name__ == '__main__':
    master.run()

