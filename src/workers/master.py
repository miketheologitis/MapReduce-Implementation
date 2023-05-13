from typing import NamedTuple, List
from flask import Flask, request
import os

from ..zookeeper.zookeeper_client import ZookeeperClient
from ..hadoop.hdfs_client import HdfsClient

# Try to find the 1st parameter in env variables which will be set up by docker-compose
# (see the .yaml file), else default to the second.
HOSTNAME = os.getenv('HOSTNAME', 'localhost')
ZK_HOSTS = os.getenv('ZK_HOSTS', '127.0.0.1:2181')

app = Flask(__name__)


class Master:
    def __init__(self):
        self.zk_client = None
        self.hdfs_client = None

    def get_zk_client(self):
        if self.zk_client is None:
            self.zk_client = ZookeeperClient(ZK_HOSTS)
        return self.zk_client

    def get_hdfs_client(self):
        if self.hdfs_client is None:
            self.hdfs_client = HdfsClient()
        return self.hdfs_client

    def add_map_reduce_job(self):
        """
        Map task route. Receives a map function and input data in JSON format,
        processes the input data using the map function, and saves the output
        data as a pickle file.

        POST request data should be in the following JSON format:
        {
            "map_func": "<serialized_map_func>",
            "reduce_func": "<serialized_reduce_func>",
            "data": [ (X,...,...), (Y, ... , ...), ... ],
            "num_workers": int
        }

        `map_func`: Serialized map function. str (base64 encoded serialized function)
        `reduce_func`: Serialized reduce function, str (base64 encoded serialized function)
        `data`: Input data on which map function is to be applied.
        `num_workers`: Requested of workers in the map task
        :return: status `ok`
        """
        zk_client = self.get_zk_client()
        zk_client.update_master_state(HOSTNAME, 'in-job')

        req_data = request.get_json()

        data = req_data['data']
        reduce_func = req_data['reduce_func']
        num_workers = req_data['num_workers']

        # Continue from here

    def run(self):
        zk_client = self.get_zk_client()
        zk_client.register_master(HOSTNAME)
        app.run(host='0.0.0.0', port=5000)

    @staticmethod
    def split_data(lst, n):
        """
        Split data into `n` chunks.

        :param lst: The list to be split.
        :param n: The number of chunks to create.
        :returns: A generator that yields chunks of data from the original list.
        """
        for i in range(0, len(lst), n):
            # Extract a chunk of data from the original list
            yield lst[i:i + n]


# Create a singleton instance of Master
master = Master()


@app.route('/submit-map-reduce-job', methods=['POST'])
def add_map_reduce_job():
    return master.add_map_reduce_job()


if __name__ == '__main__':
    master.run()

