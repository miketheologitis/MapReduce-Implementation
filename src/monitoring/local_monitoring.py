import time

from ..hadoop.hdfs_client import HdfsClient
from ..zookeeper.zookeeper_client import ZookeeperClient


class LocalMonitoring:

    def __init__(self):
        self.zk_hosts = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
        self.hdfs_host = "localhost:9870"  # namenode

        self.zk_client, self.hdfs_client = None, None

    def get_zk_client(self, retries=10, sleep_sec=5):
        if self.zk_client is None:
            for i in range(retries):
                try:
                    self.zk_client = ZookeeperClient(self.zk_hosts)
                except Exception as e:
                    if i < retries - 1:
                        time.sleep(sleep_sec)
                        continue
                    else:  # raise exception if this was the last retry
                        raise Exception("Could not connect to Zookeeper after multiple attempts") from e
        return self.zk_client

    def get_hdfs_client(self, retries=10, sleep_sec=5):
        if self.hdfs_client is None:
            for i in range(retries):
                try:
                    self.hdfs_client = HdfsClient(self.hdfs_host)
                    self.hdfs_client.hdfs.status('')  # Raises exception
                except Exception as e:
                    if i < retries - 1:
                        time.sleep(sleep_sec)
                        continue
                    else:  # raise exception if this was the last retry
                        raise Exception("Could not connect to Zookeeper after multiple attempts") from e
        return self.hdfs_client

    def get_registered_masters(self):
        """
        Get the list of registered masters and the count of registered masters in Zookeeper.

        :return: Tuple containing the list of registered masters and the count of registered masters.
        """
        masters_registered = self.get_zk_client().zk.get_children('/masters/')
        n_masters_registered = len(masters_registered)
        return masters_registered, n_masters_registered

    def get_registered_workers(self):
        """
        Get the list of registered workers and the count of registered workers in Zookeeper.

        :return: Tuple containing the list of registered workers and the count of registered workers.
        """
        workers_registered = self.get_zk_client().zk.get_children('/workers/')
        n_workers_registered = len(workers_registered)
        return workers_registered, n_workers_registered
