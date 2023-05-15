import time
import subprocess

from ..hadoop.hdfs_client import HdfsClient
from ..zookeeper.zookeeper_client import ZookeeperClient
from ..monitoring.local_monitoring import LocalMonitoring


class LocalCluster:
    def __init__(self, n_workers=None, n_masters=None, initialize=True):
        self.zk_hosts = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
        self.hdfs_host = "localhost:9870"  # namenode

        self.local_monitoring = LocalMonitoring()

        self.zk_client, self.hdfs_client = None, None

        if initialize:
            self.cluster_initialize()

        self.scale(n_workers, n_masters)

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

    def cluster_initialize(self, verbose=False):
        """
        Initialize the local cluster by starting necessary services and creating directories in ZooKeeper
        and HDFS.

        :param verbose: Whether to print verbose output.
        """
        subprocess.run(
            ['docker-compose', 'up', '-d', '--scale', 'worker=0',
             '--scale', 'master=0', '--scale'],
            stdout=subprocess.DEVNULL if not verbose else None,
            stderr=subprocess.DEVNULL if not verbose else None
        )

        zk_client = self.get_zk_client()
        zk_client.setup_paths()

        hdfs_client = self.get_hdfs_client()
        hdfs_client.initialize_job_dir()

    def mapreduce(self, data, map_func, reduce_func):
        """
        Perform MapReduce on the local cluster.

        :param data: Input data for MapReduce.
        :param map_func: Map function.
        :param reduce_func: Reduce function.
        """
        hdfs_client = self.get_hdfs_client()
        zk_client = self.zk_client()

        job_id = zk_client.get_sequential_job_id()
        hdfs_client.job_create(job_id=job_id, data=data, map_func=map_func, reduce_func=reduce_func)

        # TODO: Job create in zookeeper ?

    def scale(self, n_workers=None, n_masters=None, verbose=False):
        """
        Scale the local cluster to the specified number of workers and masters.

        :param n_workers: Number of workers. If not provided, the current number of registered workers will be used.
        :param n_masters: Number of masters. If not provided, the current number of registered masters will be used.
        :param verbose: Whether to print verbose output.
        """
        if not n_workers:
            _, n_workers = self.local_monitoring.get_registered_workers()
        if not n_masters:
            _, n_masters = self.local_monitoring.get_registered_masters()

        subprocess.run(
            ['docker-compose', 'up', '-d', '--scale', f'worker={n_workers}',
             '--scale', f'master={n_masters}', '--no-recreate'],
            stdout=subprocess.DEVNULL if not verbose else None,
            stderr=subprocess.DEVNULL if not verbose else None
        )

    def shutdown_cluster(self, verbose=False):
        """
        Shutdown the local cluster by stopping services and cleaning up resources.

        :param verbose: Whether to print verbose output.
        """
        zk_client = self.get_zk_client()
        zk_client.stop()
        zk_client.close()

        hdfs_client = self.get_hdfs_client()
        hdfs_client.cleanup()

        subprocess.run(
            ['docker-compose', 'down', '-d'],
            stdout=subprocess.DEVNULL if not verbose else None,
            stderr=subprocess.DEVNULL if not verbose else None
        )




