import time
import subprocess

from ..hadoop.hdfs_client import HdfsClient
from ..zookeeper.zookeeper_client import ZookeeperClient
from ..monitoring.local_monitoring import LocalMonitoring


class LocalCluster:
    def __init__(self, n_workers=None, n_masters=None, initialize=False, verbose=False):
        self.zk_hosts = "localhost:2181,localhost:2182,localhost:2183"
        self.hdfs_host = "localhost:9870"  # namenode

        self.local_monitoring = LocalMonitoring(self.zk_hosts)

        self.zk_client, self.hdfs_client = None, None

        if initialize:
            self.cluster_initialize(verbose=verbose)

        self.scale(n_workers, n_masters, verbose=verbose)

    def get_zk_client(self):
        if self.zk_client is None:
            self.zk_client = ZookeeperClient(self.zk_hosts)
        return self.zk_client

    def get_hdfs_client(self, retries=15, sleep_sec=10, with_init=False):
        if self.hdfs_client is None:
            self.hdfs_client = HdfsClient(self.hdfs_host)
        return self.hdfs_client

    def mapreduce(self, data, map_func, reduce_func):
        """
        Perform MapReduce on the local cluster. Blocks until completion.

        :param data: Input data for MapReduce.
        :param map_func: Map function.
        :param reduce_func: Reduce function.
        """
        hdfs_client = self.get_hdfs_client()
        zk_client = self.get_zk_client()

        job_id = zk_client.get_sequential_job_id()
        hdfs_client.job_create(job_id=job_id, data=data, map_func=map_func, reduce_func=reduce_func)

        self.local_monitoring.wait_for_job_completion(job_id)

        output_data = []
        for filename in hdfs_client.hdfs.list(f'jobs/job_{job_id}/reduce_results/'):
            output_data.extend(hdfs_client.get_data(f'jobs/job_{job_id}/reduce_results/{filename}'))

        return output_data

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

    def cluster_initialize(self, verbose=False):
        """
        Initialize the local cluster by starting necessary services and creating directories in ZooKeeper
        and HDFS.

        :param verbose: Whether to print verbose output.
        """
        subprocess.run(
            ['docker-compose', 'up', '-d', '--scale', 'worker=0',
             '--scale', 'master=0', '--no-recreate'],
            stdout=subprocess.DEVNULL if not verbose else None,
            stderr=subprocess.DEVNULL if not verbose else None
        )

        _ = self.get_hdfs_client()
        self.get_zk_client().setup_paths()

    def shutdown_cluster(self, verbose=False):
        """
        Shutdown the local cluster by stopping services and cleaning up resources.

        :param verbose: Whether to print verbose output.
        """
        zk_client = self.get_zk_client()
        zk_client.zk.stop()
        zk_client.zk.close()

        hdfs_client = self.get_hdfs_client()
        hdfs_client.cleanup()

        subprocess.run(
            ['docker-compose', 'down'],
            stdout=subprocess.DEVNULL if not verbose else None,
            stderr=subprocess.DEVNULL if not verbose else None
        )




