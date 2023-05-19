import subprocess
import concurrent.futures
from ..monitoring.local_monitoring import LocalMonitoring


class LocalCluster:
    def __init__(self, n_workers=None, n_masters=None, initialize=False, verbose=False):
        self.zk_hosts = "localhost:2181,localhost:2182,localhost:2183"
        self.hdfs_host = "localhost:9870"  # namenode

        self.local_monitoring = LocalMonitoring(self.zk_hosts)

        self.zk_client, self.hdfs_client = None, None

        if initialize:
            self.cluster_initialize(verbose=verbose)

        if n_workers is not None or n_masters is not None:
            self.scale(n_workers, n_masters, verbose=verbose)

    def get_zk_client(self):
        return self.local_monitoring.get_zk_client()

    def get_hdfs_client(self):
        return self.local_monitoring.get_hdfs_client()

    def mapreduce(self, data, map_func, reduce_func, requested_n_workers=None):
        """
        Perform MapReduce on the local cluster. Returns a Future object immediately.

        :param data: Input data for MapReduce.
        :param map_func: Map function.
        :param reduce_func: Reduce function.
        :param requested_n_workers: The number of workers requested for this job (`None` means all available)
        :return: concurrent.futures.Future
        """
        hdfs_client = self.get_hdfs_client()
        zk_client = self.get_zk_client()

        job_id = zk_client.get_sequential_job_id()

        hdfs_client.job_create(job_id=job_id, data=data, map_func=map_func, reduce_func=reduce_func)

        zk_client.register_job(job_id=job_id, requested_n_workers=requested_n_workers)

        # Job completion event. When it is set we know the job is complete
        event = self.local_monitoring.job_completion_event(job_id)

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        job_completion_future = executor.submit(self.wait_for_completion_and_get_results, job_id, event)

        return job_completion_future

    def wait_for_completion_and_get_results(self, job_id, event):
        """
        Wait for the job to complete, then get the results.

        :param job_id: The id of the job to wait for.
        :param event: The thread.Event() job-completion event. We wait until it is set
        :return: The results of the job.
        """

        while not event.is_set():
            # TODO: print beautiful info
            event.wait(1)

        hdfs_client = self.get_hdfs_client()

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

        # This to make sure we do not delete workers. --scale with the same number as live, does not recreate
        if n_workers is None:
            _, n_workers = self.local_monitoring.get_registered_workers()
        if n_masters is None:
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

    def shutdown_cluster(self, verbose=False, delete_hdfs_jobs=False):
        """
        Shutdown the local cluster by stopping services and cleaning up resources.

        :param verbose: Whether to print verbose output.
        :param delete_hdfs_jobs: Whether to delete and not persist in the `volume` the finished jobs in HDFS
        """
        zk_client = self.get_zk_client()
        zk_client.zk.stop()
        zk_client.zk.close()

        hdfs_client = self.get_hdfs_client()
        if delete_hdfs_jobs:
            hdfs_client.cleanup()

        subprocess.run(
            ['docker-compose', 'down'],
            stdout=subprocess.DEVNULL if not verbose else None,
            stderr=subprocess.DEVNULL if not verbose else None
        )
