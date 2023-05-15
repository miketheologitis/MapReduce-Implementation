import time
import pickle
import threading
from ..zookeeper.zookeeper_client import ZookeeperClient


class LocalMonitoring:

    def __init__(self):
        self.zk_hosts = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

        self.zk_client = None

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

    def wait_for_job_completion(self, job_id):
        """
        Blocks until the specified job is completed.

        :param job_id: ID of the job to wait for.
        """

        # Get the Zookeeper client
        zk_client = self.get_zk_client()

        # The path of the job in the Zookeeper's namespace
        job_path = f'/jobs/{job_id}'

        # An event object is used to block the current thread until the event is set.
        # The event is initially unset.
        event = threading.Event()

        # Before setting up the watch, check if the job is already completed.
        # This step is important because changes that occurred before the watch was set up
        # would not trigger the watch.
        job_info = zk_client.get(job_path)
        if job_info.state == 'completed':
            # If the job is already completed, there's no need to set up a watch.
            # So we return from the function.
            return

        # Set up a watch on the job path.
        # A watch is a one-time trigger that occurs when the data at the watched znode changes.
        # The DataWatch decorator sets up the watch and calls the decorated function
        # every time the data changes.
        @zk_client.zk.DataWatch(job_path)
        def callback(data, stat):
            # The callback function is called when the data at the watched znode changes.

            # Deserialize the data back into a Job object
            job = pickle.loads(data)

            # If the job has completed, set the event.
            # Setting the event will unblock the wait_for_job_completion function.
            if job.state == 'completed':
                event.set()

        # Wait until the job is completed.
        # This is done by entering a loop that continues until the event is set.
        # In each iteration of the loop, the thread is blocked for 1 second or until the event is set,
        # whichever happens first.
        while not event.is_set():
            event.wait(1)
