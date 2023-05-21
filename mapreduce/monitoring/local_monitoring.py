import pickle
import threading
from ..zookeeper.zookeeper_client import ZookeeperClient
from ..hadoop.hdfs_client import HdfsClient


class LocalMonitoring:

    def __init__(self, zk_hosts="localhost:2181,localhost:2182,localhost:2183", hdfs_host="localhost:9870"):
        self.zk_hosts = zk_hosts
        self.hdfs_host = hdfs_host

        self.zk_client, self.hdfs_client = None, None

    def get_zk_client(self):
        if self.zk_client is None:
            self.zk_client = ZookeeperClient(self.zk_hosts)
        return self.zk_client

    def get_hdfs_client(self):
        if self.hdfs_client is None:
            self.hdfs_client = HdfsClient(self.hdfs_host)
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

    def job_completion_event(self, job_id):
        """
        Blocks until the specified job is completed.

        :param job_id: ID of the job to wait for.
        """

        # Get the Zookeeper client
        zk_client = self.get_zk_client()

        # An event object is used to block the current thread until the event is set.
        # The event is initially unset.
        event = threading.Event()

        # The path of the job in the Zookeeper's namespace
        job_path = f'/jobs/{job_id}'

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
                return False  # stop further calls https://kazoo.readthedocs.io/_/downloads/en/2.2/pdf/ page:43

        return event

    def print_hdfs(self, path, indent=''):
        hdfs_client = self.get_hdfs_client()

        # Retrieve the content of the path.
        content = hdfs_client.hdfs.list(path, status=True)

        # Iterate over each item.
        for name, stats in content:
            if stats['type'] == 'DIRECTORY':
                print(f'{indent}{name}/')
                # If the item is a directory, recurse.
                self.print_hdfs(f'{path}/{name}', indent + '  ')
            else:
                print(f'{indent}{name}')

    def print_zoo(self):
        zk_client = self.get_zk_client()

        print()
        print("----------------- Zoo Masters -----------------")
        for file in zk_client.zk.get_children('/masters'):
            print(f'Master {file} :  {zk_client.get(f"/masters/{file}")}')

        print()

        print("----------------- Zoo Workers -----------------")
        for file in zk_client.zk.get_children('/workers'):
            print(f'Worker {file} :  {zk_client.get(f"/workers/{file}")}')

        print()

        print("----------------- Zoo Map Tasks -----------------")
        for file in zk_client.zk.get_children('/map_tasks'):
            print(f'Task {file} :  {zk_client.get(f"/map_tasks/{file}")}')

        print()

        print("----------------- Zoo Shuffle Tasks -----------------")
        for file in zk_client.zk.get_children('/shuffle_tasks'):
            print(f'Task {file} :  {zk_client.get(f"/shuffle_tasks/{file}")}')

        print()

        print("----------------- Zoo Reduce Tasks -----------------")
        for file in zk_client.zk.get_children('/reduce_tasks'):
            print(f'Task {file} :  {zk_client.get(f"/reduce_tasks/{file}")}')

        print()

        print("----------------- Zoo Jobs ---------------------")
        for file in zk_client.zk.get_children('/jobs'):
            print(f'Job {file} :  {zk_client.get(f"/jobs/{file}")}')

        print("\n\n------------- Dead Worker Tasks ---------------------")
        for file in zk_client.zk.get_children('/dead_tasks'):
            print(f'Dead Worker Task {file} :  {zk_client.get(f"/dead_tasks/{file}")}')
