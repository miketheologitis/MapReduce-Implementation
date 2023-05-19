import threading
from concurrent.futures import ThreadPoolExecutor
from flask import Flask
import pickle
import os
import time
import requests

from ..zookeeper.zookeeper_client import ZookeeperClient
from ..hadoop.hdfs_client import HdfsClient

"""
The Python's Global interpreter Lock (GIL) can sometimes limit the effectiveness of multithreading 
when it comes to CPU-bound tasks. However, in this case, as the tasks are I/O-bound (waiting for 
responses from HTTP requests and Zookeeper), multithreading is the preferred option (in-contrast with
the option of multiprocessing that is used in `worker.py` for pure parallelism).
"""


# Try to find the 1st parameter in env variables which will be set up by docker-compose
# (see the .yaml file), else default to the second.
HOSTNAME = os.getenv('HOSTNAME', 'localhost')
ZK_HOSTS = os.getenv('ZK_HOSTS', '127.0.0.1:2181')
HDFS_HOST = os.getenv('HDFS_HOST', 'localhost:9870')

app = Flask(__name__)


class Master:
    def __init__(self):
        self.zk_client = None
        self.hdfs_client = None

        self.registered_workers = set()  # set of registered workers (hostnames) at any point in time

    def get_zk_client(self):
        if self.zk_client is None:
            self.zk_client = ZookeeperClient(ZK_HOSTS)
        return self.zk_client

    def get_hdfs_client(self):
        if self.hdfs_client is None:
            self.hdfs_client = HdfsClient(HDFS_HOST)
        return self.hdfs_client

    def get_idle_workers(self, requested_n_workers):
        """
        Continuously polls Zookeeper for idle workers until enough are available.
        Retrieves a list of idle workers from Zookeeper (when returned the workers'
         state will be modified as 'in-task').

        :param requested_n_workers: The number of workers requested.
        :return: List of idle workers.
        """
        zk_client = self.get_zk_client()
        idle_assigned_workers = []

        # Continuously ask Zookeeper for idle workers until we have enough.
        while not idle_assigned_workers:
            # Get the list of idle workers from Zookeeper with distributed Lock
            idle_assigned_workers = zk_client.get_workers_for_tasks(requested_n_workers)
            if not idle_assigned_workers:
                time.sleep(3)

        return idle_assigned_workers

    def handle_map(self, job_id, requested_n_workers):

        zk_client = self.get_zk_client()
        hdfs_client = self.get_hdfs_client()

        map_data = self.hdfs_client.get_data(hdfs_path=f'jobs/job_{job_id}/data.pickle')

        # Polls zookeeper for workers. Blocks until at least one worker is assigned to this master.
        # Maximum amount of workers to ask for is len(data)
        assigned_workers = self.get_idle_workers(
            requested_n_workers=min(
                requested_n_workers if requested_n_workers else len(map_data),
                len(map_data)
            )
        )
        num_assigned_workers = len(assigned_workers)

        # Split data to `num_assigned_workers` chunks and save them to HDFS
        for i, chunk in enumerate(self.split_data(map_data, num_assigned_workers)):
            hdfs_client.save_data(hdfs_path=f'jobs/job_{job_id}/map_tasks/{i}.pickle', data=chunk)

        # Register the tasks with the Zookeeper
        for i, worker_hostname in enumerate(assigned_workers):
            zk_client.register_task(
                task_type='map', job_id=job_id, state='in-progress',
                worker_hostname=worker_hostname, task_id=i
            )

        event = self.map_completion_event(job_id=job_id, n_tasks=num_assigned_workers)

        # Send async request to all the workers for each map task
        with ThreadPoolExecutor(max_workers=num_assigned_workers) as executor:
            executor.map(
                lambda task_info: requests.post(
                    f'http://{task_info[1]}:5000/map-task',
                    json={'job_id': job_id, 'task_id': task_info[0]}
                ),
                enumerate(assigned_workers)
            )
            # shutdown automatically, wait for all tasks to complete

        # wait on that event
        while not event.is_set():
            event.wait(1)

    def handle_shuffle(self, job_id):
        zk_client = self.get_zk_client()

        # Polls zookeeper for workers. Blocks until one worker is assigned to this master.
        assigned_worker_hostname = self.get_idle_workers(requested_n_workers=1)[0]

        # Register the shuffle task to zookeeper
        zk_client.register_task(
            task_type='shuffle', job_id=job_id, state='in-progress', worker_hostname=assigned_worker_hostname
        )

        # Set up an event using `DataWatcher` for the completion of the shuffle task
        event = self.shuffle_completion_event(job_id=job_id)

        # Send async request for the shuffle task
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(
                lambda: requests.post(
                    f'http://{assigned_worker_hostname}:5000/shuffle-task',
                    json={'job_id': job_id}
                )
            )

        # wait on that event
        while not event.is_set():
            event.wait(1)

    def handle_reduce(self, job_id, requested_n_workers):
        zk_client = self.get_zk_client()
        hdfs_client = self.get_hdfs_client()

        # Get the distinct keys from the shuffling by number of .pickle files on shuffle_results/ in hdfs.
        num_distinct_keys = len(hdfs_client.hdfs.list(f'jobs/job_{job_id}/shuffle_results'))

        # Polls zookeeper for workers. Blocks until at least one worker is assigned to this master.
        # Maximum amount of workers to ask for is `num_distinct_keys`
        assigned_workers = self.get_idle_workers(
            requested_n_workers=min(
                requested_n_workers if requested_n_workers else num_distinct_keys,
                num_distinct_keys
            )
        )
        num_assigned_workers = len(assigned_workers)

        # Zookeeper gave us `num_assigned_workers` for the reduce operation hence now we have to assign
        # equally the shuffle results to the reduce workers. We can do this like this
        equal_split_task_ids = [
            chunk for chunk in self.split_data(data=list(range(num_distinct_keys)), n=num_assigned_workers)
        ]
        # Register reduce tasks to Zookeeper
        for worker_hostname, task_ids in zip(assigned_workers, equal_split_task_ids):
            # Notice that in the reduce stage, we defined `task_id` to be a list of the names of the shuffle
            # .pickle files that one worker will reduce. (Workers possibly reduce multiple shuffle outs)
            zk_client.register_task(
                task_type='reduce', job_id=job_id, state='in-progress',
                worker_hostname=worker_hostname, task_id=task_ids
            )

        # Set up an event using `DataWatcher` for the completion of all the reduce tasks
        event = self.reduce_completion_event(job_id=job_id, list_tasks=equal_split_task_ids)

        # Send async request to all the workers for each map task
        with ThreadPoolExecutor(max_workers=num_assigned_workers) as executor:
            executor.map(
                lambda task_info: requests.post(
                    f'http://{task_info[0]}:5000/reduce-task',
                    json={'job_id': job_id, 'task_ids': task_info[1]}
                ),
                zip(assigned_workers, equal_split_task_ids)
            )
            # shutdown automatically, wait for all tasks to complete

        # wait on that event
        while not event.is_set():
            event.wait(1)

    def handle_job(self, job_id, requested_n_workers):
        """
        For Map, Reduce we will try to get `requested_n_workers` workers from zookeeper. If
        Zookeeper can only acquire us less than that number of workers then we continue the task,
        with this number of workers (even if only 1 worker is available for example). When no workers
        are available, we wait until at least a single worker is available.

        """
        # 1. Map Tasks (blocks of course)
        self.handle_map(job_id, requested_n_workers)

        # 2. Shuffle Task (blocks of course)
        self.handle_shuffle(job_id)

        # 3. Reduce Tasks (blocks of course)
        self.handle_reduce(job_id, requested_n_workers)

        # 4. Mark Job completed
        self.get_zk_client().update_job(job_id=job_id, state='completed')

    def handle_dead_worker_task(self, task_filename, task_type):
        """
        Given the fact that we have 'map', 'shuffle', 'reduce' tasks, and the fact that the
        information of job_id, task_id, etc. lies in:

        'map' : filename = f'/map_tasks/{job_id}_{task_id}'
        'shuffle' : filename = f'/shuffle_tasks/{job_id}'
        'reduce' : filename =  f'/reduce_tasks/{job_id}_{suffix_id}' where `shuffix_id` is
                               '_'.join(map(str, task_id)) of the shuffle tasks that this reduce task reduces.

        We can uniquely identify the task type by looking at the filename.

        :param task_filename: The filename of the task in Zookeeper
        :param task_type: The type of the task (map, shuffle, reduce)
        """
        zk_client = self.get_zk_client()

        # Polls zookeeper for workers. Blocks until one worker is assigned to this master.
        assigned_worker_hostname = self.get_idle_workers(requested_n_workers=1)[0]

        if task_type == 'map':
            # Convert the filename to the job_id, task_id (integer)
            job_id, task_id = list(map(int, task_filename.split('_')))

            # Update the task in Zookeeper (to the new worker)
            zk_client.update_task('map', job_id, task_id, worker_hostname=assigned_worker_hostname)

            # send async request to the worker
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(
                    lambda: requests.post(
                        f'http://{assigned_worker_hostname}:5000/map-task',
                        json={'job_id': job_id, 'task_id': task_id}
                    )
                )

        if task_type == 'reduce':
            # Convert the filename to the job_id, task_ids (integer)
            job_id, *task_ids = list(map(int, task_filename.split('_')))

            # Update the task in Zookeeper (to the new worker)
            zk_client.update_task('reduce', job_id, task_ids, worker_hostname=assigned_worker_hostname)

            # send async request to the worker
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(
                    lambda: requests.post(
                        f'http://{assigned_worker_hostname}:5000/reduce-task',
                        json={'job_id': job_id, 'task_ids': task_ids}
                    )
                )

        if task_type == 'shuffle':
            # Convert the filename to the job_id
            job_id = int(task_filename)

            # Update the task in Zookeeper (to the new worker)
            zk_client.update_task('shuffle', job_id, worker_hostname=assigned_worker_hostname)

            # send async request to the worker
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(
                    lambda: requests.post(
                        f'http://{assigned_worker_hostname}:5000/shuffle-task',
                        json={'job_id': job_id}
                    )
                )

    def map_completion_event(self, job_id, n_tasks):
        """
        Blocks until the specified job is completed.

        :param job_id: ID of the job that the tasks belong to
        :param n_tasks: Number of map tasks for this job. ids = 0, 1, ..., n_tasks-1
        """
        zk_client = self.get_zk_client()
        event = threading.Event()
        lock = threading.Lock()
        completed_tasks = 0  # counter to keep track of completed tasks

        for i in range(n_tasks):
            task_path = f'/map_tasks/{job_id}_{i}'

            @zk_client.zk.DataWatch(task_path)
            def callback(data, stat):
                # The callback function is called when the data at the watched znode changes.
                task = pickle.loads(data)

                if task.state == 'completed':
                    with lock:
                        nonlocal completed_tasks  # declare the variable as nonlocal to modify it
                        completed_tasks += 1
                    # Check if all tasks are completed
                    if completed_tasks == n_tasks:
                        event.set()
                    return False
        return event

    def shuffle_completion_event(self, job_id):
        """
        Blocks until the specified shuffle task for `job_id` is completed.

        :param job_id: ID of the job that the shuffle task belongs to.
        """
        zk_client = self.get_zk_client()
        event = threading.Event()

        task_path = f'/shuffle_tasks/{job_id}'

        @zk_client.zk.DataWatch(task_path)
        def callback(data, stat):
            # The callback function is called when the data at the watched znode changes.
            task = pickle.loads(data)

            if task.state == 'completed':
                event.set()
                return False  # stop watching
        return event

    def reduce_completion_event(self, job_id, list_tasks):
        """
        Blocks until the reduce is completed for the job `job_id`.

        :param job_id: ID of the job that the tasks belong to
        :param list_tasks: Each reduce task handles possibly multiple tasks which is represented
            as a list. This is because reduce gets data from the shuffle tasks, so we could assign
            multiple shuffle results to a single reducer.

            For example: list_tasks = [[0, 1], [2, 3, 4], [5]] which means that worker1 reduces the
            shuffle results 0,1 , worker2 reduces the shuffle results 2,3,4 and worker3 reduces the
            shuffle results 5.
        """
        zk_client = self.get_zk_client()
        event = threading.Event()
        lock = threading.Lock()
        completed_tasks = 0  # counter to keep track of completed tasks
        n_tasks = len(list_tasks)

        for tasks in list_tasks:
            task_path = f"/reduce_tasks/{job_id}_{'_'.join(map(str, tasks))}"

            @zk_client.zk.DataWatch(task_path)
            def callback(data, stat):
                # The callback function is called when the data at the watched znode changes.
                task = pickle.loads(data)

                if task.state == 'completed':
                    with lock:
                        nonlocal completed_tasks  # declare the variable as nonlocal to modify it
                        completed_tasks += 1
                    # Check if all tasks are completed
                    if completed_tasks == n_tasks:
                        event.set()
                    return False  # stop watching
        return event

    def new_job_watcher(self):
        """
        Sets up a watch on the /jobs path in Zookeeper.
        """

        # Get the Zookeeper client
        zk_client = self.get_zk_client()

        # The path of the jobs in the Zookeeper's namespace
        jobs_path = '/jobs'

        # Set up a watch on the job's path.
        @zk_client.zk.ChildrenWatch(jobs_path)
        def watch_jobs(children):
            # The callback function is called when the children of the watched znode change.
            # children is a list of the names of the children of jobs_path.

            job_id, requested_n_workers = zk_client.get_job(HOSTNAME)
            # we indeed got assigned a job (not None)
            if job_id is not None:
                # Start a new thread to handle the job
                job_thread = threading.Thread(target=self.handle_job, args=(job_id, requested_n_workers))
                job_thread.start()

    def dead_workers_watcher(self):
        """
        Sets up a watcher on the /workers path in Zookeeper. When a worker dies the ephemeral node at that path
        will be deleted and the watcher will be triggered. The callback function will then be called and a master
        must find whether the worker left any incomplete ('in-progress') tasks and reassign them.
        """
        zk_client = self.get_zk_client()

        @zk_client.zk.ChildrenWatch('/workers')
        def callback(children):
            # The callback function is called when the children of the watched znode change.
            # children is a list of the names of the children of jobs_path.

            children_set = set(children)

            # Find the dead workers. Set of dead worker hostnames
            dead_workers = self.registered_workers - children_set

            # Update the registered workers
            self.registered_workers = children_set

            if dead_workers:
                # if there are dead workers, reassign their tasks
                for dead_worker in dead_workers:
                    task_filename, task_type = zk_client.get_worker_task(dead_worker)

                    if task_filename is not None:
                        # Start a new thread to handle the task
                        dead_worker_task_thread = threading.Thread(
                            target=self.handle_dead_worker_task, args=(task_filename, task_type)
                        )
                        dead_worker_task_thread.start()

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

    def run(self):
        zk_client = self.get_zk_client()
        zk_client.register_master(HOSTNAME)
        self.new_job_watcher()
        # TODO: setup_dead_worker_watcher()
        app.run(host='0.0.0.0', port=5000)


# Create a singleton instance of Master
master = Master()

if __name__ == '__main__':
    master.run()

