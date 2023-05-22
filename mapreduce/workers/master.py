import threading
from concurrent.futures import ThreadPoolExecutor
from flask import Flask
import pickle
import os
import time
import requests
import logging

from ..zookeeper.zookeeper_client import ZookeeperClient
from ..hadoop.hdfs_client import HdfsClient

"""
Master death assumptions:

We define death of the master 'during-map' and 'during-reduce' if and only if we find out a task
that is 'in-progress' in each phase, respectively.

Assumption 1.

If the master fails 'during-map', 'during-reduce' phases, and we find out that one of the corresponding
tasks has not been received by the worker, i.e., Task.received is false, then no other worker has
received its task. In other words, we treat the 

# Send async request to all the workers for each map task
with ThreadPoolExecutor(max_workers=num_assigned_workers) as executor:
    executor.map(..., ...)

as a single operation.

Assumption 2.

If the master fails 'during-map', 'during-reduce' phases, all the tasks have been registered with
Zookeeper. In other words, we treat the

# Polls zookeeper for workers. Blocks until at least one worker is assigned to this master.
assigned_workers = self.get_idle_workers(...)
num_assigned_workers = len(assigned_workers)

# Register the tasks with the Zookeeper
for i, worker_hostname in enumerate(assigned_workers):
    zk_client.register_task(...)
    
lines of code as a single operation.

To clarify the second assumption, notice  that if at least one worker is assigned to the 
respective phase the `get_idle_workers` will return immediately. There is no unnecessary 
waiting happening from the point where one worker is assigned. Hence, assumption 2 also takes
a few milliseconds to complete as a whole operation. The first assumption takes nanoseconds.
The reasoning for those two assumptions is that these operations are EXTREMELY quick in respect
to the phase. If we are unlucky enough to have the master fail in the middle of one of those 
operations, then we can restart the system and re-run the operation.
"""

# Try to find the 1st parameter in env variables which will be set up by docker-compose
# (see the .yaml file), else default to the second.
HOSTNAME = os.getenv('HOSTNAME', 'localhost')
ZK_HOSTS = os.getenv('ZK_HOSTS', '127.0.0.1:2181')
HDFS_HOST = os.getenv('HDFS_HOST', 'localhost:9870')

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)


class Master:
    def __init__(self):
        self.zk_client = None
        self.hdfs_client = None

        # set of registered workers (hostnames) at any point in time
        self.registered_workers = set()

        # set of registered masters (hostnames) at any point in time (our hostname is always in this set)
        self.registered_masters = set()

        # set of jobs that are in progress
        self.in_progress_jobs = set()

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

        logging.info(f'Getting idle workers for task. I will ask for {requested_n_workers} workers.')

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

        logging.info(f'Reading data from HDFS for job {job_id}')
        map_data = self.hdfs_client.get_data(hdfs_path=f'jobs/job_{job_id}/data.pickle')

        # Polls zookeeper for workers. Blocks until at least one worker is assigned to this master.
        # Maximum amount of workers to ask for is len(data)
        logging.info(f'Getting idle workers for job {job_id}')
        assigned_workers = self.get_idle_workers(
            requested_n_workers=min(
                requested_n_workers if requested_n_workers else len(map_data),
                len(map_data)
            )
        )
        num_assigned_workers = len(assigned_workers)
        logging.info(f'Got {num_assigned_workers} idle workers for job {job_id}')

        # Register the tasks with the Zookeeper
        logging.info(f'Registering tasks with Zookeeper for job {job_id}')
        for i, worker_hostname in enumerate(assigned_workers):
            zk_client.register_task(
                task_type='map', job_id=job_id, state='in-progress',
                worker_hostname=worker_hostname, task_id=i
            )

        # Split data to `num_assigned_workers` chunks and save them to HDFS
        logging.info(f'Splitting data for job {job_id}')
        for i, chunk in enumerate(self.split_data(map_data, num_assigned_workers)):
            hdfs_client.save_data(hdfs_path=f'jobs/job_{job_id}/map_tasks/{i}.pickle', data=chunk)

        event = self.map_completion_event(job_id=job_id, n_tasks=num_assigned_workers)

        # Send async request to all the workers for each map task. Set timeout to 1 sec which will not raise an
        # exception because we are not collecting the responses. An exception will be raised when we try to iterate
        # over the results which we are not doing here.
        logging.info(f'Sending async requests to workers for job {job_id}')
        with ThreadPoolExecutor(max_workers=num_assigned_workers) as executor:
            executor.map(
                lambda task_info: requests.post(
                    f'http://{task_info[1]}:5000/map-task',
                    json={'job_id': job_id, 'task_id': task_info[0]},
                    timeout=1  # Set timeout to 1 sec
                ),
                enumerate(assigned_workers)
            )
            # shutdown automatically, wait for all tasks to complete

        # wait on that event
        logging.info(f'Waiting for map tasks to complete for job {job_id}')
        while not event.is_set():
            event.wait(1)

    def handle_shuffle(self, job_id):
        zk_client = self.get_zk_client()

        # Polls zookeeper for workers. Blocks until one worker is assigned to this master.
        logging.info(f'Getting idle worker for job {job_id}')
        assigned_worker_hostname = self.get_idle_workers(requested_n_workers=1)[0]

        # Register the shuffle task to zookeeper
        logging.info(f'Registering shuffle task with Zookeeper for job {job_id}')
        zk_client.register_task(
            task_type='shuffle', job_id=job_id, state='in-progress', worker_hostname=assigned_worker_hostname
        )

        # Set up an event using `DataWatcher` for the completion of the shuffle task
        event = self.shuffle_completion_event(job_id=job_id)

        # Send async request for the shuffle task. Set timeout to 1 sec which will not raise an exception because
        # we are not collecting the responses. An exception will be raised when we try to iterate over the results
        # which we are not doing here.
        logging.info(f'Sending async request to worker for job {job_id}')
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(
                lambda: requests.post(
                    f'http://{assigned_worker_hostname}:5000/shuffle-task',
                    json={'job_id': job_id},
                    timeout=1  # Set timeout to 1 sec
                )
            )

        # wait on that event
        logging.info(f'Waiting for shuffle task to complete for job {job_id}')
        while not event.is_set():
            event.wait(1)

    def handle_reduce(self, job_id, requested_n_workers):
        zk_client = self.get_zk_client()
        hdfs_client = self.get_hdfs_client()

        # Get the distinct keys from the shuffling by number of .pickle files on shuffle_results/ in hdfs.
        logging.info(f'Getting distinct keys for job {job_id}')
        num_distinct_keys = len(hdfs_client.hdfs.list(f'jobs/job_{job_id}/shuffle_results'))

        # Polls zookeeper for workers. Blocks until at least one worker is assigned to this master.
        # Maximum amount of workers to ask for is `num_distinct_keys`
        logging.info(f'Getting idle workers for job {job_id}')
        assigned_workers = self.get_idle_workers(
            requested_n_workers=min(
                requested_n_workers if requested_n_workers else num_distinct_keys,
                num_distinct_keys
            )
        )
        num_assigned_workers = len(assigned_workers)
        logging.info(f'Got {num_assigned_workers} idle workers for job {job_id}')

        # Zookeeper gave us `num_assigned_workers` for the reduce operation hence now we have to assign
        # equally the shuffle results to the reduce workers. We can do this like this
        equal_split_task_ids = [
            chunk for chunk in self.split_data(data=list(range(num_distinct_keys)), n=num_assigned_workers)
        ]
        # Register reduce tasks to Zookeeper
        logging.info(f'Registering reduce tasks with Zookeeper for job {job_id}')
        for worker_hostname, task_ids in zip(assigned_workers, equal_split_task_ids):
            # Notice that in the reduce stage, we defined `task_id` to be a list of the names of the shuffle
            # .pickle files that one worker will reduce. (Workers possibly reduce multiple shuffle outs)
            zk_client.register_task(
                task_type='reduce', job_id=job_id, state='in-progress',
                worker_hostname=worker_hostname, task_id=task_ids
            )

        # Set up an event using `DataWatcher` for the completion of all the reduce tasks
        event = self.reduce_completion_event(job_id=job_id, list_tasks=equal_split_task_ids)

        # Send async request to all the workers for each map task. Set timeout to 1 sec which will not raise an
        # exception because we are not collecting the responses. An exception will be raised when we try to iterate over
        # the results which we are not doing here.
        logging.info(f'Sending async requests to workers for job {job_id}')
        with ThreadPoolExecutor(max_workers=num_assigned_workers) as executor:
            executor.map(
                lambda task_info: requests.post(
                    f'http://{task_info[0]}:5000/reduce-task',
                    json={'job_id': job_id, 'task_ids': task_info[1]},
                    timeout=1  # Set timeout to 1 sec
                ),
                zip(assigned_workers, equal_split_task_ids)
            )
            # shutdown automatically, wait for all tasks to complete

        # wait on that event
        logging.info(f'Waiting for reduce tasks to complete for job {job_id}')
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
        logging.info(f'Handling map stage for {job_id}')
        self.handle_map(job_id, requested_n_workers)

        # 2. Shuffle Task (blocks of course)
        logging.info(f'Handling shuffle stage for {job_id}')
        self.handle_shuffle(job_id)

        # 3. Reduce Tasks (blocks of course)
        logging.info(f'Handling reduce stage for {job_id}')
        self.handle_reduce(job_id, requested_n_workers)

        # 4. Mark Job completed
        logging.info(f'Marking job {job_id} as completed')
        self.get_zk_client().update_job(job_id=job_id, state='completed')

    def handle_dead_worker_task(self, task_filename, task_type, register_dead_task=True):
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
        :param register_dead_task: If True, then we register the task as dead in Zookeeper
        """
        logging.info(f'Handling dead worker task: {task_filename}')

        zk_client = self.get_zk_client()

        if register_dead_task:
            zk_client.register_dead_task(filename=task_filename, task_type=task_type, state='in-progress', master_hostname=HOSTNAME)
            logging.info(f'Registered dead task: {task_filename} to Zookeeper')

        # Polls zookeeper for workers. Blocks until one worker is assigned to this master.
        assigned_worker_hostname = self.get_idle_workers(requested_n_workers=1)[0]

        if task_type == 'map':
            logging.info(f'Found out that the dead worker was assigned to a map task')

            # Convert the filename to the job_id, task_id (integer)
            job_id, task_id = list(map(int, task_filename.split('_')))

            # Update the task in Zookeeper (to the new worker)
            zk_client.update_task('map', job_id, task_id, worker_hostname=assigned_worker_hostname)

            # send async request to the worker. Set timeout to 1 sec which will not raise an exception because
            # we are not collecting the responses. An exception will be raised when we try to iterate over the results
            # which we are not doing here.
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(
                    lambda: requests.post(
                        f'http://{assigned_worker_hostname}:5000/map-task',
                        json={'job_id': job_id, 'task_id': task_id},
                        timeout=1  # Set timeout to 1 sec
                    )
                )

        if task_type == 'reduce':
            logging.info(f'Found out that the dead worker was assigned to a reduce task')

            # Convert the filename to the job_id, task_ids (integer)
            job_id, *task_ids = list(map(int, task_filename.split('_')))

            # Update the task in Zookeeper (to the new worker)
            zk_client.update_task('reduce', job_id, task_ids, worker_hostname=assigned_worker_hostname)

            # send async request to the worker Set timeout to 1 sec which will not raise an exception because
            # we are not collecting the responses. An exception will be raised when we try to iterate over the results
            # which we are not doing here.
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(
                    lambda: requests.post(
                        f'http://{assigned_worker_hostname}:5000/reduce-task',
                        json={'job_id': job_id, 'task_ids': task_ids},
                        timeout=1  # Set timeout to 1 sec
                    )
                )

        if task_type == 'shuffle':
            logging.info(f'Found out that the dead worker was assigned to a shuffle task')

            # Convert the filename to the job_id
            job_id = int(task_filename)

            # Update the task in Zookeeper (to the new worker)
            zk_client.update_task('shuffle', job_id, worker_hostname=assigned_worker_hostname)

            # send async request to the worker. Set timeout to 1 sec which will not raise an exception because
            # we are not collecting the responses. An exception will be raised when we try to iterate over the results
            # which we are not doing here.
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(
                    lambda: requests.post(
                        f'http://{assigned_worker_hostname}:5000/shuffle-task',
                        json={'job_id': job_id},
                        timeout=1  # Set timeout to 1 sec
                    )
                )

        logging.info(f'Finished handling dead worker task: {task_filename}')
        zk_client.update_dead_task(filename=task_filename, state='completed')
        logging.info(f'Updated dead worker task as "completed" to Zookeeper')

    def handle_dead_master_job(self, job_id, requested_n_workers):
        """
        When this function is called we know that we were assigned to handle the job of the dead master. There are
        seven scenarios that we need to handle:

        1. The master died before the map phase started. Alias: 'before-map'
        2. The master died during the map phase. Alias: 'during-map'
        3. The master died before the shuffle phase started. Alias: 'before-shuffle'
        4. The master died during the shuffle phase. Alias: 'during-shuffle'
        5. The master died before the reduce phase started. Alias: 'before-reduce'
        6. The master died during the reduce phase. Alias: 'during-reduce'
        7. The master died after the reduce phase and before marking the job as completed. Alias: 'before-completion'
        """
        logging.info(f'I am currently handling the job {job_id} of the dead master')

        zk_client = self.get_zk_client()
        hdfs_client = self.get_hdfs_client()

        # Note that the job master is us right now. We investigate the job based solely on the job_id.
        info_dict = zk_client.investigate_job_of_dead_master(job_id)

        if info_dict['scenario'] == 'before-map':
            logging.info(f'I found that the master died before the map phase started')

            # Job just started so treat it as a new job
            self.handle_job(job_id, requested_n_workers)

        if info_dict['scenario'] == 'during-map':
            logging.info(f'I found that the master died during the map phase')

            # Based on Assumption 2. (see comments at the top of the file)
            num_assigned_workers = len(info_dict['map_tasks'])

            # Assumption 1. (see comments at the top of the file)
            if any([not task.received for task in info_dict['map_tasks']]):
                logging.info(f'At least one task was not received by a worker. I will continue based on assumption 1.')

                # Save data to HDFS (maybe again)
                map_data = self.hdfs_client.get_data(hdfs_path=f'jobs/job_{job_id}/data.pickle')

                # Split data to `num_assigned_workers` chunks and save them to HDFS
                for i, chunk in enumerate(self.split_data(map_data, num_assigned_workers)):
                    hdfs_client.save_data(hdfs_path=f'jobs/job_{job_id}/map_tasks/{i}.pickle', data=chunk)

                event = self.map_completion_event(job_id=job_id, n_tasks=num_assigned_workers)

                # Then based on Assumption 1. no map task has been received by a worker yet.
                # (see comments at the top of the file). Set timeout to 1 sec which will not raise an exception because
                # we are not collecting the responses. An exception will be raised when we try to iterate over the
                # results which we are not doing here.
                with ThreadPoolExecutor(max_workers=num_assigned_workers) as executor:
                    executor.map(
                        lambda task_info: requests.post(
                            f'http://{task_info[1]}:5000/map-task',
                            json={'job_id': job_id, 'task_id': task_info[0]},
                            timeout=1  # Set timeout to 1 sec
                        ),
                        zip(
                            [task_id for task_id in info_dict['task_ids']],
                            [task.worker_hostname for task in info_dict['map_tasks']]
                        )
                    )
                    # shutdown automatically, wait for all tasks to complete

                # wait on that event
                while not event.is_set():
                    event.wait(1)

            else:
                logging.info(f'All map tasks were received by the workers. I am waiting for map phase to complete.')

                # All workers have received their tasks and running so just wait for map to finish
                event = self.map_completion_event(job_id=job_id, n_tasks=num_assigned_workers)

                # wait on that event
                while not event.is_set():
                    event.wait(1)

            # 2. Shuffle Task (blocks of course)
            logging.info('Map phase completed. I am handling shuffle phase now.')
            self.handle_shuffle(job_id)

            # 3. Reduce Tasks (blocks of course)
            logging.info('Shuffle phase completed. I am handling reduce phase now.')
            self.handle_reduce(job_id, requested_n_workers)

            # 4. Mark Job completed
            logging.info('Reduce phase completed. I am marking the job as completed now.')
            zk_client.update_job(job_id=job_id, state='completed')
            logging.info('I am done.')

        if info_dict['scenario'] == 'before-shuffle':
            logging.info(f'I found that the master died before the shuffle phase started')

            # 2. Shuffle Task (blocks of course)
            logging.info('I am handling shuffle phase now.')
            self.handle_shuffle(job_id)

            # 3. Reduce Tasks (blocks of course)
            logging.info('Shuffle phase completed. I am handling reduce phase now.')
            self.handle_reduce(job_id, requested_n_workers)

            # 4. Mark Job completed
            logging.info('Reduce phase completed. I am marking the job as completed now.')
            zk_client.update_job(job_id=job_id, state='completed')
            logging.info('I am done.')

        if info_dict['scenario'] == 'during-shuffle':
            logging.info(f'I found that the master died during the shuffle phase')

            shuffle_task = info_dict['shuffle_task']

            if not shuffle_task.received:
                logging.info(f'The shuffle task was not received by a worker.')
                # Set up an event using `DataWatcher` for the completion of the shuffle task
                event = self.shuffle_completion_event(job_id=job_id)

                # Send async request for the shuffle task Set timeout to 1 sec which will not raise an exception because
                # we are not collecting the responses. An exception will be raised when we try to iterate over the
                # results which we are not doing here.
                with ThreadPoolExecutor(max_workers=1) as executor:
                    executor.submit(
                        lambda: requests.post(
                            f'http://{shuffle_task.worker_hostname}:5000/shuffle-task',
                            json={'job_id': job_id},
                            timeout=1  # Set timeout to 1 sec
                        )
                    )

                # wait on that event
                while not event.is_set():
                    event.wait(1)
            else:
                logging.info(
                    f'The shuffle task was received by a worker. I am waiting for the shuffle phase to complete.'
                )
                event = self.shuffle_completion_event(job_id=job_id)

                # wait on that event
                while not event.is_set():
                    event.wait(1)

            # 3. Reduce Tasks (blocks of course)
            logging.info('Shuffle phase completed. I am handling reduce phase now.')
            self.handle_reduce(job_id, requested_n_workers)

            # 4. Mark Job completed
            logging.info('Reduce phase completed. I am marking the job as completed now.')
            zk_client.update_job(job_id=job_id, state='completed')
            logging.info('I am done.')

        if info_dict['scenario'] == 'before-reduce':
            logging.info(f'I found that the master died before the reduce phase started')

            # 3. Reduce Tasks (blocks of course)
            logging.info('I am handling reduce phase now.')
            self.handle_reduce(job_id, requested_n_workers)

            # 4. Mark Job completed
            logging.info('Reduce phase completed. I am marking the job as completed now.')
            zk_client.update_job(job_id=job_id, state='completed')
            logging.info('I am done.')

        if info_dict['scenario'] == 'during-reduce':
            logging.info(f'I found that the master died during the reduce phase')

            # Based on Assumption 2. (see comments at the top of the file)
            num_assigned_workers = len(info_dict['reduce_tasks'])

            # Assumption 1. (see comments at the top of the file)
            if any([not task.received for task in info_dict['reduce_tasks']]):
                logging.info(f'Not all reduce tasks were received by the workers. Following assumption 1.')

                event = self.reduce_completion_event(job_id=job_id, list_tasks=info_dict['task_ids'])

                # Then based on Assumption 1. no map task has been received by a worker yet.
                # (see comments at the top of the file). Set timeout to 1 sec which will not raise an exception because
                # we are not collecting the responses. An exception will be raised when we try to iterate over the
                # results which we are not doing here.
                with ThreadPoolExecutor(max_workers=num_assigned_workers) as executor:
                    executor.map(
                        lambda task_info: requests.post(
                            f'http://{task_info[0]}:5000/reduce-task',
                            json={'job_id': job_id, 'task_ids': task_info[1]},
                            timeout=1  # Set timeout to 1 sec
                        ),
                        zip(
                            [task.worker_hostname for task in info_dict['reduce_tasks']],
                            [task_id for task_id in info_dict['task_ids']]
                        )
                    )
                    # shutdown automatically, wait for all tasks to complete

                # wait on that event
                while not event.is_set():
                    event.wait(1)

            else:
                logging.info(
                    f'All reduce tasks were received by the workers. I am waiting for the reduce phase to complete.'
                )

                # All workers have received their tasks and running so just wait for map to finish
                event = self.reduce_completion_event(job_id=job_id, list_tasks=info_dict['task_ids'])

                # wait on that event
                while not event.is_set():
                    event.wait(1)

            zk_client.update_job(job_id=job_id, state='completed')

        if info_dict['scenario'] == 'before-completion':
            logging.info(f'I found that the master died before the job was completed')

            # 4. Mark Job completed
            logging.info('I am marking the job as completed now.')
            zk_client.update_job(job_id=job_id, state='completed')
            logging.info('I am done.')

    def map_completion_event(self, job_id, n_tasks):
        """
        Blocks until the specified job is completed.

        :param job_id: ID of the job that the tasks belong to
        :param n_tasks: Number of map tasks for this job. ids = 0, 1, ..., n_tasks-1
        """
        zk_client = self.get_zk_client()
        event = threading.Event()
        lock = threading.Lock()  # lock to protect the counter
        completed_tasks = 0  # counter to keep track of completed tasks

        for i in range(n_tasks):
            task_path = f'/map_tasks/{job_id}_{i}'

            # Notice that the following callback uses @DataWatch which will be called the first time it is set up.
            # Hence, even if the map tasks have already been completed, the event will be set, and it will not block.
            @zk_client.zk.DataWatch(task_path)
            def callback(data, stat):
                # The callback function is called when the data at the watched z-node changes.
                task = pickle.loads(data)

                logging.info(f'Callback called for map task {task} in waiting for completion event')

                if task.state == 'completed':
                    # Increment the counter with lock
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

            # The callback function is called when the children of the watched znode change.
            # children is a list of the filenames (job ids) of the children of the watched znode.

            children_set = set(children)

            # Find the new jobs
            new_jobs = children_set - self.in_progress_jobs

            logging.info(f'New jobs: {new_jobs}')

            # Update the registered jobs
            self.in_progress_jobs = children_set

            # For each new job, try and get it
            for job_id in new_jobs:
                job_id = int(job_id)

                # Get the job from Zookeeper
                logging.info(f'Trying to get job {job_id} from Zookeeper')
                got_job = zk_client.get_job(HOSTNAME, job_id)

                if got_job:
                    # If the job was successfully got, then handle it
                    logging.info(f'Got job {job_id} from Zookeeper')
                    job = zk_client.get(f'/jobs/{job_id}')
                    logging.info(f'Got job {job} from Zookeeper')

                    job_thread = threading.Thread(
                        target=self.handle_job, args=(job_id, job.requested_n_workers)
                    )
                    job_thread.start()

    def dead_workers_watcher(self):
        """
        Sets up a watcher on the /workers path in Zookeeper. When a worker dies the ephemeral node at that path
        will be deleted and the watcher will be triggered. The callback function will then be called and a master
        must find whether the worker left any incomplete ('in-progress') tasks and reassign them.

        Remember that the callback function is called the first time the watcher is set up, so the
        `self.registered_workers` are updated with the current workers. We rely on the guarantee that the
        ephemeral nodes are deleted when the worker dies, hence on the first call we indeed have the current
        workers in the distributed system.
        """
        zk_client = self.get_zk_client()

        @zk_client.zk.ChildrenWatch('/workers')
        def callback(children):
            # The callback function is called when the children of the watched znode change.
            # children is a list of the names of the children of jobs_path.

            logging.info(f'Children of /workers changed. Current children: {children}')

            children_set = set(children)

            # Find the dead workers. Set of dead worker hostnames
            dead_workers = self.registered_workers - children_set

            # Update the registered workers
            self.registered_workers = children_set

            # if there are dead workers, reassign their tasks
            for dead_worker in dead_workers:
                task_filename, task_type = zk_client.get_dead_worker_task(dead_worker)

                if task_filename is not None:
                    logging.info(
                        f'I managed to get myself assigned to handle the task of the dead worker {dead_worker}'
                    )

                    # Start a new thread to handle the task
                    dead_worker_task_thread = threading.Thread(
                        target=self.handle_dead_worker_task, args=(task_filename, task_type)
                    )
                    dead_worker_task_thread.start()

    def dead_masters_watcher(self):
        """
        Sets up a watcher on the /masters path in Zookeeper. When a master dies the ephemeral node at that path
        will be deleted and the watcher will be triggered. The callback function will then be called and a master
        must find whether the master left any incomplete ('in-progress') jobs and handle them accordingly.

        Remember that the callback function is called the first time the watcher is set up, so the
        `self.registered_masters` are updated with the current masters. We rely on the guarantee that the
        ephemeral nodes are deleted when the master dies, hence on the first call we indeed have the current
        masters in the distributed system.
        """
        zk_client = self.get_zk_client()

        @zk_client.zk.ChildrenWatch('/masters')
        def callback(children):
            # The callback function is called when the children of the watched znode change.
            # children is a list of the names of the children of jobs_path.

            children_set = set(children)

            # Find the dead masters. Set of dead master hostnames
            dead_masters = self.registered_masters - children_set

            # Update the registered masters
            self.registered_masters = children_set

            # if there are dead masters, reassign their jobs
            for dead_master in dead_masters:

                # Protected by distributed lock, only one master can handle the dead master's responsibilities
                # The rest will have empty lists. Each job and dead task's hostname is the dead master's hostname
                # and after the lock is released, the new master will be responsible for them, i.e., the hostname
                # will be changed to the new master's hostname
                jobs, dead_tasks = zk_client.get_dead_master_responsibilities(
                    new_master_hostname=HOSTNAME, dead_master_hostname=dead_master
                )

                for dead_task_dict in dead_tasks:
                    logging.info(
                        f"Handling dead task {dead_task_dict['filename']} of dead master {dead_master}"
                    )
                    # Start a new thread to handle the task
                    dead_worker_task_thread = threading.Thread(
                        target=self.handle_dead_worker_task,
                        args=(dead_task_dict['filename'], dead_task_dict['task_type'], False)
                    )
                    dead_worker_task_thread.start()

                for job_dict in jobs:
                    logging.info(
                        f"Handling job {job_dict['job_id']} of dead master {dead_master}"
                    )
                    # Start a new thread to handle the job
                    dead_master_job_thread = threading.Thread(
                        target=self.handle_dead_master_job,
                        args=(job_dict['job_id'], job_dict['requested_n_workers'])
                    )
                    dead_master_job_thread.start()

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
        logging.info(f'Started running...')
        zk_client = self.get_zk_client()
        zk_client.register_master(HOSTNAME)
        self.new_job_watcher()
        self.dead_workers_watcher()
        self.dead_masters_watcher()
        app.run(host='0.0.0.0', port=5000)


# Create a singleton instance of Master
master = Master()

if __name__ == '__main__':
    master.run()

