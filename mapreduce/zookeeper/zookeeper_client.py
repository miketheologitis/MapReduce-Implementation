from typing import NamedTuple
import pickle
import logging
import time
from kazoo.client import KazooClient

# Get the logger for Kazoo
kazoo_logger = logging.getLogger('kazoo.client')

# Set the log level for this logger to ERROR
# This means that WARNING messages will be ignored
kazoo_logger.setLevel(logging.ERROR)

"""
/
├── workers/    (created by `user` `not ephemeral`)
│   ├── <HOSTNAME>      (created by `worker` on register `ephemeral`)
│   ├── ...
├── masters/        (created by `user` `not ephemeral`)
│   ├── <HOSTNAME>      (created by `master` on register `ephemeral`)
│   ├── ...
├── jobs/
│   ├── <job_id>  `master` hostname. (created by `user`)
├── map_tasks/      (created by `user` `not ephemeral`)
│   ├── <job_id>_<task_id>   
├── shuffle_tasks/      (created by `user` `not ephemeral`)
│   ├── <job_id>   
├── reduce_tasks/      (created by `user` `not ephemeral`)
│   ├── <job_id>_<task_id1>_<task_id1>...     , For example: 1_20_30_32 job_id : 1 , task_ids: [20,30,32]
├── generators/  (created by `user` `not ephemeral`)
│   ├── job_id_sequential
├── locks/  (created by `user` `not ephemeral`)
│   ├── get_workers_for_tasks_lock
│   ├── master_job_assignment_lock
│   ├── dead_worker_task_lock
"""


class WorkerInfo(NamedTuple):
    """
    :param state: Task state ('idle', 'in-task').
    """
    state: str = 'idle'  # 'idle', 'in-task'


class Job(NamedTuple):
    """
    :param master_hostname: `master` hostname that is assigned to this job
    :param requested_n_workers: The number of workers requested for this job
    :param state: Task state ('idle', 'in-progress', 'completed').
    """
    state: str = 'idle'
    requested_n_workers: int = None
    master_hostname: str = None


class MasterInfo(NamedTuple):
    state: str = 'nothing'


class Task(NamedTuple):
    """
    :param worker_hostname: Worker hostname that runs the task.
    :param state: Task state ('in-progress', 'completed').
    :param received: True if the worker has received the task. Will help us in case of master failure
    """
    state: str
    worker_hostname: str
    received: bool = False


class ZookeeperClient:
    def __init__(self, hosts):
        self.zk = KazooClient(hosts=hosts)
        self.start_with_retries()

    def start_with_retries(self, max_retries=15, retry_delay=10):
        for i in range(max_retries):
            try:
                self.zk.start()
            except Exception as e:
                if i < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:  # raise exception if this was the last retry
                    raise Exception("Could not connect to Zookeeper after multiple attempts") from e

    def setup_paths(self):
        """Creates the necessary directories in ZooKeeper."""
        paths = [
            '/workers', '/masters', '/map_tasks', '/shuffle_tasks',
            '/reduce_tasks', '/generators', '/locks', '/jobs'
        ]
        for path in paths:
            self.zk.create(path)

    def register_worker(self, worker_hostname):
        """
        Registers a worker in ZooKeeper.

        :param worker_hostname: Hostname of the worker.
        """
        worker_info = WorkerInfo()
        serialized_worker_info = pickle.dumps(worker_info)
        # The `ephemeral` flag ensures that the z-node is automatically deleted if
        # the master disconnects
        self.zk.create(f'/workers/{worker_hostname}', serialized_worker_info, ephemeral=True)

    def register_master(self, master_hostname):
        """
        Registers a master in ZooKeeper.

        :param master_hostname: Hostname of the master.
        """
        master_info = MasterInfo()
        serialized_master_info = pickle.dumps(master_info)
        # The `ephemeral` flag ensures that the z-node is automatically deleted if
        # the master disconnects
        self.zk.create(f'/masters/{master_hostname}', serialized_master_info, ephemeral=True)

    def register_task(self, task_type, job_id, state, worker_hostname, task_id=None):
        """
        Registers a task in ZooKeeper.

        :param task_type: Task type ('map', 'reduce', 'shuffle').
        :param job_id: Unique job ID.
        :param state: 'in-progress'
        :param worker_hostname: The worker that got assigned this task (hostname)
        :param task_id: Task ID
        """
        task = Task(state=state, worker_hostname=worker_hostname)
        serialized_task = pickle.dumps(task)

        if task_type == 'map':
            self.zk.create(f'/map_tasks/{job_id}_{task_id}', serialized_task)

        if task_type == 'reduce':
            suffix_id = '_'.join(map(str, task_id))
            self.zk.create(f'/reduce_tasks/{job_id}_{suffix_id}', serialized_task)

        if task_type == 'shuffle':
            self.zk.create(f'/shuffle_tasks/{job_id}', serialized_task)

    def register_job(self, job_id, requested_n_workers=None):
        """
        Registers a job in ZooKeeper.

        :param job_id: The unique job id
        :param requested_n_workers: The number of workers requested for this job
        """
        job = Job(requested_n_workers=requested_n_workers)
        serialized_job = pickle.dumps(job)
        self.zk.create(f'/jobs/{job_id}', serialized_job)

    def update_task(self, task_type, job_id, task_id=None, **kwargs):
        """
        Updates task information in ZooKeeper.

        :param task_type: Task type ('map', 'reduce', 'shuffle').
        :param job_id: Unique job ID.
        :param task_id: Task ID (optional for 'map' and 'shuffle' tasks).
        :param kwargs: Additional attributes to update in the task.
        """
        task_path = None

        if task_type == 'map':
            task_path = f'/map_tasks/{job_id}_{task_id}'

        if task_type == 'reduce':
            suffix_id = '_'.join(map(str, task_id))
            task_path = f'/reduce_tasks/{job_id}_{suffix_id}'

        if task_type == 'shuffle':
            task_path = f'/shuffle_tasks/{job_id}'

        task = self.get(task_path)
        updated_task = task._replace(**kwargs)
        self.zk.set(task_path, pickle.dumps(updated_task))

    def update_worker(self, worker_hostname, **kwargs):
        """
        Updates worker information in ZooKeeper.

        :param worker_hostname: Hostname of the worker.
        :param kwargs: Additional attributes to update in the worker.
        """
        worker_path = f'/workers/{worker_hostname}'
        worker_info = self.get(worker_path)
        updated_worker_info = worker_info._replace(**kwargs)
        self.zk.set(worker_path, pickle.dumps(updated_worker_info))

    def update_job(self, job_id, **kwargs):
        """
        Updates job information in ZooKeeper.

        This method is used to modify the state and master_hostname of a job in ZooKeeper.
        For 'idle' jobs, it is expected to be called under a distributed lock, i.e.,
        `with self.zk.Lock("/locks/master_job_assignment_lock")`. This is to prevent
        race conditions when multiple masters try to update the same 'idle' job simultaneously.

        For 'in-progress' jobs, only the assigned master will ever modify it again (to mark it as 'completed').
        Therefore, it is not necessary to acquire a lock in such cases as there is no risk of concurrent modification.

        :param job_id: The unique job id
        :param kwargs: Additional attributes to update in the job.
                       This can include 'state' (to mark the job as 'in-progress' or 'completed')
                       and 'master_hostname' (to assign or reassign the job to a master).
        """
        job_path = f'/jobs/{job_id}'
        job_info = self.get(job_path)
        updated_job_info = job_info._replace(**kwargs)
        self.zk.set(job_path, pickle.dumps(updated_job_info))

    def get_workers_for_tasks(self, n=None):
        """
        Retrieves 'idle' workers, marks them as 'in-task', and returns their hostnames.

        This method is protected by a distributed lock to ensure that only one client can
        execute it at a time across the distributed system. This is necessary to avoid
        race conditions that could occur when multiple clients try to assign tasks to workers
        simultaneously.

        The distributed lock is implemented with Zookeeper's Lock recipe, which provides
        the guarantee of mutual exclusion even distributed system.

        :param n: Maximum number of idle workers to retrieve.
        :return: List of worker hostnames.
        """
        idle_workers = []

        with self.zk.Lock("/locks/get_workers_for_tasks_lock"):

            # Get the children (worker hostnames) under the workers path
            children = self.zk.get_children('/workers')

            # Iterate over the children to find 'idle' workers
            for hostname in children:
                worker_path = f'/workers/{hostname}'
                worker_info = self.get(worker_path)

                if worker_info.state == 'idle':
                    # Update the worker state to 'in-task'
                    self.update_worker(hostname, state='in-task')

                    # Add the worker hostname to the list of idle workers
                    idle_workers.append(hostname)

                    if n is not None and len(idle_workers) == n:
                        # If the desired number of idle workers is reached, break the loop
                        break

        return idle_workers

    def get_job(self, master_hostname):
        """
        Retrieves an idle job and updates its state to 'in-progress' with the assigned master hostname.

        This method is protected by a distributed lock to ensure that only one client can
        execute it at a time across the distributed system. This is necessary to avoid
        race conditions that could occur when multiple masters try to retrieve and update jobs simultaneously.

        The distributed lock is implemented with ZooKeeper's Lock recipe, which provides
        the guarantee of mutual exclusion even in a distributed system.

        :param master_hostname: Hostname of the master.
        :return: The job ID of the retrieved idle job, or None if no idle jobs are available.
                 and the number of workers requested for this job
        """

        with self.zk.Lock("/locks/master_job_assignment_lock"):

            # Get the children (worker hostnames) under the workers path
            children = self.zk.get_children('/jobs')

            for job_id in children:
                job = self.get(f'/jobs/{job_id}')
                requested_n_workers = job.requested_n_workers

                if job.state == 'idle':
                    # Update the job state to 'in-progress' with the assigned master hostname
                    self.update_job(job_id, state='in-progress', master_hostname=master_hostname)
                    return int(job_id), requested_n_workers

            return None, None

    def get_dead_worker_task(self, dead_worker_hostname):
        """
        Finds the task that of the dead worker that is 'in-progress'. Notice that only one such task exists (if any).
        This method is protected by a distributed lock to ensure that only one master can execute it at a time across
        the distributed system. If such a task is found we update its HOSTNAME to 'None' so that no other master gets
        assigned to handle that. We also return the type of task it was, i.e., 'map', 'shuffle', or 'reduce'.
        If no such task is found we return None.

        :param dead_worker_hostname: Hostname of the dead worker.
        :returns: Tuple (filename, task_type) or (None, None) if no such task is found. From the filename we can
                    extract the job_id and task_id.
        """

        with self.zk.Lock("/locks/dead_worker_task_lock"):

            # Look for such 'in-progress' task in `/map_tasks`
            for task_file in self.zk.get_children('/map_tasks'):
                task = self.get(f'/map_tasks/{task_file}')
                if task.worker_hostname == dead_worker_hostname and task.state == 'in-progress':
                    self.zk.set(f'/map_tasks/{task_file}', pickle.dumps(task._replace(worker_hostname='None')))
                    return task_file, 'map'

            # Look for such 'in-progress' task in `/shuffle_tasks`
            for task_file in self.zk.get_children('/shuffle_tasks'):
                task = self.get(f'/shuffle_tasks/{task_file}')
                if task.worker_hostname == dead_worker_hostname and task.state == 'in-progress':
                    self.zk.set(f'/shuffle_tasks/{task_file}', pickle.dumps(task._replace(worker_hostname='None')))
                    return task_file, 'shuffle'

            # Look for such 'in-progress' task in `/reduce_tasks`
            for task_file in self.zk.get_children('/reduce_tasks'):
                task = self.get(f'/reduce_tasks/{task_file}')
                if task.worker_hostname == dead_worker_hostname and task.state == 'in-progress':
                    self.zk.set(f'/reduce_tasks/{task_file}', pickle.dumps(task._replace(worker_hostname='None')))
                    return task_file, 'reduce'

            return None, None

    def investigate_job_of_dead_master(self, job_id):
        """
        Given the fact that the master that was handling this job is dead, and the fact that the job with the provided
        job_id is in-progress, this method returns the needed information to resume the job. There are five scenarios:

        1. The master died before the map phase started. Alias: 'before-map'
        2. The master died during the map phase. Alias: 'during-map'
        3. The master died before the shuffle phase started. Alias: 'before-shuffle'
        4. The master died during the shuffle phase. Alias: 'during-shuffle'
        5. The master died before the reduce phase started. Alias: 'before-reduce'
        6. The master died during the reduce phase. Alias: 'during-reduce'
        7. The master died after the reduce phase and before marking the job as completed. Alias: 'before-completion'

        For most of these scenarios there is a catch lurking around the corner. Let's take the 2. scenario as an example.
        We know we are at 2. if a map task that corresponds to this job_id is 'in-progress'. However, we don't know
        if the master died before sending the POST request. Hence, we will need to examine the Task.task_received
        attribute for each such task.

        :param job_id: The job ID of the job that was being handled by the dead master. Integer!
        """
        scenario = None

        job_map_tasks = []
        # We first iterate over all the map tasks. Reminder: The filename of a map task is of the form
        # <job_id>_<task_id>  where <job_id> is the job ID and <task_id> is the task ID.
        for task_file in self.zk.get_children('/map_tasks'):

            # Extract the job ID and task ID from the filename
            task_job_id, _ = map(int, task_file.split('_'))

            if task_job_id == job_id:
                # Get the task from ZooKeeper
                task = self.get(f'/map_tasks/{task_file}')

                job_map_tasks.append(task)

                # If any of the map tasks is 'in-progress' it means that the master died during the map phase
                if task.state == 'in-progress':
                    # The master died during the map phase
                    scenario = 'during-map'

        # If the job_map_tasks list is empty it means that the master died before the map phase started
        if not job_map_tasks:
            return {'scenario': 'before-map'}

        # If the master died during the map stage we return the scenario and the list of map tasks
        if scenario == 'during-map':
            return {'scenario': scenario, 'map_tasks': job_map_tasks}

        # We now check the shuffle task. Reminder: For each job there is only one shuffle task with filename <job_id>.
        if self.zk.exists(f'/shuffle_tasks/{job_id}'):
            # Get the shuffle task from ZooKeeper
            shuffle_task = self.get(f'/shuffle_tasks/{job_id}')

            if shuffle_task.state == 'in-progress':
                # The master died during the shuffle phase
                scenario = 'during-shuffle'

                return {'scenario': scenario, 'shuffle_task': shuffle_task}
        else:
            # The master died before the shuffle phase started
            scenario = 'before-shuffle'

            return {'scenario': scenario}

        job_reduce_tasks = []
        # We now check the reduce tasks. Reminder: The filename of a reduce task is of the form
        # <job_id>_<task_id1>_<task_id1>...
        for task_file in self.zk.get_children('/reduce_tasks'):
            # Extract the job ID and task IDs from the filename
            task_job_id, *_ = map(int, task_file.split('_'))

            if task_job_id == job_id:
                # Get the task from ZooKeeper
                task = self.get(f'/reduce_tasks/{task_file}')

                job_reduce_tasks.append(task)

                # If any of the reduce tasks is 'in-progress' it means that the master died during the reduce phase
                if task.state == 'in-progress':
                    # The master died during the reduce phase
                    scenario = 'during-reduce'

        # If the job_reduce_tasks list is empty it means that the master died before the reduce phase started
        if not job_reduce_tasks:
            return {'scenario': 'before-reduce'}

        if scenario == 'during-reduce':
            return {'scenario': scenario, 'reduce_tasks': job_reduce_tasks}

        # If we reach this point it means that the master died after the reduce phase and before marking the job as
        # completed. Remember that we entered this method because the job was in-progress. Hence, the job is not
        # completed. Therefore, the master died before marking the job as completed.
        return {'scenario': 'before-completion'}

    def get_sequential_job_id(self):
        """
        Gets a sequential integer from ZooKeeper. ZooKeeper will ensure that the sequential IDs are unique and ordered
        even when multiple application instances try to create IDs simultaneously.

        :returns: Sequential integer.
        """
        # Create a sequential z-node under the /generators/job_id_sequential path
        sequential_path = self.zk.create('/generators/job_id_sequential', sequence=True)

        # Extract the sequential number from the z-node path
        _, sequential_filename = sequential_path.rsplit('/', 1)

        # Remove the prefix "job_id_sequential" from the sequential number
        sequential_number = sequential_filename.replace("job_id_sequential", "")

        # Convert the sequential number to an integer and return it
        return int(sequential_number)

    def setup_first_job_id(self, num_persisted_jobs):
        """ HDFS persists jobs, so we need to make sure that the first job id is greater than the number of persisted
        jobs. Hence, we call this method when we start the distributed system.
        """
        for i in range(num_persisted_jobs):
            _ = self.get_sequential_job_id()

    def get(self, path):
        """
        Retrieves the data stored at the specified path in ZooKeeper.

        :param path: The path to retrieve data from.
        :return: The deserialized data retrieved from ZooKeeper.
        """
        serialized_data, _ = self.zk.get(path)
        return pickle.loads(serialized_data)

    def clear(self):
        """
        Clears the ZooKeeper state. Be careful when using this method as it will delete all the data stored in ZooKeeper.
        You can use it to reset the state of the distributed system when paired with the `clear` method of hdfs.
        You have to make sure that all jobs are completed before calling this method. We leave it up to the user to
        ensure that the system is in a consistent state before calling this method.
        """
        nodes = ["jobs", "map_tasks", "shuffle_tasks", "reduce_tasks"]

        for node in nodes:
            for child in self.zk.get_children(f'/{node}'):
                self.zk.delete(f'/{node}/{child}')

