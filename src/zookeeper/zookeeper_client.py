from typing import NamedTuple
import pickle
from kazoo.client import KazooClient

"""
/
├── workers/    (created by `user` `not ephemeral`)
│   ├── <HOSTNAME>      (created by `worker` on register `ephemeral`)
│   ├── ...
├── masters/        (created by `user` `not ephemeral`)
│   ├── <HOSTNAME>      (created by `master` on register `ephemeral`)
│   ├── ...
├── tasks/      (created by `user` `not ephemeral`)
│   ├── <job_id>_<type>_<task_id>   (see down)
├── generators/  (created by `user` `not ephemeral`)
│   ├── job_id_sequential


About `tasks`:
<job_id>_<type>_<task_id>  :  `TaskInfo`. created by `master` and also modified by `master` (`worker_hostname`, 
    `state` to 'in-progress' when `master` assigns a worker). The `worker` modifies `state` when job finishes.
"""


class WorkerInfo(NamedTuple):
    hostname: str
    state: str = 'idle'  # 'idle', 'in-task'


class MasterInfo(NamedTuple):
    hostname: str


class TaskInfo(NamedTuple):
    """
    Represents task information. The idea is that given `job_id`-`type`-`task_id` the worker
    can run the task (using HDFS).

    :param job_id: Unique job ID.
    :param task_id: Task ID (unique for the `job`-`type` combination).
    :param task_type: Task type ('map', 'shuffle', 'reduce').
    :param worker_hostname: Worker hostname that runs the task.
    :param state: Task state ('idle', 'in-progress', 'completed').
    """
    job_id: int
    task_id: int
    task_type: str
    worker_hostname: str = None
    state: str = 'idle'


class ZookeeperClient:
    def __init__(self, hosts):
        self.zk = KazooClient(hosts=hosts)
        self.zk.start()

    def setup_paths(self):
        """Creates the necessary directories in ZooKeeper."""
        paths = ['/workers', '/masters', '/tasks', '/generators']
        for path in paths:
            self.zk.create(path)

    def register_worker(self, worker_hostname):
        """
        Registers a worker in ZooKeeper.

        :param worker_hostname: Hostname of the worker.
        """
        worker_info = WorkerInfo(hostname=worker_hostname)
        serialized_worker_info = pickle.dumps(worker_info)
        # The `ephemeral` flag ensures that the z-node is automatically deleted if
        # the master disconnects
        self.zk.create(f'/workers/{worker_hostname}', serialized_worker_info, ephemeral=True)

    def register_master(self, master_hostname):
        """
        Registers a master in ZooKeeper.

        :param master_hostname: Hostname of the master.
        """
        master_info = MasterInfo(hostname=master_hostname)
        serialized_master_info = pickle.dumps(master_info)
        # The `ephemeral` flag ensures that the z-node is automatically deleted if
        # the master disconnects
        self.zk.create(f'/masters/{master_hostname}', serialized_master_info, ephemeral=True)

    def register_task(self, job_id, task_type, task_id):
        """
        Registers a task in ZooKeeper.

        :param job_id: Unique job ID.
        :param task_type: Task type ('map', 'shuffle', 'reduce').
        :param task_id: Task ID.
        """
        task_info = TaskInfo(job_id=job_id, task_type=task_type, task_id=task_id)
        serialized_task_info = pickle.dumps(task_info)
        task_path = f'/tasks/{job_id}_{task_type}_{task_id}'
        self.zk.create(task_path, serialized_task_info)

    def update_task(self, job_id, task_type, task_id, **kwargs):
        """
        Updates task information in ZooKeeper.

        :param job_id: Unique job ID.
        :param task_type: Task type ('map', 'shuffle', 'reduce').
        :param task_id: Task ID.
        :param kwargs: Attributes to update in the task.
        """
        task_path = f'/tasks/{job_id}_{task_type}_{task_id}'
        serialized_task_info, _ = self.zk.get(task_path)
        task_info = pickle.loads(serialized_task_info)
        updated_task_info = task_info._replace(**kwargs)
        self.zk.set(task_path, pickle.dumps(updated_task_info))

    def update_worker(self, worker_hostname, **kwargs):
        """
        Updates worker information in ZooKeeper.

        :param worker_hostname: Hostname of the worker.
        :param kwargs: Additional attributes to update in the worker.
        """
        worker_path = f'/workers/{worker_hostname}'
        serialized_worker_info, _ = self.zk.get(worker_path)
        worker_info = pickle.loads(serialized_worker_info)
        updated_worker_info = worker_info._replace(**kwargs)
        self.zk.set(worker_path, pickle.dumps(updated_worker_info))

    def get_sequential_integer(self):
        """
        Gets a sequential integer from ZooKeeper.

        :returns: Sequential integer.
        """
        # Create a sequential z-node under the /generators/job_id_sequential path
        sequential_path = self.zk.create('/generators/job_id_sequential', sequence=True)

        # Extract the sequential number from the z-node path
        _, sequential_number = sequential_path.rsplit('/', 1)

        # Convert the sequential number to an integer and return it
        return int(sequential_number)







