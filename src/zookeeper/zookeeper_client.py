from typing import NamedTuple, List
import pickle
from kazoo.client import KazooClient
from kazoo.recipe.lock import Lock

"""
/
├── workers/    (created by `user` `not ephemeral`)
│   ├── <HOSTNAME>      (created by `worker` on register `ephemeral`)
│   ├── ...
├── masters/        (created by `user` `not ephemeral`)
│   ├── <HOSTNAME>      (created by `master` on register `ephemeral`)
│   ├── ...
├── jobs/
│   ├── <HOSTNAME>  `master` hostname. (created by ?)
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

"""


class WorkerInfo(NamedTuple):
    """
    :param state: Task state ('idle', 'in-task').
    """
    state: str = 'idle'  # 'idle', 'in-task'


class MasterInfo(NamedTuple):
    state: str = 'nothing'


class Task(NamedTuple):
    """
    :param worker_hostname: Worker hostname that runs the task.
    :param state: Task state ('idle', 'in-progress', 'completed').
    """
    worker_hostname: str = None
    state: str = 'idle'


class ZookeeperClient:
    def __init__(self, hosts):
        self.zk = KazooClient(hosts=hosts)
        self.zk.start()

    def setup_paths(self):
        """Creates the necessary directories in ZooKeeper."""
        paths = ['/workers', '/masters', '/map_tasks', '/shuffle_tasks', '/reduce_tasks', '/generators', 'locks']
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

    def register_task(self, task_type, job_id, task_id=None):
        """
        Registers a task in ZooKeeper.

        :param task_type: Task type ('map', 'reduce', 'shuffle').
        :param job_id: Unique job ID.
        :param task_id: Task ID (optional for 'map' and 'shuffle' tasks).
        """
        task = Task()
        serialized_task = pickle.dumps(task)

        if task_type == 'map':
            self.zk.create(f'/map_tasks/{job_id}_{task_id}', serialized_task)
        elif task_type == 'reduce':
            suffix_id = '_'.join(map(str, task_id))
            self.zk.create(f'/reduce_tasks/{job_id}_{suffix_id}', serialized_task)
        else:
            self.zk.create(f'/shuffle_tasks/{job_id}', serialized_task)

    def update_task(self, task_type, job_id, task_id=None, **kwargs):
        """
        Updates task information in ZooKeeper.

        :param task_type: Task type ('map', 'reduce', 'shuffle').
        :param job_id: Unique job ID.
        :param task_id: Task ID (optional for 'map' and 'shuffle' tasks).
        :param kwargs: Additional attributes to update in the task.
        """
        if task_type == 'map':
            task_path = f'/map_tasks/{job_id}_{task_id}'
        elif task_type == 'reduce':
            suffix_id = '_'.join(map(str, task_id))
            task_path = f'/reduce_tasks/{job_id}_{suffix_id}'
        else:
            task_path = f'/shuffle_tasks/{job_id}'

        serialized_task, _ = self.zk.get(task_path)
        task = pickle.loads(serialized_task)
        updated_task = task._replace(**kwargs)
        self.zk.set(task_path, pickle.dumps(updated_task))

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

    def get_workers_for_tasks(self, n):
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
                serialized_worker_info, _ = self.zk.get(worker_path)
                worker_info = pickle.loads(serialized_worker_info)

                if worker_info.state == 'idle':
                    # Update the worker state to 'in-task'
                    self.update_worker(hostname, state='in-task')

                    # Add the worker hostname to the list of idle workers
                    idle_workers.append(hostname)

                    if len(idle_workers) == n:
                        # If the desired number of idle workers is reached, break the loop
                        break

        return idle_workers

    def get(self, path):
        serialized_data, _ = self.zk.get(path)
        return pickle.loads(serialized_data)

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
