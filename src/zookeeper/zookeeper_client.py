from typing import NamedTuple
import pickle
from kazoo.client import KazooClient

"""
/
├── workers/
│   ├── <HOSTNAME>
│   ├── ...
├── masters/
│   ├── <HOSTNAME>
│   ├── ...
├── tasks/
│   ├── <MASTER_HOSTNAME>/
│   │   ├── <TASK_ID>
│   │   ├── ...
│   ├── <MASTER_HOSTNAME2>/
│   │   ├── <TASK_ID>
│   │   ├── ...
│   ├── ...
"""


class WorkerInfo(NamedTuple):
    hostname: str
    state: str = 'idle'  # 'idle', 'in-task'


class MasterInfo(NamedTuple):
    hostname: str
    state: str = 'idle'  # 'idle', 'in-job'


class TaskInfo(NamedTuple):
    task_id: int
    worker_hostname: str
    state: str = 'idle'  # 'idle', 'in-progress', 'completed'
    worker_file_path: str = None  # where the worker saved the results of the task


class ZookeeperClient:
    def __init__(self, hosts):
        self.zk = KazooClient(hosts=hosts)
        self.zk.start()
        self.zk.ensure_path('/workers')
        self.zk.ensure_path('/masters')
        self.zk.ensure_path('/tasks')

    def register_worker(self, worker_hostname, state='idle'):
        """
        Register a worker in Zookeeper.

        :param worker_hostname: The hostname of the worker inside the Docker-Compose network.
        :param state: The state of the worker. Defaults to 'idle'.
        """
        path = f'/workers/{worker_hostname}'
        data = WorkerInfo(worker_hostname, state)
        self.zk.create(path, pickle.dumps(data), ephemeral=True)

    def register_master(self, master_hostname):
        """
        Register a master in Zookeeper.

        :param master_hostname: The hostname of the master inside the Docker-Compose network.
        """
        path = f'/master/{master_hostname}'
        data = MasterInfo(master_hostname)
        self.zk.create(path, pickle.dumps(data), ephemeral=True)

        # Create folder /tasks/<master_hostname>
        self.zk.ensure_path(f'/tasks/{master_hostname}')

    def register_task(self, master_hostname, worker_hostname, task_id):
        """
        Register a task in Zookeeper. The master registers the tasks and provides a unique `task_id`.
        More specifically, the `task_id` is unique to the specific master and will be stored in
        tasks/<MASTER_HOSTNAME>/<TASK_ID>

        :param master_hostname: The hostname of the master inside the Docker-Compose network.
        :param worker_hostname: The hostname of the worker who will perform the task
        :param task_id: The unique task id
        """
        path = f'tasks/{master_hostname}/{task_id}'
        data = TaskInfo(task_id, worker_hostname)
        self.zk.create(path, pickle.dumps(data), ephemeral=True)

    def update_worker_state(self, worker_hostname, state):
        """
        Update the state of a worker in Zookeeper.

        :param worker_hostname: The hostname of the worker inside the Docker-Compose network.
        :param state: The new state of the worker.
        """

        path = f'/workers/{worker_hostname}'
        data, _ = self.zk.get(path)
        worker_info = pickle.loads(data)
        updated_worker_info = worker_info._replace(state=state)
        self.zk.set(path, pickle.dumps(updated_worker_info))

    def update_task(self, task_id, master_hostname, state, worker_file_path=None):
        """
        Update task

        :param master_hostname: The hostname of the master inside the Docker-Compose network.
        :param task_id: The unique task id
        :param state: The new state of the task 'in-progress' or 'completed'
        :param worker_file_path: The file path in the worker container of the 'completed' task, else None
        """
        path = f'tasks/{master_hostname}/{task_id}'
        data, _ = self.zk.get(path)
        task_info = pickle.loads(data)
        updated_task_info = task_info._replace(state=state, worker_file_path=worker_file_path)
        self.zk.set(path, pickle.dumps(updated_task_info))

    def get_worker_state(self, worker_hostname):
        """
        Retrieve the state of a worker from Zookeeper.

        :param worker_hostname: The hostname of the worker inside the Docker-Compose network.
        :return: `WorkerInfo`
        """
        path = f'/workers/{worker_hostname}'
        data, _ = self.zk.get(path)
        worker_info = pickle.loads(data)
        return worker_info

    def get_idle_workers(self):
        """
        Retrieve information about all workers that are currently in the "idle" state.

        :return: A list of WorkerInfo for all workers in the "idle" state.
        """
        worker_hostnames = self.zk.get_children('/workers')
        idle_workers = []
        for worker_hostname in worker_hostnames:
            path = f'/workers/{worker_hostname}'
            data, _ = self.zk.get(path)
            worker_info = pickle.loads(data)
            if worker_info.state == 'idle':
                idle_workers.append(worker_info)
        return idle_workers

