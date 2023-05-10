from typing import NamedTuple
import pickle
from kazoo.client import KazooClient


class WorkerInfo(NamedTuple):
    hostname: str
    state: str = 'idle'
    task_file: str = None


class ZookeeperClient:
    def __init__(self, hosts):
        self.zk = KazooClient(hosts=hosts)
        self.zk.start()
        self.zk.ensure_path('/workers')

    def register_worker(self, worker_hostname, state='idle', task_file=None):
        """
        Register a worker in Zookeeper.

        :param worker_hostname: The hostname of the worker inside the Docker-Compose network.
        :param state: The state of the worker. Defaults to 'idle'.
        :param task_file: The file path of the task assigned to the worker. Defaults to None.
        """
        path = f'/workers/{worker_hostname}'
        data = WorkerInfo(worker_hostname, state, task_file)
        self.zk.create(path, pickle.dumps(data), ephemeral=True)

    def update_worker_state(self, worker_hostname, state, task_file=None):
        """
        Update the state of a worker in Zookeeper.

        :param worker_hostname: The hostname of the worker inside the Docker-Compose network.
        :param state: The new state of the worker.
        :param task_file: The new file path of the task assigned to the worker.
        """

        path = f'/workers/{worker_hostname}'
        data, _ = self.zk.get(path)
        worker_info = pickle.loads(data)
        updated_info = worker_info._replace(state=state, task_file=task_file)
        self.zk.set(path, pickle.dumps(updated_info))

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

