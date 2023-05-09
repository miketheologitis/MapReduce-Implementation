from typing import NamedTuple
import pickle
from kazoo.client import KazooClient


class WorkerInfo(NamedTuple):
    ip: str
    port: int
    state: str = 'idle'
    task_file: str = None


class ZookeeperClient:
    def __init__(self, hosts):
        self.zk = KazooClient(hosts=hosts)
        self.zk.start()
        self.zk.ensure_path('/workers')

    def register_worker(self, worker_id, worker_ip, worker_port, state='idle', task_file=None):
        """
        Register a worker in Zookeeper.

        :param worker_id: The ID of the worker.
        :param worker_ip: The IP address of the worker.
        :param worker_port: The port of the worker.
        :param state: The state of the worker. Defaults to 'idle'.
        :param task_file: The file path of the task assigned to the worker. Defaults to None.
        """
        path = f'/workers/{worker_id}'
        data = WorkerInfo(worker_ip, worker_port, state, task_file)
        self.zk.create(path, pickle.dumps(data), ephemeral=True)

    def update_worker_state(self, worker_id, state, task_file=None):
        """
        Update the state of a worker in Zookeeper.

        :param worker_id: The ID of the worker.
        :param state: The new state of the worker.
        :param task_file: The new file path of the task assigned to the worker.
        """

        path = f'/workers/{worker_id}'
        data = self.zk.get(path)
        worker_info = pickle.loads(data[0])
        updated_info = worker_info._replace(state=state, task_file=task_file)
        self.zk.set(path, pickle.dumps(updated_info))

