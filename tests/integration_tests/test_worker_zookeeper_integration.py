import unittest
import requests
import subprocess
import yaml
import os
import pickle
import time
from src.zookeeper.zookeeper_client import ZookeeperClient


class TestWorker(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # Deploy the zookeeper containers and two worker containers
        subprocess.run(
            ['docker-compose', 'up', '-d', '--scale', 'worker=2', '--no-recreate'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        # This was needed because even though the docker-compose ends, containers
        # need some more time to be reachable
        time.sleep(10)

        cls.num_docker_workers = 0
        cls.worker_hostnames = []

        docker_compose_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../docker-compose.yaml'))

        with open(docker_compose_file) as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            zoo1_port = data['services']['zoo1']['ports'][0].split(':')[0]
            zoo2_port = data['services']['zoo2']['ports'][0].split(':')[0]
            zoo3_port = data['services']['zoo3']['ports'][0].split(':')[0]
            zk_hosts = f'127.0.0.1:{zoo1_port},127.0.0.1:{zoo2_port},127.0.0.1:{zoo3_port}'
        # We connect to zookeeper from the open ports in localhost
        cls.zk_client = ZookeeperClient(zk_hosts)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.zk_client.zk.stop()
        cls.zk_client.zk.close()

        # Run `docker-compose down`
        subprocess.run(
            ['docker-compose', 'down'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

    def test_live_zookeeper(self):
        self.assertTrue(self.zk_client.zk.connected)

    def test_scale_workers(self):
        self._test_add_n_workers(1)
        self._test_add_n_workers(2)
        self._test_add_n_workers(3)
        self._test_add_n_workers(5)

    def _test_add_n_workers(self, n):
        subprocess.run(
            ['docker-compose', 'up', '-d', '--scale', f'worker={self.num_docker_workers+n}', '--no-recreate'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        # This was needed because even though the docker-compose ends, containers
        # need some more time to be reachable
        time.sleep(10)

        self._test_no_workers_destroyed_in_scaling(n)
        self.num_docker_workers += n
        self.worker_hostnames = self.zk_client.zk.get_children('/workers')
        self._test_workers_registered_with_zookeeper()

    def _test_no_workers_destroyed_in_scaling(self, n):
        new_worker_hostnames = set(self.zk_client.zk.get_children('/workers'))
        new_hostnames = new_worker_hostnames.difference(set(self.worker_hostnames))
        self.assertEqual(len(new_hostnames), n)

    def _test_workers_registered_with_zookeeper(self):
        self.assertTrue(len(self.worker_hostnames) == self.num_docker_workers)

        # Distinct hostnames
        self.assertCountEqual(self.worker_hostnames, list(set(self.worker_hostnames)))

        for worker_hostname in self.worker_hostnames:
            path = f'/workers/{worker_hostname}'
            data, _ = self.zk_client.zk.get(path)
            worker_info = pickle.loads(data)

            self.assertEqual(worker_info.state, 'idle')
            self.assertIsNotNone(worker_info.hostname)


if __name__ == '__main__':
    unittest.main()
