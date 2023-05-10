import unittest

import requests
import subprocess
import yaml
from src.zookeeper.zookeeper_client import ZookeeperClient
import os
import pickle
import sys
import time


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

    @classmethod
    def tearDownClass(cls) -> None:
        # Run `docker-compose down`
        subprocess.run(
            ['docker-compose', 'down'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

    def setUp(self):
        docker_compose_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../docker-compose.yaml'))

        with open(docker_compose_file) as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            zoo1_port = data['services']['zoo1']['ports'][0].split(':')[0]
            zoo2_port = data['services']['zoo2']['ports'][0].split(':')[0]
            zoo3_port = data['services']['zoo3']['ports'][0].split(':')[0]
            zk_hosts = f'127.0.0.1:{zoo1_port},127.0.0.1:{zoo2_port},127.0.0.1:{zoo3_port}'
        # We connect to zookeeper from the open ports in localhost
        self.zk_client = ZookeeperClient(zk_hosts)

    def tearDown(self):
        self.zk_client.zk.stop()
        self.zk_client.zk.close()

    def test_live_zookeeper(self):
        self.assertTrue(self.zk_client.zk.connected)

    def test_fist_two_registered_with_zookeeper(self):
        self._test_n_workers_registered_with_zookeeper(2)

    def _test_n_workers_registered_with_zookeeper(self, n):
        worker_hostnames = self.zk_client.zk.get_children('/workers')

        self.assertTrue(len(worker_hostnames) == n)

        # Distinct hostnames
        self.assertCountEqual(worker_hostnames, list(set(worker_hostnames)))

        for worker_hostname in worker_hostnames:
            path = f'/workers/{worker_hostname}'
            data, _ = self.zk_client.zk.get(path)
            worker_info = pickle.loads(data)

            self.assertEqual(worker_info.state, 'idle')
            self.assertIsNone(worker_info.task_file)
            self.assertIsNotNone(worker_info.hostname)


if __name__ == '__main__':
    unittest.main()
