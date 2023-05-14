import unittest
import subprocess
import time
from src.zookeeper.zookeeper_client import ZookeeperClient


class TestZookeeper(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # Deploy the zookeeper containers and two worker containers
        subprocess.run(
            ['docker-compose', 'up', '-d', '--scale', 'worker=0',
             '--scale', 'master=0', '--scale', 'namenode=0', '--scale', 'datanode=0',
             '--scale', 'resourcemanager=0', '--scale', 'nodemanager1=0', '--no-recreate'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        # This was needed because even though the docker-compose ends, containers
        # need some more time to be reachable
        time.sleep(15)

        tries = 5
        for _ in range(tries):
            try:
                cls.zk_client = ZookeeperClient("127.0.0.1:2181")
                break
            except Exception as e:
                time.sleep(5)
                continue

        cls.zk_client.setup_paths()

        cls.idle_workers = []
        cls.masters = []

    def test_setup_paths(self):
        """Test case to ensure that the necessary paths are set up in ZooKeeper."""
        children = self.zk_client.zk.get_children('/')

        expected_children = [
            'workers', 'masters', 'map_tasks', 'locks',
            'shuffle_tasks', 'reduce_tasks', 'generators', 'zookeeper'
        ]

        self.assertCountEqual(children, expected_children)

    def test_register_worker(self):
        """Test case to register workers in ZooKeeper and verify their registration."""
        for i in range(10):
            self.zk_client.register_worker(f'worker{i}')
            self.idle_workers.append(f'worker{i}')

        self.assertCountEqual(self.zk_client.zk.get_children('/workers/'), self.idle_workers)

    def test_register_master(self):
        """Test case to register masters in ZooKeeper and verify their registration."""
        for i in range(10):
            self.zk_client.register_master(f'master{i}')
            self.masters.append(f'master{i}')

        self.assertCountEqual(self.zk_client.zk.get_children('/masters/'), self.masters)

    def test_task_ops(self):
        """Test case to perform task operations in ZooKeeper and verify their correctness."""
        # Assert correct creations
        self.zk_client.register_task(task_type='map', job_id=1, task_id=1)
        self.zk_client.register_task(task_type='shuffle', job_id=1)
        self.zk_client.register_task(task_type='reduce', job_id=1, task_id=[0, 1, 2, 3, 4])

        self.assertTrue(self.zk_client.zk.exists('/map_tasks/1_1'))
        self.assertTrue(self.zk_client.zk.exists('/shuffle_tasks/1'))
        self.assertTrue(self.zk_client.zk.exists('/reduce_tasks/1_0_1_2_3_4'))

        # Assert correct updates
        self.zk_client.update_task(task_type='map', job_id=1, task_id=1, state='in-progress')
        self.zk_client.update_task(task_type='shuffle', job_id=1, worker_hostname='worker1')
        self.zk_client.update_task(task_type='reduce', job_id=1, task_id=[0, 1, 2, 3, 4], state='completed')
        self.assertTrue(self.zk_client.get('/map_tasks/1_1').state == 'in-progress')
        self.assertTrue(self.zk_client.get('/shuffle_tasks/1').worker_hostname == 'worker1')
        self.assertTrue(self.zk_client.get('/reduce_tasks/1_0_1_2_3_4').state == 'completed')

    def test_update_worker(self):
        """Test case to update worker information in ZooKeeper and verify the updates."""
        self.zk_client.register_worker('worker99')
        self.zk_client.update_worker('worker99', state='in-task')

        self.assertTrue(self.zk_client.get('/workers/worker99').state == 'in-task')

    def test_sequential_job_id(self):
        """Test case to verify the sequential job ID generation in ZooKeeper."""
        increasing_ids = []
        for _ in range(100):
            increasing_ids.append(self.zk_client.get_sequential_job_id())

        self.assertListEqual(increasing_ids, sorted(increasing_ids))
        self.assertCountEqual(increasing_ids, list(set(increasing_ids)))

    def test_get_workers_for_tasks(self):
        """Test case to retrieve idle workers from ZooKeeper for tasks and verify the results."""
        for i in range(30, 50):
            self.zk_client.register_worker(f'worker{i}')
            self.idle_workers.append(f'worker{i}')

        # Ask for all workers
        workers = self.zk_client.get_workers_for_tasks(1000)

        self.assertCountEqual(workers, self.idle_workers)

        # Assert that ZooKeeper made them 'in-task'
        for worker in workers:
            self.assertTrue(self.zk_client.get(f'/workers/{worker}').state == 'in-task')

    @classmethod
    def tearDownClass(cls) -> None:
        """Tear down the ZooKeeper client and shut down the containers."""
        cls.zk_client.zk.stop()
        cls.zk_client.zk.close()

        # Run `docker-compose down`
        subprocess.run(
            ['docker-compose', 'down'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )