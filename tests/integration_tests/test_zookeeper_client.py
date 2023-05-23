import unittest
import subprocess
from mapreduce.zookeeper.zookeeper_client import ZookeeperClient


import logging

logging.disable(logging.CRITICAL)


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

        cls.zk_client = ZookeeperClient("127.0.0.1:2181")
        cls.zk_client.setup_paths()

    def test_setup_paths(self):
        """Test case to ensure that the necessary paths are set up in ZooKeeper."""
        children = self.zk_client.zk.get_children('/')

        expected_children = [
            'workers', 'masters', 'map_tasks', 'locks', 'dead_tasks',
            'shuffle_tasks', 'reduce_tasks', 'generators', 'zookeeper', 'jobs'
        ]

        self.assertCountEqual(children, expected_children)

    def test_job_ops(self):
        """Test case to perform job operations in ZooKeeper and verify their correctness."""
        self.zk_client.clear()

        job_ids = []

        # Generate job IDs and register jobs in ZooKeeper
        for _ in range(100):
            job_id = self.zk_client.get_sequential_job_id()
            self.assertIsInstance(job_id, int)
            self.zk_client.register_job(job_id)
            job_ids.append(job_id)

        assigned_jobs = []

        # Assign jobs to masters and verify job state and assigned master
        for i in range(10):
            got_job = self.zk_client.get_job('master1', i)
            self.assertTrue(got_job)
            assigned_jobs.append(i)

        # Verify the state and assigned master of each job
        for job_id in job_ids:
            job = self.zk_client.get(f'jobs/{job_id}')
            if job_id not in assigned_jobs:
                self.assertTrue(job.state == 'idle')
                self.assertIsNone(job.master_hostname)
            else:
                self.assertTrue(job.state == 'in-progress')
                self.assertTrue(job.master_hostname == 'master1')
            self.assertIsNone(job.requested_n_workers)

        self._remove_masters_workers()
        self.zk_client.clear()

    def test_register_worker(self):
        """Test case to register workers in ZooKeeper and verify their registration."""

        idle_workers = []

        for i in range(10):
            self.zk_client.register_worker(f'worker{i}')
            idle_workers.append(f'worker{i}')

        self.assertCountEqual(self.zk_client.zk.get_children('/workers/'), idle_workers)

        self._remove_masters_workers()
        self.zk_client.clear()

    def test_register_master(self):
        """Test case to register masters in ZooKeeper and verify their registration."""
        masters = []

        for i in range(10):
            self.zk_client.register_master(f'master{i}')
            masters.append(f'master{i}')

        self.assertCountEqual(self.zk_client.zk.get_children('/masters/'), masters)

    def test_task_ops(self):
        """Test case to perform task operations in ZooKeeper and verify their correctness."""
        # Assert correct creations
        self.zk_client.register_task(task_type='map', job_id=1, state='idle', worker_hostname='123', task_id=1)
        self.zk_client.register_task(task_type='shuffle', state='idle', worker_hostname='123', job_id=1)
        self.zk_client.register_task(task_type='reduce', state='idle', job_id=1, worker_hostname='123', task_id=[0, 1, 2, 3, 4])

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

        self._remove_masters_workers()
        self.zk_client.clear()

    def test_update_worker(self):
        """Test case to update worker information in ZooKeeper and verify the updates."""
        self.zk_client.register_worker('worker99')
        self.zk_client.update_worker('worker99', state='in-task')

        self.assertTrue(self.zk_client.get('/workers/worker99').state == 'in-task')

        self._remove_masters_workers()
        self.zk_client.clear()

    def test_sequential_job_id(self):
        """Test case to verify the sequential job ID generation in ZooKeeper."""
        increasing_ids = []
        for _ in range(100):
            increasing_ids.append(self.zk_client.get_sequential_job_id())

        self.assertListEqual(increasing_ids, sorted(increasing_ids))
        self.assertCountEqual(increasing_ids, list(set(increasing_ids)))

    def test_get_workers_for_tasks(self):
        """Test case to retrieve idle workers from ZooKeeper for tasks and verify the results."""

        idle_workers = []

        for i in range(30, 50):
            self.zk_client.register_worker(f'worker{i}')
            idle_workers.append(f'worker{i}')

        # Ask for all workers
        workers = self.zk_client.get_workers_for_tasks(1000)

        self.assertCountEqual(workers, idle_workers)

        # Assert that ZooKeeper made them 'in-task'
        for worker in workers:
            self.assertTrue(self.zk_client.get(f'/workers/{worker}').state == 'in-task')

        self._remove_masters_workers()
        self.zk_client.clear()

    def test_single_worker_bug(self):
        # Make all workers' state 'idle'
        idle_workers = []

        for i in range(10):
            self.zk_client.register_worker(f'worker{i}')

        for worker_hostname in self.zk_client.zk.get_children('/workers'):
            self.zk_client.update_worker(worker_hostname, state='idle')
            idle_workers.append(f'{worker_hostname}')

        # Get only one worker
        assigned_worker = self.zk_client.get_workers_for_tasks(1)

        self.assertTrue(len(assigned_worker) == 1)

        # Make all workers' state 'idle'
        num_in_task = 0
        for worker_hostname in self.zk_client.zk.get_children('/workers'):
            worker = self.zk_client.get(f'/workers/{worker_hostname}')
            if worker.state == 'in-task':
                num_in_task += 1

        self.assertEqual(num_in_task, 1)

        self._remove_masters_workers()
        self.zk_client.clear()

    def test_investigate_job_of_dead_master_scenario1(self):
        self.zk_client.clear()

        """
        Test the investigate_job_of_dead_master function.
        We test the scenario 1., i.e.,
        1. The master died before the map phase started. Alias: 'before-map'
        """
        self.zk_client.register_job(job_id=1, requested_n_workers=2)
        self.zk_client.update_job(job_id=1, master_hostname='master1', state='in-progress')

        info_dict = self.zk_client.investigate_job_of_dead_master(1)

        self.assertTrue(info_dict['scenario'] == 'before-map')

        self.zk_client.clear()

    def test_investigate_job_of_dead_master_scenario2(self):
        """
        Test the investigate_job_of_dead_master function.
        We test the scenario 2., i.e.,
        2. The master died during the map phase. Alias: 'during-map'
        """
        self.zk_client.clear()

        self.zk_client.register_job(job_id=2, requested_n_workers=2)
        self.zk_client.update_job(job_id=2, master_hostname='master2', state='in-progress')

        self.zk_client.register_task(
            task_type='map', job_id=2, state='in-progress', worker_hostname='worker1', task_id=1
        )
        self.zk_client.register_task(
            task_type='map', job_id=2, state='completed', worker_hostname='worker1', task_id=2
        )
        self.zk_client.register_task(
            task_type='map', job_id=2, state='completed', worker_hostname='worker1', task_id=3
        )

        info_dict = self.zk_client.investigate_job_of_dead_master(2)

        self.assertTrue(info_dict['scenario'] == 'during-map')
        self.assertTrue(len(info_dict['map_tasks']) == 3)

        self.zk_client.clear()

    def test_investigate_job_of_dead_master_scenario3(self):
        """
        Test the investigate_job_of_dead_master function.
        3. The master died before the shuffle phase started. Alias: 'before-shuffle'
        """
        self.zk_client.register_job(job_id=3, requested_n_workers=2)
        self.zk_client.update_job(job_id=3, master_hostname='master2', state='in-progress')

        self.zk_client.register_task(
            task_type='map', job_id=3, state='completed', worker_hostname='worker1', task_id=1
        )
        self.zk_client.register_task(
            task_type='map', job_id=3, state='completed', worker_hostname='worker1', task_id=2
        )
        self.zk_client.register_task(
            task_type='map', job_id=3, state='completed', worker_hostname='worker1', task_id=3
        )

        # add shuffle task for other job
        self.zk_client.register_task(
            task_type='shuffle', job_id=99, state='completed', worker_hostname='worker1'
        )

        # add shuffle task for other job
        self.zk_client.register_task(
            task_type='shuffle', job_id=199, state='completed', worker_hostname='worker1'
        )

        info_dict = self.zk_client.investigate_job_of_dead_master(3)

        self.assertTrue(info_dict['scenario'] == 'before-shuffle')

    def test_investigate_job_of_dead_master_scenario4(self):
        """
        Test the investigate_job_of_dead_master function.
        4. The master died during the shuffle phase. Alias: 'during-shuffle'
        """
        self.zk_client.register_job(job_id=4, requested_n_workers=2)
        self.zk_client.update_job(job_id=4, master_hostname='master2', state='in-progress')

        self.zk_client.register_task(
            task_type='map', job_id=4, state='completed', worker_hostname='worker1', task_id=1
        )
        self.zk_client.register_task(
            task_type='map', job_id=4, state='completed', worker_hostname='worker1', task_id=2
        )
        self.zk_client.register_task(
            task_type='map', job_id=4, state='completed', worker_hostname='worker1', task_id=3
        )

        self.zk_client.register_task(
            task_type='shuffle', job_id=4, state='in-progress', worker_hostname='worker1'
        )

        info_dict = self.zk_client.investigate_job_of_dead_master(4)

        self.assertTrue(info_dict['scenario'] == 'during-shuffle')
        self.assertTrue(info_dict['shuffle_task'].worker_hostname == 'worker1')

    def test_investigate_job_of_dead_master_scenario5(self):
        """
        Test the investigate_job_of_dead_master function.
        5. The master died before the reduce phase started. Alias: 'before-reduce'
        """
        self.zk_client.register_job(job_id=5, requested_n_workers=2)
        self.zk_client.update_job(job_id=5, master_hostname='master2', state='in-progress')

        self.zk_client.register_task(
            task_type='map', job_id=5, state='completed', worker_hostname='worker1', task_id=1
        )
        self.zk_client.register_task(
            task_type='map', job_id=5, state='completed', worker_hostname='worker1', task_id=2
        )
        self.zk_client.register_task(
            task_type='map', job_id=5, state='completed', worker_hostname='worker1', task_id=3
        )

        self.zk_client.register_task(
            task_type='shuffle', job_id=5, state='completed', worker_hostname='worker1'
        )

        info_dict = self.zk_client.investigate_job_of_dead_master(5)

        self.assertTrue(info_dict['scenario'] == 'before-reduce')

    def test_investigate_job_of_dead_master_scenario6(self):
        """
        Test the investigate_job_of_dead_master function.
        6. The master died during the reduce phase. Alias: 'during-reduce'
        """
        self.zk_client.register_job(job_id=6, requested_n_workers=2)
        self.zk_client.update_job(job_id=6, master_hostname='master2', state='in-progress')

        self.zk_client.register_task(
            task_type='map', job_id=6, state='completed', worker_hostname='worker1', task_id=1
        )
        self.zk_client.register_task(
            task_type='map', job_id=6, state='completed', worker_hostname='worker1', task_id=2
        )
        self.zk_client.register_task(
            task_type='map', job_id=6, state='completed', worker_hostname='worker1', task_id=3
        )

        self.zk_client.register_task(
            task_type='shuffle', job_id=6, state='completed', worker_hostname='worker1'
        )

        self.zk_client.register_task(
            task_type='reduce', job_id=6, task_id=[1, 2], state='in-progress', worker_hostname='worker1'
        )

        self.zk_client.register_task(
            task_type='reduce', job_id=6, task_id=[3, 4, 5], state='completed', worker_hostname='worker1'
        )

        info_dict = self.zk_client.investigate_job_of_dead_master(6)

        self.assertTrue(info_dict['scenario'] == 'during-reduce')
        self.assertTrue(len(info_dict['reduce_tasks']) == 2)

    def test_investigate_job_of_dead_master_scenario7(self):
        """
        Test the investigate_job_of_dead_master function.
        7. The master died after the reduce phase and before marking the job as completed. Alias: 'before-completion'
        """
        self.zk_client.register_job(job_id=7, requested_n_workers=2)
        self.zk_client.update_job(job_id=7, master_hostname='master2', state='in-progress')

        self.zk_client.register_task(
            task_type='map', job_id=7, state='completed', worker_hostname='worker1', task_id=1
        )
        self.zk_client.register_task(
            task_type='map', job_id=7, state='completed', worker_hostname='worker1', task_id=2
        )
        self.zk_client.register_task(
            task_type='map', job_id=7, state='completed', worker_hostname='worker1', task_id=3
        )

        self.zk_client.register_task(
            task_type='shuffle', job_id=7, state='completed', worker_hostname='worker1'
        )

        self.zk_client.register_task(
            task_type='reduce', job_id=7, task_id=[1, 2], state='completed', worker_hostname='worker1'
        )

        self.zk_client.register_task(
            task_type='reduce', job_id=7, task_id=[3, 4, 5], state='completed', worker_hostname='worker1'
        )

        info_dict = self.zk_client.investigate_job_of_dead_master(7)

        self.assertTrue(info_dict['scenario'] == 'before-completion')

    def _remove_masters_workers(self):
        for master in self.zk_client.zk.get_children('/masters'):
            self.zk_client.zk.delete(f'/masters/{master}')

        for worker in self.zk_client.zk.get_children('/workers'):
            self.zk_client.zk.delete(f'/workers/{worker}')

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
