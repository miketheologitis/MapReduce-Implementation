import unittest
import subprocess
import time
from src.hadoop.hdfs_client import HdfsClient
from operator import itemgetter
from itertools import groupby, chain
from functools import reduce


class TestHdfs(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # Deploy the zookeeper containers and two worker containers
        subprocess.run(
            ['docker-compose', 'up', '-d', '--scale', 'worker=0',
             '--scale', 'master=0', '--scale', 'zoo1=0', '--scale', 'zoo2=0',
             '--scale', 'zoo3=0', '--no-recreate'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        # This was needed because even though the docker-compose ends, containers
        # need some more time to be reachable
        time.sleep(15)

        cls.hdfs_client = HdfsClient("localhost:9870")

        tries = 5
        for _ in range(tries):
            try:
                cls.hdfs_client = HdfsClient("localhost:9870")
                cls.hdfs_client.initialize_job_dir()
                break
            except Exception as e:
                time.sleep(5)
                continue

    def test_job_create_dirs(self):
        for i in range(10):
            self.hdfs_client.job_create_dirs(job_id=i)
            self._test_job_create_dirs(job_id=i)

        self._test_cleanup_jobs_dir()

    def test_save_retrieve_data(self):
        data = [("mike1",), ("george",), ("1g2eommrg3",)]

        self.hdfs_client.job_create_dirs(job_id=50)
        self.hdfs_client.save_data('jobs/job_50/data.pickle', data)

        retrieved_data = self.hdfs_client.get_data('jobs/job_50/data.pickle')

        self.assertCountEqual(data, retrieved_data)

        self._test_cleanup_jobs_dir()

    #@unittest.skip('skip')
    def test_perform_job(self):
        job_id = 106
        self.hdfs_client.job_create_dirs(job_id=job_id)

        data = [("mike1",), ("george",), ("1g2eommrg3",)]
        map_func = lambda x: map(lambda letter: (letter, 1), x)  # [('m', 1), ('i', 1), ...]
        reduce_func = lambda x, y: x+y

        # 1. Save to HDFS the MapReduce `data`, `map_func`, `reduce_func`

        self.hdfs_client.save_data(f'jobs/job_{job_id}/data.pickle', data)
        self.hdfs_client.save_func(f'jobs/job_{job_id}/map_func.pickle', map_func)
        self.hdfs_client.save_func(f'jobs/job_{job_id}/reduce_func.pickle', reduce_func)

        # 2. (we skip the data split part from `master`) The `worker` comes and gets the (split) data
        #    from HDFS along with the `map_func`
        task_id = 1
        worker_map_data = self.hdfs_client.get_data(f'jobs/job_{job_id}/data.pickle')
        worker_map_func = self.hdfs_client.get_func(f'jobs/job_{job_id}/map_func.pickle')
        worker_map_results = list(chain.from_iterable(map(lambda x: map_func(*x), worker_map_data)))

        # 3. `worker` saves the map_results to `jobs/job_<job_id>/map_tasks/map_results_<task_id>.pickle
        self.hdfs_client.save_data(f'jobs/job_{job_id}/map_results/{task_id}.pickle', worker_map_results)

        # 4. `worker` retrieves the mapped data and shuffles
        map_results = self.hdfs_client.get_data(f'jobs/job_{job_id}/map_results/{task_id}.pickle')

        def shuffle_generator(input_data):
            input_data.sort(key=itemgetter(0))
            grouped_data = groupby(input_data, key=itemgetter(0))
            for key, group in grouped_data:
                values = [item[1] for item in group]
                yield key, values

        for i, key_value_tuple in enumerate(shuffle_generator(map_results)):
            self.hdfs_client.save_data(f'jobs/job_{job_id}/shuffle_results/{i}.pickle', key_value_tuple)

        # 5. `worker` comes in and reduces the results of every file in 'shuffle_results'
        listing = self.hdfs_client.hdfs.list(f'jobs/job_{job_id}/shuffle_results/')

        shuffle_data = []
        for shuffle_file in listing:
            shuffle_data.append(self.hdfs_client.get_data(f'jobs/job_{job_id}/shuffle_results/{shuffle_file}'))

        # reduce
        reduce_results = [
            (key, reduce(reduce_func, values))
            for key, values in shuffle_data
        ]

        # 5. Save to HDFS the reduce results (notice concatenated all the shuffle results in a single
        # list, this is not necessary and in the implementaton this is not the case

        self.hdfs_client.save_data(f'jobs/job_{job_id}/reduce_results/1.pickle', reduce_results)

        mapreduce_output = self.hdfs_client.get_data(f'jobs/job_{job_id}/reduce_results/1.pickle')

        actual_results = [('1', 2), ('2', 1), ('3', 1), ('e', 4),
                          ('g', 4), ('i', 1), ('k', 1), ('m', 3), ('o', 2), ('r', 2)]
        self.assertCountEqual(mapreduce_output, actual_results)

        self._test_cleanup_jobs_dir()

    def _test_job_create_dirs(self, job_id):
        # Assert that the directories were created in HDFS
        self.assertTrue(self.hdfs_client.hdfs.status(f"jobs/job_{job_id}/map_tasks"))
        self.assertTrue(self.hdfs_client.hdfs.status(f"jobs/job_{job_id}/reduce_tasks"))
        self.assertTrue(self.hdfs_client.hdfs.status(f"jobs/job_{job_id}/map_results"))
        self.assertTrue(self.hdfs_client.hdfs.status(f"jobs/job_{job_id}/shuffle_results"))
        self.assertTrue(self.hdfs_client.hdfs.status(f"jobs/job_{job_id}/reduce_results"))

    def _test_cleanup_jobs_dir(self):
        listing = self.hdfs_client.hdfs.list('jobs/')
        for job_dir in listing:
            self.assertTrue(self.hdfs_client.hdfs.delete(f'jobs/{job_dir}', recursive=True))
        # Check if 'jobs/' directory is empty
        listing = self.hdfs_client.hdfs.list('jobs/')
        self.assertEqual(len(listing), 0, "jobs/ directory should be empty")
    """
    @classmethod
    def tearDownClass(cls) -> None:
        # Run `docker-compose down`
        subprocess.run(
            ['docker-compose', 'down'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    """


if __name__ == '__main__':
    unittest.main()
