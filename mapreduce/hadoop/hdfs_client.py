import pickle
import dill
import time
from hdfs import InsecureClient

"""
user='mapreduce'
/
├── jobs/ (from `user`)
│    ├── job_<job_id>/  (from `user`)
│    │      ├── data.pickle  (from `user`), Will be pulled from `master` for the job
│    │      ├── map_func.pickle (from `user`), Will be pulled from `master` for the job
│    │      ├── reduce_func.pickle (from `user`), Will be pulled from `master` for the job
│    │      ├── map_tasks/  (from `user`)
│    │      │       ├── <task_id>.pickle  (from `master`)
│    │      │       ├── ...
│    │      ├── map_results/  (from `user`)
│    │      │       ├── <task_id>.pickle  (from `worker`)
│    │      │       ├── ...
│    │      ├── shuffle_results/  (from `user`)
│    │      │       ├── 0.pickle  (from `worker`)   (key, values) tuple
│    │      │       ├── 1.pickle  (from `worker`)
│    │      │       ├── ...
│    │      ├── reduce_results/ (from `user`)
│    │      │       ├── 0.pickle  (from `worker`)   | OR  1_2.pickle if the `worker` gets two shuffle
│    │      │       ├── 1.pickle  (from `worker`)   |                input files
│    │      │       ├── ...
│    ├── ...

map_results -> <task_id>.pickle [(k, v), ...]
shuffle_results -> 0.pickle (k,values)
reduce_results -> 0.pickle [(k,v), ...]
"""


class HdfsClient:

    def __init__(self, host):
        self.hdfs = InsecureClient(f'http://{host}', user='mapreduce')
        self.check_start_with_retries()

    def check_start_with_retries(self, max_retries=15, retry_delay=10):
        for i in range(max_retries):
            try:
                self.hdfs.list('')
                self.initialize_jobs_dir()
            except Exception as e:
                if i < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:  # raise exception if this was the last retry
                    raise Exception("Could not connect to HDFS after multiple attempts") from e

    def initialize_jobs_dir(self):
        self.hdfs.makedirs('jobs/')

    def job_create(self, job_id, data, map_func, reduce_func):
        """
        Create directories for a job in HDFS.

        :param job_id: The unique id of the job.
        :param data: The initial data
        :param map_func: The map func
        :param reduce_func: The reduce func
        """

        self.hdfs.makedirs(f'jobs/job_{job_id}/map_tasks/')
        self.hdfs.makedirs(f'jobs/job_{job_id}/map_results/')
        self.hdfs.makedirs(f'jobs/job_{job_id}/shuffle_results/')
        self.hdfs.makedirs(f'jobs/job_{job_id}/reduce_results/')
        self.save_data(f'jobs/job_{job_id}/data.pickle', data)
        self.save_func(f'jobs/job_{job_id}/map_func.pickle', map_func)
        self.save_func(f'jobs/job_{job_id}/reduce_func.pickle', reduce_func)

    def save_data(self, hdfs_path, data):
        """
        Serialize and save data to HDFS. Overwrite if already exists (should never happen).

        :param hdfs_path: The HDFS path to save the data.
        :param data: The data to be saved.
        """
        pickled_data = pickle.dumps(data)
        self.hdfs.write(hdfs_path, data=pickled_data, overwrite=True)

    def save_func(self, hdfs_path, func):
        """
        Serialize using dill and save a function to HDFS.
        Dill is used because it can serialize almost any Python object,
        including functions and lambdas.

        :param hdfs_path: The HDFS path to save the function.
        :param func: The function to be saved.
        """
        serialized_func = dill.dumps(func)
        self.hdfs.write(hdfs_path, data=serialized_func)

    def get_data(self, hdfs_path):
        """
        Read, deserialize and return data from HDFS.

        :param hdfs_path: The HDFS path to read the data from.
        :returns: The deserialized data.
        """
        with self.hdfs.read(hdfs_path) as reader:
            pickled_data = reader.read()
        data = pickle.loads(pickled_data)
        return data

    def get_func(self, hdfs_path):
        """
        Read, deserialize using dill and return a function from HDFS.

        :param hdfs_path: The HDFS path to read the function from.
        :returns: The deserialized function.
        """
        with self.hdfs.read(hdfs_path) as reader:
            serialized_func = reader.read()
        func = dill.loads(serialized_func)
        return func

    def cleanup(self):
        """ Delete `jobs` directory completely with everything inside """
        self.hdfs.delete('jobs', recursive=True)


