import pickle
import dill
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
│    │      │       ├── map_data_<task_id>.pickle  (from `master`)
│    │      │       ├── ...
│    │      ├── map_results/  (from `user`)
│    │      │       ├── map_results_<task_id>.pickle  (from `worker`)
│    │      │       ├── ...
│    │      ├── shuffle_results/  (from `user`)
│    │      │       ├── shuffle_results_1.pickle  (from `worker`)
│    │      │       ├── shuffle_results_2.pickle  (from `worker`)
│    │      │       ├── ...
│    │      ├── reduce_results/ (from `user`)
│    │      │       ├── reduce_results_1.pickle  (from `worker`)
│    │      │       ├── reduce_results_2.pickle  (from `worker`)
│    │      │       ├── ...
│    ├── ...
"""


class HdfsClient:

    def __init__(self, host):
        self.hdfs = InsecureClient(f'http://{host}', user='mapreduce')
        self.hdfs.makedirs('jobs/')

    def job_create_dirs(self, job_id):
        """
        Create directories for a job in HDFS.

        :param job_id: The unique id of the job.
        """
        self.hdfs.makedirs(f'jobs/job_{job_id}/map_tasks/')
        self.hdfs.makedirs(f'jobs/job_{job_id}/reduce_tasks/')
        self.hdfs.makedirs(f'jobs/job_{job_id}/map_results/')
        self.hdfs.makedirs(f'jobs/job_{job_id}/shuffle_results/')
        self.hdfs.makedirs(f'jobs/job_{job_id}/reduce_results/')

    def save_data(self, hdfs_path, data):
        """
        Serialize and save data to HDFS.

        :param hdfs_path: The HDFS path to save the data.
        :param data: The data to be saved.
        """
        pickled_data = pickle.dumps(data)
        self.hdfs.write(hdfs_path, data=pickled_data)

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


