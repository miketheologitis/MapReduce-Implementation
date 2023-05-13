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

    def initialize_job_dirs(self, job_id):
        """ Called by master """
        self.hdfs.makedirs(f'jobs/job_{job_id}/map_tasks/')
        self.hdfs.makedirs(f'jobs/job_{job_id}/reduce_tasks/')

        self.hdfs.makedirs(f'jobs/job_{job_id}/map_results/')
        self.hdfs.makedirs(f'jobs/job_{job_id}/shuffle_results/')
        self.hdfs.makedirs(f'jobs/job_{job_id}/reduce_results/')



