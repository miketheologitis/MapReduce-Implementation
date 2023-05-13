from hdfs import InsecureClient

"""
/
├── <job_id>/
│   ├── map_tasks/
│   │   ├── map_func.pickle  (from `master`)
│   │   ├── map_data_<task_id>.pickle  (from `master`)
│   │   ├── ...
│   ├── map_results/
│   │   ├── map_results_<task_id>.pickle  (from `worker`)
│   │   ├── ...
│   ├── shuffle_results/
│   │   ├── shuffle_results_1.pickle  (from `worker`)
│   │   ├── shuffle_results_2.pickle  (from `worker`)
│   │   ├── ...
│   ├── reduce_tasks/
│   │   └── reduce_func.pickle  (from `master`)
│   ├── reduce_results/
│   │   ├── reduce_results_<task_id>.pickle  (from `worker`)
│   │   ├── ...
├── ...
"""


class HdfsClient:

    def __init__(self, host="namenode"):
        self.hdfs = InsecureClient(f'http://{host}:9870')

    def initialize_job_dirs(self, job_id):
        """ Called by master """
        self.hdfs.makedirs(f'{job_id}/map_tasks/')
        self.hdfs.makedirs(f'{job_id}/reduce_tasks/')

        self.hdfs.makedirs(f'{job_id}/map_results/')
        self.hdfs.makedirs(f'{job_id}/shuffle_results/')
        self.hdfs.makedirs(f'{job_id}/reduce_results/')



