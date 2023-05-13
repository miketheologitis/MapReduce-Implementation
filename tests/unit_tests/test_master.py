import unittest
from itertools import chain
from unittest.mock import patch, call, create_autospec
from src.zookeeper.zookeeper_client import ZookeeperClient
from src.hadoop.hdfs_client import HdfsClient
from src.workers.master import app, master, Master

mock_zk_client = create_autospec(ZookeeperClient)
# Mock the update_master_state method on the ZookeeperClient
mock_zk_client.update_master_state.return_value = None

mock_hdfs_client = create_autospec(HdfsClient)
mock_hdfs_client.initialize_job_dirs.return_value = None


class TestMaster(unittest.TestCase):

    def test_split_data(self):
        lst1 = [range(1000)]
        for i in range(1, 1000):
            self._test_split_data(lst1, Master.split_data(lst1, i))

        lst2 = [(i, ) for i in range(1000)]
        for i in range(1, 1000):
            self._test_split_data(lst2, Master.split_data(lst2, i))

    def _test_split_data(self, data, split_data):
        self.assertCountEqual(data, list(chain(*split_data)))


if __name__ == '__main__':
    unittest.main()
