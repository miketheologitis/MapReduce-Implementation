import unittest
import requests
import subprocess
import yaml
import os
import pickle
import time
from src.workers.master import master
from src.hadoop.hdfs_client import HdfsClient


class TestMasterHdfs(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # Deploy the zookeeper containers and two worker containers
        subprocess.run(
            ['docker-compose', 'up', '-d', '--scale', 'worker=0', '--scale', 'master=1', '--no-recreate'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        # This was needed because even though the docker-compose ends, containers
        # need some more time to be reachable
        time.sleep(15)

        cls.hdfs_client = HdfsClient("localhost:9870")

    def test_test(self):
        print("hi")

    @classmethod
    def tearDownClass(cls) -> None:
        # Run `docker-compose down`
        subprocess.run(
            ['docker-compose', 'down'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )




if __name__ == '__main__':
    unittest.main()
