import unittest

from src.cluster.local_cluster import LocalCluster


class LocalClusterTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.clu = LocalCluster(n_workers=0, n_masters=0, initialize=True, verbose=True)

    def test_print(self):
        pass


    @classmethod
    def tearDownClass(cls) -> None:
        cls.clu.shutdown_cluster(verbose=True)
