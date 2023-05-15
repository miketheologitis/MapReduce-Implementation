import time
import unittest
import threading

from src.cluster.local_cluster import LocalCluster


class LocalClusterTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.clu = LocalCluster(n_workers=0, n_masters=0, initialize=True, verbose=False)

    def test_wait_for_job_completion(self):
        self.clu.get_zk_client().register_job(job_id=1)

        # Create a new thread that will call wait_for_job_completion
        def wait_for_job():
            self.clu.local_monitoring.wait_for_job_completion(job_id=1)

        wait_thread = threading.Thread(target=wait_for_job)

        def in_progress_job():
            time.sleep(3)
            zk_client = self.clu.get_zk_client()
            zk_client.update_job(1, state='in-progress')

        in_progress_thread = threading.Thread(target=in_progress_job)

        def complete_job():
            time.sleep(3)
            zk_client = self.clu.get_zk_client()
            zk_client.update_job(1, state='completed')

        complete_thread = threading.Thread(target=complete_job)

        # Start the threads
        wait_thread.start()
        in_progress_thread.start()

        # Join the threads to wait for them to finish, but only for a maximum of 10 seconds each
        wait_thread.join(timeout=10)
        in_progress_thread.join(timeout=10)

        # At this point, wait_for_job_completion should have returned, since we simulated
        # job completion. If it hasn't, the test will fail. More specifically,
        # if wait_thread is still alive at this point, it means that
        # wait_for_job_completion did not return even though the job was marked as completed
        self.assertTrue(wait_thread.is_alive())


        # Start the thread
        complete_thread.start()

        # Join the threads to wait for them to finish, but only for a maximum of 10 seconds each
        wait_thread.join(timeout=10)
        complete_thread.join(timeout=10)

        # At this point, wait_for_job_completion should have returned, since we simulated
        # job completion. If it hasn't, the test will fail. More specifically,
        # if wait_thread is still alive at this point, it means that
        # wait_for_job_completion did not return even though the job was marked as completed
        self.assertFalse(wait_thread.is_alive())

        self.assertFalse(complete_thread.is_alive())
        self.assertFalse(in_progress_thread.is_alive())

    def test_scale(self):
        self.clu.scale(n_workers=2)
        self._check_workers_master(n_workers=2, n_masters=0)

        self.clu.scale(n_masters=2)
        self._check_workers_master(n_workers=2, n_masters=2)

        # No change
        self.clu.scale(n_workers=2, n_masters=2)
        self._check_workers_master(n_workers=2, n_masters=2)

        self.clu.scale(n_workers=4, n_masters=2)
        self._check_workers_master(n_workers=4, n_masters=2)

        self.clu.scale(n_masters=0)
        self._check_workers_master(n_workers=4, n_masters=0)

        self.clu.scale(n_workers=0)
        self._check_workers_master(n_workers=0, n_masters=0)

    def _check_workers_master(self, n_workers, n_masters, max_retries=15, retry_delay=5):

        for _ in range(max_retries):
            _, n_masters_registered = self.clu.local_monitoring.get_registered_masters()
            _, n_workers_registered = self.clu.local_monitoring.get_registered_workers()
            if n_workers_registered == n_workers and n_masters_registered == n_masters:
                return True
            time.sleep(retry_delay)
        return False


    @classmethod
    def tearDownClass(cls) -> None:
        zk_client1 = cls.clu.local_monitoring.get_zk_client()
        zk_client2 = cls.clu.get_zk_client()
        zk_client1.zk.stop()
        zk_client1.zk.close()
        zk_client2.zk.stop()
        zk_client2.zk.close()
        cls.clu.shutdown_cluster(verbose=False)

