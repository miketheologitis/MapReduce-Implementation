import unittest
import os
import sys
from mapreduce.authentication.auth import Auth
from mapreduce.authentication.authentication_client import AuthenticationClient


class TestAuthentication(unittest.TestCase):
    def setUp(self):
        # Redirect stdout to devnull to suppress output during tests
        self.original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

        self.db_file = 'test_auth.db'
        self.auth_client = AuthenticationClient(db_file=self.db_file)
        self.auth = Auth('test_user', 'test_pass', 'user', db_file=self.db_file)

    def test_create_user(self):
        result = self.auth_client.create_user('test_user', 'test_pass', 'user')
        self.assertTrue(result)

    def test_create_existing_user(self):
        self.auth_client.create_user('test_user', 'test_pass', 'user')
        result = self.auth_client.create_user('test_user', 'test_pass', 'user')
        self.assertFalse(result)

    def test_authenticate_user(self):
        self.auth_client.create_user('test_user', 'test_pass', 'user')
        result = self.auth_client.authenticate('test_user', 'test_pass')
        self.assertTrue(result)

    def test_authenticate_non_existing_user(self):
        result = self.auth_client.authenticate('non_existing_user', 'test_pass')
        self.assertFalse(result)

    def test_is_admin(self):
        self.auth_client.create_user('admin_user', 'admin_pass', 'admin')
        result = self.auth_client.is_admin('admin_user', 'admin_pass')
        self.assertTrue(result)

    def test_is_not_admin(self):
        self.auth_client.create_user('test_user', 'test_pass', 'user')
        result = self.auth_client.is_admin('test_user', 'test_pass')
        self.assertFalse(result)

    def test_auth_is_authenticated(self):
        self.auth_client.create_user('test_user', 'test_pass', 'user')
        self.auth.username = 'test_user'
        self.auth.password = 'test_pass'
        result = self.auth.is_authenticated()
        self.assertTrue(result)

    def test_auth_create_user(self):
        self.auth.username = 'admin'
        self.auth.password = 'admin'
        result = self.auth.create_user('new_user', 'new_pass', 'user')
        self.assertIsNone(result)

    def test_auth_delete_user(self):
        self.auth_client.create_user('test_user_2', 'test_pass', 'user')
        self.auth.username = 'admin'
        self.auth.password = 'admin'
        result = self.auth.delete_user('test_user_2')
        self.assertIsNone(result)

    def tearDown(self):
        # clean up the database file after the tests
        os.remove(self.db_file)

        # Restore stdout to its original state after each test
        sys.stdout.close()
        sys.stdout = self.original_stdout


if __name__ == '__main__':
    unittest.main()

