from passlib.context import CryptContext
import sqlite3
import os


class AuthenticationClient:
    """
    This class handles user authentication tasks such as creating users, deleting users, and verifying user credentials.
    """

    def __init__(self, db_file='auth.db'):
        """
        Initialize the AuthenticationClient.

        :param db_file: the path of the SQLite database file
        """
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

        # Get the directory of this module.
        module_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_file = os.path.join(module_dir, db_file)

        with sqlite3.connect(self.db_file) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS users
                             (username TEXT PRIMARY KEY,
                              password TEXT NOT NULL,
                              role TEXT NOT NULL);''')

        # Add an admin user by default if it doesn't exist
        self.create_user('admin', 'admin', 'admin', verbose=False)

    def create_user(self, username, password, role, verbose=True):
        """
        Create a new user.

        :param username: the username
        :param password: the password
        :param role: the role of the user
        :param verbose: whether to print messages
        :return: True if the user was created, False otherwise
        """
        hashed = self.pwd_context.hash(password)
        try:
            with sqlite3.connect(self.db_file) as conn:
                conn.execute('INSERT INTO users VALUES (?, ?, ?);', (username, hashed, role))
            return True
        except sqlite3.IntegrityError:  # Username already exists
            if verbose:
                print(f"Username {username} already exists.")
            return False

    def delete_user(self, username):
        """
        Delete a user.

        :param username: the username of the user to delete
        """
        if username == 'admin':  # Do not delete the default admin user
            print("Cannot delete the default admin user.")
            return False

        with sqlite3.connect(self.db_file) as conn:
            conn.execute('DELETE FROM users WHERE username = ?;', (username,))

    def authenticate(self, username, password):
        """
        Verify the user's credentials.

        :param username: the username
        :param password: the password
        :return: True if the credentials are valid, False otherwise
        """
        with sqlite3.connect(self.db_file) as conn:
            cur = conn.cursor()
            cur.execute('SELECT password FROM users WHERE username = ?;', (username,))
            row = cur.fetchone()
        if row is None:
            return False  # No such user
        hashed = row[0]
        return self.pwd_context.verify(password, hashed)

    def is_admin(self, username, password):
        """
        Verify if the user is an admin.

        :param username: the username
        :param password: the password
        :return: True if the user is an admin, False otherwise
        """
        with sqlite3.connect(self.db_file) as conn:
            cur = conn.cursor()
            cur.execute('SELECT password, role FROM users WHERE username = ?;', (username,))
            row = cur.fetchone()

        if row is None:
            return False  # No such user

        stored_hashed_password = row[0]
        role = row[1]

        if self.pwd_context.verify(password, stored_hashed_password) and role == 'admin':
            return True

        return False
