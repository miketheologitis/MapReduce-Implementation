from authentication_client import AuthenticationClient


class Auth:
    """
    This class provides a simple user interface for the AuthenticationClient.
    """
    def __init__(self, username, password, role='user', db_file='auth.db'):
        """
        Initialize the Auth.

        :param username: the username
        :param password: the password
        :param role: the role of the user
        """
        self.auth_client = AuthenticationClient(db_file)

        self.username = username
        self.password = password
        self.role = role

    def is_authenticated(self):
        """
        Verify if the user is authenticated.

        :return: True if the user is authenticated, False otherwise
        """
        return self.auth_client.authenticate(self.username, self.password)

    def create_user(self, username, password, role='user'):
        """
        Create a new user if the current user is an admin.
        :param username: the username
        :param password: the password
        :param role: the role of the user
        """
        if self.auth_client.is_admin(self.username, self.password):
            self.auth_client.create_user(username, password, role)
        else:
            print("You are not admin. You cannot create users.")

    def delete_user(self, username):
        """
        Delete a user if the current user is an admin.

        :param username: the username of the user to delete
        """
        if self.auth_client.is_admin(self.username, self.password):
            self.auth_client.delete_user(username)
        else:
            print("You are not admin. You cannot delete users.")