
from . import data_client

class ExampleClient(data_client.DataClient):

    SOURCE_TYPE = "Example"

    def get_connection(self):
        """ Get a new connection to the client """
        return ""

    def is_connected(self, conn):
        """ Checks if a connection is still active """
        return True