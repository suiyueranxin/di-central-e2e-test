from rich import print
from di_embedded_api_test.components.rms.task import TaskEntity
from utils.case_base import BaseTest
from time import sleep


class ConnectionTest(BaseTest):

    def test_create_connections(self):
        conn_list = self._descriptor.get("connections")
        for conn in conn_list:
            conn_name = conn.get("name")
            self.safe_create_connection(conn_name)

