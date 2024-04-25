from utils.component.component_base import Component

import json


class ConnectionManagement(Component):

    # TODO Remove as no longer used
    def get_connection_status(self, conn_id: str) -> str:
        return self._cluster.connectionmanagement.get_connection_status(conn_id)

    def import_connections(self, file_path) -> None:
        path = f"/app/datahub-app-connection/connections"
        with open(file_path) as f:
            connections = json.load(f)
        for conn in connections:
            self.get_admin_session().api_post(path, data=json.dumps(conn), expected_status=201)

    # TODO Remove as no longer used
    def is_connection_existed(self, conn_name: str) -> bool:
        return self._cluster.connectionmanagement.connection_exists(conn_name)

    def delete_connection(self, conn_name: str) -> None:
        self._cluster.connectionmanagement.delete_connection(conn_name)
