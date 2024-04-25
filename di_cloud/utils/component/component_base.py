import os
import inspect
import json

from di_qa_e2e.cluster import Cluster
from di_qa_e2e.connections.connection_data import ConnectionData


class Component:

    def __init__(self, cluster: Cluster) -> None:
        self._cluster = cluster
        self._system_pwd = cluster._connectiondata.system_password
        self._tenant = cluster._connectiondata.tenant
        self._admin = cluster._connectiondata.admin_user
        self._admin_pwd = cluster._connectiondata.admin_password
        self._system_session = None
        self._admin_session = None

    def get_admin_session(self):
        if self._admin_session is None:
            self._init_admin_session()
        return self._admin_session

    def get_system_session(self):
        if self._system_session is None:
            self._init_system_session()
        return self._system_session

    def throw_exception_when_api_query_fail(self, rsp, status_code):
        func_name = inspect.stack()[1][3]
        if rsp.status_code != status_code:
            raise Exception(
                f"\nfunction:'{func_name}'\napi query:'{rsp.url}'\n{rsp.text}")

    def get_item_in_list_by_property(self, obj_list, property_name, property_value):
        if hasattr(obj_list, "__iter__"):  # if api response is not iterable, such as null
            the_obj_list = list(filter(lambda x: self._is_item_existed_in_obj(
                x, property_name, property_value), obj_list))
            if len(the_obj_list) > 0:
                return the_obj_list[0]
        return None

    def get_all_items_in_list_by_property(self, obj_list, property_name, property_value):
        if hasattr(obj_list, "__iter__"):  # if api response is not iterable, such as null
            the_obj_list = list(filter(lambda x: self._is_item_existed_in_obj(
                x, property_name, property_value), obj_list))
            if len(the_obj_list) > 0:
                return the_obj_list
        return None

    def _is_item_existed_in_obj(self, obj, property_name, property_value):
        property_list = property_name.split(".")
        item = obj
        for property in property_list:
            if item.get(property) is None:
                return False
            item = obj.get(property)
        return item == property_value

    def _init_system_session(self):
        system_info = {
            "baseurl": self._cluster._urls.base,
            "tenant": "system",
            "user": "system",
            "password": self._system_pwd
        }
        cluster_name = "CET_SYSTEM"
        os.environ.setdefault(cluster_name, json.dumps(system_info))
        cluster_info = ConnectionData.for_cluster(cluster_name)
        self._system_session = Cluster.connect_to(cluster_info)

    def _init_admin_session(self):
        admin_info = {
            "baseurl": self._cluster._urls.base,
            "tenant": self._tenant,
            "user": self._admin,
            "password": self._admin_pwd
        }
        cluster_name = "CET_ADMIN"
        os.environ.setdefault(cluster_name, json.dumps(admin_info))
        cluster_info = ConnectionData.for_cluster(cluster_name)
        self._admin_session = Cluster.connect_to(cluster_info)
