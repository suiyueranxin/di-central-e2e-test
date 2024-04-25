import json
from urllib.parse import quote

from utils.component.component_base import Component

class SystemManagement(Component):
    
    def change_application_property(self, payload):
        path = f"/scheduler/v2/parameter?tenant={self._tenant}"
        rsp = self.get_admin_session().api_post(path, data=json.dumps(payload), expected_status=200)
        # self.throw_exception_when_api_query_fail(rsp, 200)
    
    def restart_application(self, app_id, is_cluster_app=False):
        # get app_runtime_id according to app_id
        path = f"/scheduler/v2/user/instance?include-shared=true&getStoppableInfo=true"
        session = self.get_system_session() if is_cluster_app else self.get_admin_session()
        rsp = session.api_get(path, expected_status=200)
        # self.throw_exception_when_api_query_fail(rsp, 200)
        app_info = self.get_item_in_list_by_property(rsp.json(), "appTemplateId", app_id)
        
        # restart
        if app_info is not None:
            app_runtime_id = app_info.get("id")
            path = f"/scheduler/v2/instance/id/{app_runtime_id}/restart"
            rsp = session.api_post(path, data=None, expected_status=200)
            # self.throw_exception_when_api_query_fail(rsp, 200)
            # TODO: add wait_until the application is 'ready'
    
    def add_policy(self, name, policy_list, description=""):
        path = "/policy/v2/policies"
        policy = {
            "id": name,
            "description": description,
            "enabled": True,
            "exposed": True,
            "resources": [],
            "policyReferences": [{"id": x} for x in policy_list]
        }
        rsp = self.get_admin_session().api_post(path, data=json.dumps(policy), expected_status=201)
        # self.throw_exception_when_api_query_fail(rsp, 201)
        
    def get_policy(self, name):
        path = "/policy/v2/policies"
        rsp = self.get_admin_session().api_get(path, expected_status=200)
        # self.throw_exception_when_api_query_fail(rsp, 200)
        return self.get_item_in_list_by_property(rsp.json(), "id", name)
    
    def is_policy_existed(self, name):
        path = f"/policy/v2/policies/{name}"
        rsp = self.get_admin_session().api_get(path)
        return rsp.status_code != 404
    
    def delete_policy(self, name):
        if self.is_policy_existed(name):
            path = "/policy/v2/assignments"
            rsp = self.get_admin_session().api_get(path, expected_status=200)
            # self.throw_exception_when_api_query_fail(rsp, 200)
            assignments = self.get_all_items_in_list_by_property(rsp.json()["assignments"], "id", name)
            if assignments is not None:
                for assignment in assignments:
                    self.unassign_policy(assignment["user"], name)
            
            path = f"/policy/v2/policies/{name}"
            rsp = self.get_admin_session().api_delete(path, expected_status=204)
            # self.throw_exception_when_api_query_fail(rsp, 204)
    
    def add_user(self, name, password):
        path = "/auth/v2/user"
        user = {
            "username": name,
            "password": password,
            "role": "member"
        }
        rsp = self.get_admin_session().api_post(path, data=json.dumps(user), expected_status=201)
        # self.throw_exception_when_api_query_fail(rsp, 201)
        
    def get_user(self, name):
        path = "/api/idp/v1/user/"
        rsp = self.get_admin_session().api_get(path, expected_status=200)
        # self.throw_exception_when_api_query_fail(rsp, 200)
        return self.get_item_in_list_by_property(rsp.json(), "uuid", name)
    
    def is_user_existed(self, name):
        path = f"/api/idp/v1/user/{name}"
        rsp = self.get_admin_session().api_get(path)
        return rsp.status_code != 404
    
    def delete_user(self, name):
        if self.is_user_existed(name):
            path = f"/auth/v2/user/{name}"
            rsp = self.get_admin_session().api_delete(path, expected_status=200)
            # self.throw_exception_when_api_query_fail(rsp, 200)
        
    def assign_policy(self, user_name, policy_name):
        path = f"/policy/v2/users/{user_name}/policies/{policy_name}?tenant={self._tenant}"
        rsp = self.get_admin_session().api_post(path, data=None, expected_status=201)
        # self.throw_exception_when_api_query_fail(rsp, 201)
        
    def unassign_policy(self, user_name, policy_name):
        if self.is_policy_existed(policy_name):
            path = f"/policy/v2/users/{user_name}/policies/{policy_name}?tenant={self._tenant}"
            rsp = self.get_admin_session().api_delete(path, expected_status=204)
            # self.throw_exception_when_api_query_fail(rsp, 204)
        
    def get_user_assigned_policies(self, user_name):
        path = "/policy/v2/assignments"
        rsp = self.get_admin_session().api_get(path)
        self.throw_exception_when_api_query_fail(rsp, 200)
        assignments = self.get_all_items_in_list_by_property(rsp.json()["assignments"], "user", user_name)
        if assignments is None:
            return []
        return [x["id"] for x in assignments]
    
    def get_folder(self, folder_path, space="user"):
        folder_path_encode = quote(folder_path, safe='')
        path = f"/repository/v2/files/{space}{folder_path_encode}?op=list"
        rsp = self._cluster.api_get(path)
        if rsp.status_code == 200:
            return rsp.json()
        return None
    
    def get_file_by_filter(self, folder_path, regex, space="user"):
        folder_path_encode = quote(folder_path, safe='')
        path = f"/repository/v2/files/{space}{folder_path_encode}?op=find&regex={regex}"
        rsp = self._cluster.api_get(path)
        if rsp.status_code == 200:
            return rsp.json()
        return None
    
    def is_folder_existed(self, folder_path, space="user"):
        return self.get_folder(folder_path, space) is not None
    
    def add_folder(self, folder_path, space="user"):
        folder_path_encode = quote(folder_path, safe='')
        path = f"/repository/v2/files/{space}{folder_path_encode}?op=mkdirall"
        rsp = self._cluster.api_put(path, data=None, expected_status=200)
        # self.throw_exception_when_api_query_fail(rsp, 200)
            
    def remove_folder(self, folder_path, space="user"):
        if self.is_folder_existed(folder_path, space):
            folder_path_encode = quote(folder_path, safe='')
            path = f"/repository/v2/files/{space}{folder_path_encode}?op=removeall"
            rsp = self._cluster.api_delete(path, expected_status=200)
            # self.throw_exception_when_api_query_fail(rsp, 200)
            
    def import_zip_file(self, local_zip_file, target_folder_full_path, space="user"):
        target_folder_full_path_encode = quote(target_folder_full_path, safe='')
        path = f"/repository/v2/files/{space}{target_folder_full_path_encode}?op=import"
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/x-compressed',
            'X-Requested-With': 'Fetch'
        }
        url = self._cluster._urls.base + path
        with open(local_zip_file, mode="rb") as f:
            content = f.read()
            rsp = self._cluster.session.post(url, data=content, headers=headers)
            self.throw_exception_when_api_query_fail(rsp, 200)
        
    def import_file(self, local_file, target_file_full_path, space="user"):
        target_file_full_path_encode = quote(target_file_full_path, safe='')
        path = f"/repository/v2/files/{space}{target_file_full_path_encode}?op=write"
        headers = {
            'accept': 'application/json',
            'Content-Type': 'text/plain;charset=UTF-8',
            'X-Requested-With': 'Fetch'
        }
        url = self._cluster._urls.base + path
        with open(local_file) as f:
            content = f.read()
            rsp = self._cluster.session.post(url, data=content, headers=headers)
            self.throw_exception_when_api_query_fail(rsp, 200)
    
    def get_file(self, file_full_path, space="user"):
        index = file_full_path.rfind("/")
        folder_path, file_name = file_full_path[:index], file_full_path[(index+1):]
        folder = self.get_folder(folder_path, space)
        return self.get_item_in_list_by_property(folder, "name", file_name)
    
    def is_file_existed(self, file_full_path, space="user"):
        return self.get_file(file_full_path, space) is not None
    
    def import_solution(self, local_zip_file, space="user"):
        path = f"/repository/v2/files/{space}?op=import-solution&onConflict=overwrite"
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/gzip',
            'X-Requested-With': 'Fetch'
        }
        url = self._cluster._urls.base + path
        with open(local_zip_file, mode="rb") as f:
            content = f.read()
            rsp = self._cluster.session.post(url, data=content, headers=headers)
            self.throw_exception_when_api_query_fail(rsp, 200)
    
    def get_application_property(self, app_id):
        # vflow.internal.tenant
        path = f"/scheduler/v2/parameter?tenant={self._tenant}"
        parameter_obj = self.get_admin_session().api_get(path).json()
        if hasattr(parameter_obj, "__iter__"): # if api response is not iterable, such as null
            app_list = list(filter(lambda x: x.get("id") == app_id, parameter_obj))
            if len(app_list) > 0:
                return app_list[0].get("customValue")
        return None
    
    
            