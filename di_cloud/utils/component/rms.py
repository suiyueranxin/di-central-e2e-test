import json
from time import sleep

from utils.component.component_base import Component

class RMS(Component):
    
    def get_replication_flow_status(self, rf_name):
        # path = f"/app/rms/api/dt/v1/replicationflowMonitors?name={rf_name}"
        # response = self._cluster.api_get(path)
        # if response.status_code == 404:
        #     return None
        # self.throw_exception_when_api_query_fail(response, 200)
        # return response.json()[0]["status"]
        monitor = self._cluster.monitoring.replications.get_monitor(rf_name)
        if monitor is not None:
            return monitor.status
        else:
            return None
            
    
    def deploy_replication_flow(self, rf_name):      
        # rf_path = f"/repository/v2/files/user/files/rms/{rf_name}.replication?op=read"
        # response1 = self._cluster.api_get(rf_path)
        # self.throw_exception_when_api_query_fail(response1, 200)
        # payload = response1.content.decode('utf-8')
        # path = "/app/rms/api/dt/v1/replicationflows"
        # response = self._cluster.apipost(path, payload)
        # self.throw_exception_when_api_query_fail(response, 202)
        replication = self._cluster.modeler.replications.open_replication(rf_name)
        replication.deploy()

    def wait_util_deploy_complete(self, rf_name):
        path = "/app/rms/api/dt/v1/replicationflows"
        iterNum = 0
        while True:
            sleep(60)
            iterNum += 1
            response = self._cluster.api_get(path, expected_status=200)
            # self.throw_exception_when_api_query_fail(response, 200)
            rf_item = self.get_item_in_list_by_property(response.json(), "name", rf_name)
            if (rf_item):
                rf_state = rf_item["state"]
                if (rf_state == "ACTIVATED"): 
                    break
            if (iterNum == 5):
                break
        if (iterNum == 5):
            raise Exception(f"The deployment for {rf_name} is failed.")

    def undeploy_replication_flow(self, rf_name):
        if self.get_replication_flow_status(rf_name) is not None: 
            path = f"/app/rms/api/dt/v1/replicationflows/{rf_name}"
            response = self._cluster.api_delete(path, expected_status=202)
            # self.throw_exception_when_api_query_fail(response, 202)
            sleep(180)
    
    def run_replication_flow(self, rf_name):
        path = f"/app/rms/api/dt/v1/replicationflows/{rf_name}?requestType=RUN_OR_RESUME_ALL_INACTIVE_TASKS"
        response = self._cluster.api_put(path, data=None, expected_status=202)
        # self.throw_exception_when_api_query_fail(response, 202)

    def suspend_replication_flow(self, rf_name):
        path = f"/app/rms/api/dt/v1/replicationflows/{rf_name}?requestType=SUSPEND_ALL_ACTIVE_TASKS"
        response = self._cluster.api_put(path, data=None, expected_status=202)
        # self.throw_exception_when_api_query_fail(response, 202)


        
    

        
    