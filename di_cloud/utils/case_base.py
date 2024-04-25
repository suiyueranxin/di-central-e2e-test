import unittest
import sys
import os
import json
from retry import retry

from di_qa_e2e.cluster import Cluster
from di_qa_e2e.connections.connection_data import ConnectionData
from di_qa_e2e.monitoring import Monitoring
from di_qa_e2e.metadata_explorer import MetadataExplorer
from di_qa_e2e.connection_management import ConnectionManagement

from di_qa_e2e_validation.abap.CitAbapClient import CitAbapClient
from di_qa_e2e_validation.datalake.DatalakeClient import DatalakeClient
from di_qa_e2e_validation.hana.HanaClient import HanaClient

from utils.component.rms import RMS
from utils.component.system_management import SystemManagement
from content.conn import ConnConfig, ConnContent


class BaseCase(unittest.TestCase):
    
    # static variables
    __ENV_FILE_ROOT_PATH = os.path.join(os.path.dirname(__file__), '../connection_data')
    # class variables
    _descriptor = {} 
    __connections = {}
    __cluster = None
    __sm: SystemManagement = None
    __me: MetadataExplorer = None
    __cm: ConnectionManagement = None
    __monitoring: Monitoring = None
    __rms: RMS = None
    
    ### ************************Hook Begin*************************** 
    # call once before each test suite    
    @classmethod
    def setUpClass(cls):
        # load descriptor which defines cluster, connections
        cls._load_descriptor()
                        
    # call once after each test suite 
    @classmethod
    def tearDownClass(cls):
        # clean cluster
        cls.__cluster = None
    
    # call once before each test method  
    def setUp(self):
        # print("setUp")
        pass
    
    # call once after each test method
    def tearDown(self):
        # print("tearDown")
        # erase abap subscription?
        pass
    
    ### ************************Hook End***************************
           
    ### ************************Public Functions Begin***********************
    
    ## *************************Actions Begin****************************
    @classmethod 
    def get_cluster(cls) -> Cluster:
        if cls.__cluster is None:
            cls._init_cluster()
        return cls.__cluster
    
    @classmethod
    def get_connection_client(cls, conn_name):
        conn_info: ConnConfig = cls.get_connection(conn_name)
        return conn_info.get_conn_client()
     
    @classmethod
    def get_connection(cls, conn_name):
        if not cls.__connections:
            cls._init_connections()
        return cls.__connections.get(conn_name)
    
    # TODO Rename to get_graphs()
    @classmethod
    def get_graph(cls):
        return cls.get_cluster().modeler.graphs
    
    @classmethod
    def get_system_management(cls):
        if cls.__sm is None:
            cls.__sm = SystemManagement(cls.get_cluster())
        return cls.__sm
    
    @classmethod
    def get_metadata_explorer(cls):
        if cls.__me is None:
            cls.__me = MetadataExplorer(cls.get_cluster())
        return cls.__me
    
    @classmethod
    def get_connection_management(cls):
        if cls.__cm is None:
            cls.__cm = ConnectionManagement(cls.get_cluster())
        return cls.__cm
    
    @classmethod
    def get_monitoring(cls):
        if cls.__monitoring is None:
            cls.__monitoring = Monitoring(cls.get_cluster())
        return cls.__monitoring
    
    @classmethod
    def get_rms(cls):
        if cls.__rms is None:
            cls.__rms = RMS(cls.get_cluster())
        return cls.__rms
    
    def get_data_file_path(cls, relative_path):
        return os.path.join(os.path.dirname(sys.modules[cls.__module__].__file__), relative_path)       
    
    #TODO: can be done by Abap client instead of run erase graph?
    #NOTE: Regarding the todo above: it depends on how this would be done in real life
    #      If the user also runs the erase graph the we should also do it via graph.
    @retry(tries=3, delay=30)
    def abap_erase_subscription_id_by_name(self, abap_connection_id, subscription_name) -> bool:
        if self._abap_subscription_name_is_new(abap_connection_id, subscription_name):
            return True
        can_be_erased, sub_id = self._abap_subscription_name_can_be_erased(abap_connection_id, subscription_name)
        if not can_be_erased:
            raise Exception(f"The subscription name {subscription_name} cannot be erased now, retry!")    
        graph = self.get_graph()
        graph_execution = graph.run_graph('com.sap.abap-int-tools.Erase_SUBSCRIBER_ID', 
                        f"Erase SN {subscription_name}",
                        {"subscription_name":sub_id,"abap_sys_id":abap_connection_id})
        graph_execution.wait_until_completed()
        return self._abap_subscription_name_is_new(abap_connection_id, subscription_name)
    
    def get_graph_substitutions(self, graph_name):
        url = f"/app/pipeline-modeler/service/v1/introspect/graph/{graph_name}/substitutions"
        sub_objs = self.get_cluster().api_get(url, 200).json()
        return sub_objs
    
    def get_conn_content_total_count(self, conn_name, content_id):
        conn_content = ConnContent(self.get_connection(conn_name), content_id)
        return conn_content.get_count_of_content()
    
    def get_adlv2_content_total_count(self, conn_name, path, substring, header_index, is_row_count_same):
        conn_content = ConnContent(self.get_connection(conn_name), path)
        return conn_content.get_count_of_content_from_adlv2(substring, header_index, is_row_count_same)
    
    # Get a list of the subscription objects from a ABAP system.
    #TODO: can be deleted or as private
    def get_subscription_objects(self, abap_connection_id: str) -> list[dict]:
        url = f"/app/axino-service/ape/v1/{abap_connection_id}/operator/com.sap.abap.subscr.eraser/subscription"
        subscription_objects = self.get_cluster().api_get(url, 200).json()
        return subscription_objects
    
    #TODO: can be deleted
    def get_all_subscription_objects(self) -> list[dict]:
        S4_subscription_objects = self.get_subscription_objects("CIT_S4")
        DMIS_subscription_objects = self.get_subscription_objects("CIT_DMIS")
        self.subscription_objects = S4_subscription_objects + DMIS_subscription_objects
        
    """
    Sometimes the subscription name is not erased due to the fact that there is graph with delta/replication loading, 
    which will result in the graph failure for the next execution, so we need to erase
    these subscription names before starting the test cases.
    """
    #TODO: deprecated by abap_erase_subscription_id_by_name
    def erase_subscription_name(self, abap_connection_id: str, subscription_name: str):
        id = ""
        subscription_objects = self.get_subscription_objects(abap_connection_id)
        if subscription_objects:
            for subscription_object in subscription_objects:
                if subscription_name == subscription_object["Subscription Name"]:
                    id = subscription_object["Subscription ID"]
                    break
            if id:
                graphs = self.get_cluster().modeler.graphs
                execution = graphs.run_graph('com.sap.abap-int-tools.Erase_SUBSCRIBER_ID', f"Erase SN {subscription_name}",{"subscription_name":id,"abap_sys_id":abap_connection_id})
                execution.wait_until_completed()
            else:
                print(f"Could not find id for the specified subscription name {subscription_name}")
    ## *************************Actions End***************************
    
    
    ## *************************Validation Begin***********************
    def validate_conn_content_total_count_from_src_to_target(self, src_conn_name, src_content_id, target_conn_name, target_content_id):     
        src_count = self.get_conn_content_total_count(src_conn_name, src_content_id)
        target_count = self.get_conn_content_total_count(target_conn_name, target_content_id)
        self.assertEqual(src_count, target_count, 
                         f"The record number should be the same. Total of source {src_conn_name}/{src_content_id} is {src_count} and total of target {target_conn_name}/{target_content_id} is {target_count}.")
    
    def validate_conn_content_contains(self, conn_name, content_id, key_content):
        content = ConnContent(self.get_connection(conn_name), content_id).get_content()
        self.assertIn(key_content, content, 
                      f"The content should contains key information. The content of source {conn_name}/{content_id} not contains {key_content}.")
        
    def validate_conn_content_total_count(self, conn_name, content_id, expected_count):
        actual_count = self.get_conn_content_total_count(conn_name, content_id)
        self.assertEqual(expected_count, actual_count, 
                      f"The record number of {conn_name}/{content_id} should be {expected_count}, but actual is {actual_count}.")
    
    def validate_conn_content_total_count_from_src_to_filestore(self, src_conn_name, src_content_id, adlv2_conn_name, path, substring='', header_index='infer', is_row_count_same=True):     
        src_count = self.get_conn_content_total_count(src_conn_name, src_content_id)
        file_count = self.get_adlv2_content_total_count(adlv2_conn_name, path, substring, header_index, is_row_count_same)
        self.assertEqual(src_count, file_count, 
                         f"The record number should be the same. Total of source {src_conn_name}/{src_content_id} is {src_count} and total of target {adlv2_conn_name}/{path} is {file_count}.")
    
    def validate_filestore_conn_content_total_count(self, expected_count, adlv2_conn_name, path, substring='', header_index = 'infer', is_row_count_same=True):
        actual_filestore_count = self.get_adlv2_content_total_count(adlv2_conn_name, path, substring, header_index, is_row_count_same)
        self.assertEqual(expected_count, actual_filestore_count, 
                      f"The record number of {adlv2_conn_name}/{path} should be {expected_count}, but actual is {actual_filestore_count}.")
        
    # **************************System Management************************ 
    def validate_application_property(self, app_id, expected_value):
        actual_value = self.get_system_management().get_application_property(app_id)
        self.assertEqual(expected_value, actual_value, 
                         f"The application property {app_id} should be {expected_value}, but actual is {actual_value}.")
        
    def validate_policy_exists(self, policy_name):
        actual_value = self.get_system_management().is_policy_existed(policy_name)
        self.assertTrue(actual_value,
                         f"The policy {policy_name} is not found!")
        
    def validate_user_exists(self, user_name):
        actual_value= self.get_system_management().is_user_existed(user_name)
        self.assertTrue(actual_value,
                         f"The user {user_name} is not found!")
        
    def validate_policy_assignment(self, user_name, expected_policies):
        actual_assigned_policies = self.get_system_management().get_user_assigned_policies(user_name)
        self.assertTrue(set(expected_policies).issubset(set(actual_assigned_policies)),
                         f"The user {user_name} should be assigned policies {expected_policies}, but actual policies are {actual_assigned_policies}")  
    
    def validate_file_exists_in_user_workspace(self, file_full_path):
        actual_existed = self.get_system_management().is_file_existed(file_full_path)
        self.assertTrue(actual_existed,
                        f"The file {file_full_path} does not existed!")
        
    def validate_folder_exists_in_user_workspace(self, folder_full_path):
        actual_existed = self.get_system_management().is_folder_existed(folder_full_path)
        self.assertTrue(actual_existed,
                        f"The folder {folder_full_path} does not existed!")
        
    def validate_file_count_in_user_workspace(self, folder_full_path, file_name, expected_file_count):
        file_list = self.get_system_management().get_file_by_filter(folder_full_path, file_name)
        actual_file_count = 0 if file_list is None else len(file_list)
        self.assertEqual(expected_file_count, actual_file_count, 
                         f"The count of files under the folder {folder_full_path} should be {expected_file_count}, but actual is {actual_file_count}.")
        
    # **************************Connection Management************************
    def validate_connection_status(self, conn_id, expected_status):
        actual_status = self.get_connection_management().get_connection_status(conn_id)
        self.assertEqual(expected_status, actual_status, 
                         f"The connection {conn_id} status should be {expected_status}, but actual is {actual_status}.")
    
    # ****************************Metadata Explorer************************
    def validate_metadata_memory_limit(self, expected_metadata_limit, expected_preparation_limit):
        actual_metadata_limit, actual_prepartion_limit = self.get_metadata_explorer().get_dashboard_memory_limit()
        self.assertEqual(expected_metadata_limit, actual_metadata_limit, 
                         f"The metadata memory limit should be {expected_metadata_limit}, but actual is {actual_metadata_limit}.")
        self.assertEqual(expected_preparation_limit, actual_prepartion_limit, 
                         f"The metadata memory limit should be {expected_preparation_limit}, but actual is {actual_prepartion_limit}.")
    
    def validate_metadata_tags_exist(self, hierarchy_name: str, expected_tags: list):
        actual_tags = self.get_metadata_explorer().get_tags_name_under_hierarchy(hierarchy_name)
        self.assertTrue(set(expected_tags).issubset(set(actual_tags)),
                      f"The hierarchy {hierarchy_name} does not contains tags {expected_tags}")
        
    def vaidate_metadata_file_status(self, conn_name, file_path, expected_published=None, expected_profiled=None, expected_has_lineage=None):
        actual_metadata = self.get_metadata_explorer().get_file_metadata(conn_name, file_path)
        assert_msg_format = "The {0} status of file {1}:{2} should be {3}"
        if expected_published is not None:
            self.assertEqual(expected_published, actual_metadata["is_published"],
                             assert_msg_format.format("published", conn_name, file_path, expected_published))
        if expected_profiled is not None:
            self.assertEqual(expected_profiled, actual_metadata["is_profiled"],
                             assert_msg_format.format("profiled", conn_name, file_path, expected_profiled))
        if expected_has_lineage is not None:
            self.assertEqual(expected_has_lineage, actual_metadata["has_lineage"],
                             assert_msg_format.format("having lineage", conn_name, file_path, expected_has_lineage))
            
    def validate_metadata_tags_on_file(self, conn_name, file_path, hierarchy_name: str, expected_tags:list):
        actual_tags = self.get_metadata_explorer().get_tags_on_file(conn_name, file_path).get(hierarchy_name)
        self.assertTrue(set(expected_tags).issubset(set(actual_tags)),
                      f"The file {file_path} under connection {conn_name} does not contains tags {hierarchy_name}/{expected_tags}")
         
    def validate_metadata_sample_row(self, conn_name, file_path, row_index, expected_row):
        actual_row = self.get_metadata_explorer().get_sample_row_of_previewed_data(conn_name, file_path, row_index)
        self.assertEqual(expected_row, actual_row,
                         f"The row {conn_name}/{file_path}:{row_index} should be {expected_row}, but actual is {actual_row}")
        
    def validate_metadata_row_count(self, conn_name, file_path, expected_count):
        actual_count= self.get_metadata_explorer().get_count_of_data(conn_name, file_path)
        self.assertEqual(expected_count, actual_count,
                         f"The count of the file {conn_name}/{file_path} should be {expected_count}, but actual is {actual_count}")
                
    def validate_metadata_rule_exist(self, rule_category_name, rule_name):
        actual_exist = self.get_metadata_explorer().is_rule_existed(rule_category_name, rule_name)
        self.assertTrue(actual_exist,
                         f"The rule {rule_category_name}/{rule_name} is not found!")
        
    def validate_metadata_rulebook_result(self, rulebook_id, expected_passrate):
        actual_passrate = self.get_metadata_explorer().get_view_result_in_rulebook(rulebook_id)
        self.assertEqual(expected_passrate, actual_passrate,
                         f"The rulebook validation passrate should be {expected_passrate}, but actual is {actual_passrate}")
    
    def validate_metadata_rule_scorecard_score(self, ruledashboard_id, group_index, scorecard_index, expected_score):
        actual_score = self.get_metadata_explorer().get_scorecard_data_in_rule_dashboard(ruledashboard_id, group_index, scorecard_index).score
        self.assertEqual(expected_score, actual_score,
                         f"The score of the scorecard at position:[{group_index}][{scorecard_index}] should be {expected_score}, but actual is {actual_score}")
    
    def validate_metadata_rule_scorecard_percentage(self, ruledashboard_id, group_index, scorecard_index, expected_percentage):
        actual_percentage = self.get_metadata_explorer().get_scorecard_data_in_rule_dashboard(ruledashboard_id, group_index, scorecard_index).pass_percent
        self.assertEqual(actual_percentage, expected_percentage,
                         f"The percentage of the scorecard at position:[{group_index}][{scorecard_index}] should be {expected_percentage}, but actual is {actual_percentage}")    
    
    def validate_metadata_terms(self, glossary_name, expected_terms):
        actual_terms = self.get_metadata_explorer().get_terms_in_glossary(glossary_name)
        self.assertEqual(expected_terms, actual_terms,
                             f"The terms in glossary {glossary_name} should be {expected_terms}, but actual are {actual_terms}")
        
    def validate_metadata_favorite(self, conn_name, folder_path="/"):
        actual_favorites = self.get_metadata_explorer().get_favorites()
        if folder_path == "/":
            expected_favorite =  f"/{conn_name}"
        else:
            expected_favorite = f"/{conn_name}{folder_path}"
        self.assertIn(expected_favorite, actual_favorites,
                      f"The folder {folder_path} under connection {conn_name} should be favorited")
        
    def validate_metadata_monitor_task(self, expected_tasks):
        actual_tasks = self.get_metadata_explorer().get_monitor_tasks()
        actual_set = set(actual_tasks)
        expected_set = set(expected_tasks)
        self.assertTrue(expected_set.issubset(actual_set),
                        f"Tasks {expected_tasks} should not listed in current tasks {actual_tasks}.")
        
    def validate_metadata_schedules(self, expected_schedules):
        actual_schedules = self.get_metadata_explorer().get_schedules()
        actual_set = set(actual_schedules)
        expected_set = set(expected_schedules)
        self.assertTrue(expected_set.issubset(actual_set),
                        f"Schedules {expected_schedules} should not listed in current schedules {actual_schedules}.")
    # ****************************Modeler************************
    def validate_graph_status(self, graph_name, expected_status):
        executions = self.get_graph().get_executions(run_name=graph_name) #TODO: Check whether it's really the run name
        execution = executions[0]
        actual_status = execution.status
        self.assertEqual(expected_status, actual_status, 
                         f"The graph {graph_name} status should be {expected_status}, but actual is {actual_status}.")
           
    # ****************************Monitoring************************
    def validate_rf_status(self, rf_name, expected_status):
        actual_status = self.get_rms().get_replication_flow_status(rf_name)
        self.assertEqual(expected_status, actual_status, 
                         f"The replication flow {rf_name} status should be {expected_status}, but actual is {actual_status}.")
        
    def validate_schedule_exist(self, schedule_name):
        actual_is_not_existed = self.get_monitoring().schedules.get_schedule(schedule_name) is None
        self.assertFalse(actual_is_not_existed,
                         f"The schedule {schedule_name} status should be existed.")
        
    ## ***************************Validation End************************
    
    ### ************************Public Functions End***********************
    
    
    ### ************************Private Functions Begin**************************
    @classmethod  
    def _load_descriptor(cls):
        descriptor_file_path = os.path.join(os.path.dirname(sys.modules[cls.__module__].__file__), 'descriptor.json')       
        with open(descriptor_file_path, "r") as f:
            cls._descriptor = json.load(f)
            
    @classmethod   
    def _init_cluster(cls):
        cluster_name = os.environ.get("cluster", cls._descriptor["cluster"])
        url = os.environ.get("url")
        if url is None or len(url) == 0: # read cluster config from file if not set by env
            cluster_info = ConnectionData.for_cluster(cluster_name, cls.__ENV_FILE_ROOT_PATH)
        else: # read cluster config from env
            os.environ.setdefault(f"{cluster_name}_BASEURL", url)
            os.environ.setdefault(f"{cluster_name}_TENANT", os.environ.get("tenant"))
            cluster_info = ConnectionData.for_cluster(cluster_name)
        cls.__cluster = Cluster.connect_to(cluster_info)
    
    @classmethod    
    def _init_connections(cls):
        for conn in cls._descriptor["connections"]:
            conn_type = conn["type"]
            conn_id = conn["value"] # connection id of connection config under the connection_data fold or env
            conn_name = conn["name"] # connection name in DI
            conn_timeout = conn.get("timeOut")
            cls.__connections[conn_name] = ConnConfig(conn_id, conn_type, conn_timeout)
    
    def _abap_subscription_name_can_be_erased(self, abap_connection_id: str, subscription_name: str):
        url = f"/app/axino-service/ape/v1/{abap_connection_id}/operator/com.sap.abap.subscr.eraser/subscription"
        sub_objs = self.get_cluster().api_get(url, 200).json()
        if not hasattr(sub_objs, "__iter__"): # if api response is not iterable, such as null
            return False, ""
        the_sub_obj_not_in_use_list:list[dict] = list(filter(lambda x: x.get("Subscription Name") == subscription_name and (x.get("Graph Status") in ["STOP", "STEP"] or x.get("Graph StatuS") in ["STOP", "STEP"]), sub_objs))
        if len(the_sub_obj_not_in_use_list) > 0:
            return True, the_sub_obj_not_in_use_list[0].get("Subscription ID")
        return False, ""
    
    def _get_abap_subscription_id_by_name(self, abap_connection_id: str, subscription_name: str):
        url = f"/app/axino-service/ape/v1/{abap_connection_id}/operator/com.sap.abap.subscr.eraser/subscription"
        sub_objs = self.get_cluster().api_get(url, 200).json()
        if not hasattr(sub_objs, "__iter__"): # if api response is not iterable, such as null
            return ""
        the_sub_obj_list:list[dict] = list(filter(lambda x: x.get("Subscription Name") == subscription_name, sub_objs))
        if len(the_sub_obj_list) > 0:
            return the_sub_obj_list[0].get("Subscription ID")
        return ""
    
    def _abap_subscription_name_is_new(self, abap_connection_id: str, subscription_name: str):
       return len(self._get_abap_subscription_id_by_name(abap_connection_id, subscription_name)) == 0
        
    ### ************************Private Functions End**************************
    
   