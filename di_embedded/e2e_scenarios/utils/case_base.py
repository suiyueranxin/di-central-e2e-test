import unittest
from di_embedded_e2e.cluster import Cluster
import os
from di_qa_e2e.connections.connection_data import ConnectionData
from utils.content.conn import ConnConfig, ConnContent, ConnType
from di_embedded_e2e.flows.dataflow.dataflow import Dataflow, DataflowExecution, DataflowStatus
from di_embedded_e2e.flows.replication_flow import ReplicationFlow, ReplicationFlowExecution, ReplicationFlowStatus
from di_qa_e2e_validation.gcbigquery.GCBigQueryClient import GCBigQueryClient
from di_qa_e2e.connections.connection_data import ConnectionData
import json


class BaseTest(unittest.TestCase):
    
    # static variables
    __ENV_FILE_ROOT_PATH = os.path.join(os.path.dirname(__file__), '../connection_data')
    
    # connect to cluster
    _descriptor = {} 
    __cluster = None
    __connection_config = {}
    __test_data_container = ""

    @classmethod
    def setUpClass(cls):
        # load descriptor which defines cluster, connections
        cls._load_descriptor()
        cls.__test_data_container = cls._descriptor.get("testDataContainer")
        
    @classmethod
    def tearDownClass(cls):
        # clean cluster
        cls.__cluster = None
        
    # call once before each test method  
    def setUp(self):
        self._data_flow_names = []
        self._replication_flow_names = []
        
    # call once after each test method
    def tearDown(self):
        # stop data flow execution by name
        while len(self._data_flow_names) > 0:
            df_name = self._data_flow_names.pop()
            self.safe_stop_data_flow(df_name)
            
        # stop replication flow execution by name
        while len(self._replication_flow_names) > 0:
            rf_name = self._replication_flow_names.pop()
            self.safe_stop_replication_flow(rf_name)
    
    ### ************************Public Functions Begin***********************
    ## *************************Actions Begin****************************
    @property
    def cluster(self) -> Cluster:
        return self.get_cluster()
    
    @classmethod 
    def get_cluster(cls) -> Cluster:
        if cls.__cluster is None:
            cls._init_cluster()
        return cls.__cluster
    
    @classmethod
    def get_connection_session(cls, conn_name):
        conn_info: ConnConfig = cls.get_connection_config(conn_name)
        return conn_info.get_conn_client()
     
    @classmethod
    def get_connection_config(cls, conn_name):
        if not cls.__connection_config:
            cls._init_connections()
        return cls.__connection_config.get(conn_name)
    
    # ABAP, HANA
    def get_table_total_count_from_data_server(self, conn_name, table_name):
        conn_info: ConnConfig = self.get_connection_config(conn_name)
        # handle HANA schema
        conn_type = conn_info.get_conn_type()
        if conn_type == ConnType.HANA.value or conn_type == ConnType.HDL_DB.value:
            table_name = self.get_table_name_with_schema(table_name)
        conn_content = ConnContent(conn_info, table_name)
        return conn_content.get_count_of_content()
    
    # HDL_FILES, ADL_V2
    def get_non_table_total_count_from_data_server(self, conn_name, folder_path, substring='', header_index='infer', is_row_count_same=True):
        conn_info: ConnConfig = self.get_connection_config(conn_name)
        conn_type = conn_info.get_conn_type()
        conn_content = ConnContent(conn_info, folder_path)
        if conn_type == ConnType.HDL_FILES.value:
            return conn_content.get_count_of_content()
        elif conn_type == ConnType.ADL_V2.value:
            return conn_content.get_count_of_content_from_adlv2(substring, header_index, is_row_count_same)
        return 0
    
    # GBQ
    def get_table_total_count_from_gbq(self, conn_name, table_name):
        conn_info: ConnConfig = self.get_connection_config(conn_name)
        conn_content = ConnContent(conn_info, table_name)
        row_count = conn_content.get_count_of_content()
        return row_count
    
    # S3
    def get_table_total_count_from_S3_server(self, conn_name, folder_path):
        conn_info: ConnConfig = self.get_connection_config(conn_name)
        conn_content = ConnContent(conn_info, folder_path)
        return conn_content.get_count_of_content()
    
    # GCS
    def get_table_total_count_from_GCS_server(self, conn_name, folder_path):
        conn_info: ConnConfig = self.get_connection_config(conn_name)
        conn_content = ConnContent(conn_info, folder_path)
        return conn_content.get_count_of_content()
    
    # DWC
    def get_table_total_count_from_dwc(self, table_name=None, is_remote=False):
        table = self.get_cluster().data_builder.table
        if is_remote: # which conn type support remote table?
            return table.get_remote_table_row_count(table_name)
        else:
            return table.get_local_table_row_count(table_name)
    
    def get_table_name_with_schema(self, table_name, schema=None, join="."):
        if schema is None:
            schema = self.__test_data_container
        return f"{schema}{join}{table_name}"
    
    def push_data_flow(self, df_name):
        self._data_flow_names.append(df_name)
        
    def push_replication_flow(self, rf_name):
        self._replication_flow_names.append(rf_name)
    
    def get_data_flow(self, df_name) -> Dataflow:
        data_builder = self.get_cluster().data_builder
        data_flow = data_builder.open_data_flow(df_name)
        return data_flow
    
    def get_replication_flow(self, rf_name) -> ReplicationFlow:
        data_builder = self.get_cluster().data_builder
        replication_flow = data_builder.open_replication_flow(rf_name)
        return replication_flow
    
    def safe_run_data_flow(self, df_name) -> DataflowExecution:
        data_flow = self.safe_stop_data_flow(df_name)
        data_flow_execution = data_flow.run()
        return data_flow_execution
        
    def safe_run_replication_flow(self, rf_name) -> ReplicationFlowExecution:
        replication_flow = self.safe_stop_replication_flow(rf_name)
        replication_flow_execution = replication_flow.run()
        return replication_flow_execution
        
    def safe_stop_data_flow(self, df_name) -> Dataflow:
        data_flow = self.get_data_flow(df_name)
        data_flow_execution = data_flow.get_latest_execution()
        if data_flow_execution.status == DataflowStatus.RUNNING:
            data_flow_execution.stop()
        return data_flow
    
    def safe_stop_replication_flow(self, rf_name) -> ReplicationFlow:
        replication_flow = self.get_replication_flow(rf_name)
        replication_flow_execution = replication_flow.get_latest_execution()
        if replication_flow_execution.status == ReplicationFlowStatus.RUNNING:
            replication_flow_execution.stop()
        return replication_flow

    ## *************************Actions End***************************
    
    ## *************************Validation Begin***********************
    def validate_table_total_count_by_src_and_target(self, src_conn_name, src_table, target_conn_name, target_table, src_count, target_count):
        self.assertEqual(src_count, target_count, 
                         f"The record number should be the same. Total of source {src_conn_name}/{src_table} is {src_count} and total of target {target_conn_name}/{target_table} is {target_count}.")
    
    def validate_table_total_count(self, conn_name, table, actual_count, expected_count):
        self.assertEqual(actual_count, expected_count, 
                         f"The record number should be the same. Total of source {conn_name}/{table} should be {expected_count} but actual is {actual_count}.")
    ## ***************************Validation End************************
    ### ************************Public Functions End***********************
    
    ### ************************Private Functions Begin************************** 
    @classmethod  
    def _load_descriptor(cls):
        descriptor_file_path = os.path.join(os.path.dirname(__file__), './descriptor.json')      
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
            cluster_info = ConnectionData.for_cluster(cluster_name)
        space = cls._descriptor["space"]
        cls.__cluster = Cluster.connect_to(cluster_info, space)
    
    @classmethod    
    def _init_connections(cls):
        for conn in cls._descriptor["connections"]:
            conn_type = conn["type"]
            conn_id = conn["value"] # connection file name under the connection_data fold
            conn_name = conn["name"] # connection name in DSC
            conn_timeout = conn.get("timeOut")
            cls.__connection_config[conn_name] = ConnConfig(conn_id, conn_type, conn_timeout)
    
    ### ************************Private Functions End**************************