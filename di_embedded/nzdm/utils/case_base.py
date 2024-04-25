import unittest
import os
import json
from rich import print
from di_qa_e2e.connections.connection_data import ConnectionData
from utils.content.conn import ConnConfig, ConnType, ConnContent, ConnCreationConfig
from di_embedded_api_test.cluster import Cluster
from di_embedded_api_test.components.connection.connection_manager import ConnectionManagement, Connection
from di_embedded_api_test.components.connection.table_metadata import TableMetaData
from di_embedded_api_test.components.rms.task import TaskEntity, TaskStatus
from utils.content.task import Task
from di_qa_e2e.models.graph import Graph, GraphExecution, GraphStatus


class BaseTest(unittest.TestCase):
    
    # static variables
    __ENV_FILE_ROOT_PATH = os.path.join(os.path.dirname(__file__), '../connection_data')
    __RF_DATA_PATH = os.path.join(os.path.dirname(__file__), '../rf_data')
    __GRAPH_DATA_PATH = os.path.join(os.path.dirname(__file__), '../graph_data')
    
    # connect to cluster
    _descriptor = {} 
    __cluster = None
    __connection_config = {}
    __connection_creation_config = {}
    __test_data_container = ""

    @classmethod
    def setUpClass(cls):
        # load descriptor which defines cluster, connections
        cls._load_descriptor()
        cls.__test_data_container = cls._descriptor.get("testDataContainer")
        cls._stage = os.environ.get("stage", "pre-upgrade")
        cls._max_retries = os.environ.get("max_retries", 240)
        
    @classmethod
    def tearDownClass(cls):
        # clean cluster
        cls.__cluster = None
        
    # call once before each test method  
    def setUp(self):
        self._rf_task_names = []
        self._df_graph_names = []
        
    # call once after each test method
    def tearDown(self):
        # clean replication task by name
        while len(self._rf_task_names) > 0:
            rf_name = self._rf_task_names.pop()
            self.safe_delete_rf_task(rf_name)

        # clean df graph by name
        while len(self._df_graph_names) > 0:
            graph_name = self._df_graph_names.pop()
            self.safe_stop_graph(graph_name)
    
    ### ************************Public Functions Begin***********************
    ## *************************Actions Begin****************************
    
    @property
    def cluster(self) -> Cluster:
        return self.get_cluster()
    
    @property
    def is_pre_upgrade(self):
        return self._stage == "pre-upgrade"
    
    @property
    def is_during_ugprade(self):
        return self._stage == "upgrade"
    
    @property
    def is_post_upgrade(self):
        return self._stage == "post-upgrade"
    
    @classmethod 
    def get_cluster(cls) -> Cluster:
        if cls.__cluster is None:
            cls._init_cluster()
        return cls.__cluster
    
    @classmethod
    def get_connection_session(cls, conn_name):
        conn_info: ConnConfig = cls.get_connection_config(conn_name)
        return conn_info.conn_client
     
    @classmethod
    def get_connection_config(cls, conn_name):
        if not cls.__connection_config:
            cls._init_connections()
        return cls.__connection_config.get(conn_name)
    
    @classmethod
    def get_connection_creation_config(cls, conn_name):
        if not cls.__connection_creation_config:
            cls._init_connections(for_conn_creation=True)
        return cls.__connection_creation_config.get(conn_name)
    
    def safe_create_connection(self, conn_name) -> Connection:
        self.safe_delete_connection(conn_name)

        connectionmanagement:ConnectionManagement = self.get_cluster().connectionmanagement
        conn_info: ConnCreationConfig = self.get_connection_creation_config(conn_name)
        conn_data = conn_info.conn_data
        conn_type = conn_info.conn_type
        connection: Connection = None
        if conn_type ==  ConnType.ABAP.value:
           connection = connectionmanagement.create_abap_connection(conn_name, conn_data)
        elif conn_type == ConnType.HANA.value:
            connection = connectionmanagement.create_hana_connection(conn_name, conn_data)
        elif conn_type == ConnType.ADL_V2.value:
            connection = connectionmanagement.create_datalake_connection(conn_name, conn_data)
        elif conn_type == ConnType.BIGQUERY.value:
            connection = connectionmanagement.create_gbq_connection(conn_name, conn_data)
        elif conn_type == ConnType.HDL_FILES.value:
            connection = connectionmanagement.create_hana_datalake_file_connection(conn_name, conn_data)
        elif conn_type == ConnType.S3.value:
            connection = connectionmanagement.create_s3_connection(conn_name, conn_data)
        elif conn_type == ConnType.GCS.value:
            connection = connectionmanagement.create_gcs_connection(conn_name, conn_data)
        return connection

    def safe_delete_connection(self, conn_name):
        connectionmanagement:ConnectionManagement = self.get_cluster().connectionmanagement
        if connectionmanagement.check_connection_exist(conn_name):
            connectionmanagement.delete_connection(conn_name)

    def get_connection(self, conn_name) -> Connection:
        connectionmanagement:ConnectionManagement = self.get_cluster().connectionmanagement
        return connectionmanagement.get_connection(conn_name)
    
    def get_table_name_with_schema(self, table_name, schema=None, join="."):
        if schema is None:
            schema = self.__test_data_container
        return f"{schema}{join}{table_name}"
    
    def push_rf_task(self, task_name):
        self._rf_task_names.append(task_name)

    def safe_delete_rf_task(self, task_name):
        task: Task = Task(self.cluster.rms, task_name)
        task.cleanup()

    def safe_create_rf_task(self, rf_data_file_name, auto_clean=True):
        task_name = rf_data_file_name
        task = Task(self.cluster.rms, task_name)
        task.cleanup()

        # read task info from local json
        task_json_path = os.path.join(self.__RF_DATA_PATH, f'{rf_data_file_name}.json')      
        with open(task_json_path, "r") as f:
            task_json = json.load(f)

       # source
        source = task_json.get("source")
        source_connection_name = source.get("name")
        source_connection = self.get_connection(source_connection_name)
        source_container_name = source.get("container")
        source_table = source.get("table")
        
        # target
        target = task_json.get("target")
        target_connection_name = target.get("name")
        target_connection = self.get_connection(target_connection_name)
        target_container_name = target.get("container")
        target_table = target.get("table") 
        
        # Create source space properties   
        source_space_name = task.source_space
        source_metadata = source_connection.get_table_definition(source_container_name, source_table)
        # print(source_metadata)
        source_space_properties = {
            "name": source_space_name,
            "connection_data": source_connection,
            "container": source_container_name,
            "table": source_metadata["table"],
            "scalar": source_metadata["scalar"]
        }
    
        # Create target space properties
        target_space_name = task.target_space
        if target_connection.typeId == "BIGQUERY":
            table_metadata = TableMetaData.convert_to_GBQ_table_definition(source_connection.table_description)
        elif target_connection.typeId in ["HDL_FILES","ADL_GEN2","S3","GCS"]:
            target_format_properties = {"compression": "NONE",
                                    "format": "PARQUET",
                                    "groupDeltaFilesBy": "NONE"
                                }
            table_metadata = TableMetaData.convert_to_storageDB_table_definition(source_connection.table_description, table_format_properties = target_format_properties)
        else:
            table_metadata = source_metadata
        target_metadata = {"table":{target_table:{}},"scalar":{}}
        target_metadata["table"][target_table] = table_metadata["table"][source_table]
        target_metadata["table"][target_table]["description"] = target_table
        target_metadata["scalar"] = table_metadata["scalar"]
        target_metadata["table"][target_table]["description"] = target_table
        # print(target_metadata)
        target_space_properties = {
            "name": target_space_name,
            "connection_data": target_connection,
            "container": target_container_name,
            "table": target_metadata["table"],
            "scalar": target_metadata["scalar"]
        }
        
        # Create constellation properties
        constellation_properties = {
            "name": task.constellation,
            "is_active": True,
            "priority": 50
        }
        
        # Create task properties
        task_properties = task_json.get("task")

        # Create replication flow
        task.runtime_instance = self.cluster.rms.create_replication_task(
            source_space_properties=source_space_properties,
            target_space_properties=target_space_properties,
            constellation_properties=constellation_properties,
            task_properties=task_properties
            )
        
        if auto_clean:
            self.push_rf_task(task.name)

        return task.runtime_instance
    
    def push_df_graph(self, graph_name):
        self._df_graph_names.append(graph_name)

    def safe_stop_graph(self, run_name):
        graph = self.cluster.session.modeler.graphs
        executions = graph.get_executions(run_name=run_name)
        if len(executions) > 0:
            graph_execution = executions[0]
            graph_execution.update()
            if graph_execution.status == GraphStatus.RUNNING:
                graph_execution.stop()
        
    def safe_run_graph(self, graph_data_file_name, auto_clean=True):
        graph_name = graph_data_file_name
        self.safe_stop_graph(graph_name)

        # read graph info from local json
        graph_json_path = os.path.join(self.__GRAPH_DATA_PATH, f'{graph_data_file_name}.json')      
        with open(graph_json_path, "r") as f:
            graph_json = json.load(f)

        query_path = "/app/pipeline-modeler/service/v1/runtime/graphs"
        response = self.cluster.session.api_post(query_path, data=json.dumps(graph_json), expected_status=200)
        content = json.loads(response.text)
        handle = content["handle"]
        graph: Graph = Graph(self.cluster.session, graph_name, graph_name)
        instance: GraphExecution = GraphExecution(handle, graph, content)

        if auto_clean:
            self.push_df_graph(graph_name)
        return instance
    
    def get_graph_execution_by_name(self, graph_name):
        graph = self.cluster.session.modeler.graphs
        executions = graph.get_executions(run_name=graph_name)
        graph_execution = None
        if len(executions) > 0:
            graph_execution = executions[0]
            graph_execution.update()
        return graph_execution
    
    def get_graph_status(self, graph_name):
        graph_execution = self.get_graph_execution_by_name(graph_name)
        return graph_execution.status if graph_execution else None

    # ABAP, HANA
    def get_table_total_count_from_data_server(self, conn_name, table_name):
        conn_info: ConnConfig = self.get_connection_config(conn_name)
        # handle HANA schema
        conn_type = conn_info.conn_type
        if conn_type == ConnType.HANA.value or conn_type == ConnType.HDL_DB.value:
            table_name = self.get_table_name_with_schema(table_name)
        conn_content = ConnContent(conn_info, table_name)
        return conn_content.get_count_of_content()
    
    # HDL_FILES, ADL_V2
    def get_non_table_total_count_from_data_server(self, conn_name, folder_path, substring='', header_index='infer', is_row_count_same=True):
        conn_info: ConnConfig = self.get_connection_config(conn_name)
        conn_type = conn_info.conn_type
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
    
    ## *************************Actions End***************************
    
    ## *************************Validation Begin***********************
    def validate_table_total_count_by_src_and_target(self, src_conn_name, src_table, target_conn_name, target_table, src_count, target_count):
        self.assertEqual(src_count, target_count, 
                         f"The record number should be the same. Total of source {src_conn_name}/{src_table} is {src_count} and total of target {target_conn_name}/{target_table} is {target_count}.")
    
    def validate_table_total_count_by_src_and_target_during_upgrade(self, src_conn_name, src_table, target_conn_name, target_table, src_count, target_count):
        self.assertGreaterEqual(src_count, target_count, 
                         f"The target count is increasing to source count. Total of source {src_conn_name}/{src_table} is {src_count} and total of target {target_conn_name}/{target_table} is {target_count}.")

    def validate_table_total_count(self, conn_name, table, actual_count, expected_count):
        self.assertEqual(actual_count, expected_count, 
                         f"The record number should be the same. Total of source {conn_name}/{table} should be {expected_count} but actual is {actual_count}.")
        
    def validate_rf_task_status(self, task_name, expected_task_status:TaskStatus):
        task = Task(self.cluster.rms, task_name)
        actual_status = task.get_task_status_by_name()
        self.assertTrue(actual_status == expected_task_status, 
                        f"The rf task should be {expected_task_status} but actual is {actual_status}.")
        
    def validate_graph_status(self, graph_name, expected_graph_status:GraphStatus):
        actual_graph_status = self.get_graph_status(graph_name)
        self.assertTrue(actual_graph_status == expected_graph_status, 
                        f"The graph status should be {expected_graph_status} but actual is {actual_graph_status}.")
        
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
        cls.__cluster:Cluster = Cluster.connect_to(cluster_info)
    
    @classmethod    
    def _init_connections(cls, for_conn_creation=False):
        for conn in cls._descriptor["connections"]:
            conn_type = conn["type"]
            conn_id = conn["value"] # connection file name under the connection_data fold
            conn_name = conn["name"] # connection name in DSC
            if for_conn_creation:
                cls.__connection_creation_config[conn_name] = ConnCreationConfig(conn_id, conn_type)
            else:
                if "specialDataSourceClient" in conn:
                    conn_id = conn["specialDataSourceClient"]                    
                cls.__connection_config[conn_name] = ConnConfig(conn_id, conn_type)
    
    ### ************************Private Functions End**************************