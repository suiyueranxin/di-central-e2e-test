from __future__ import annotations
from typing import Optional
from case_base import BaseCase
from di_qa_e2e.models.graph import GraphExecution, GraphStatus

class PipelineCase(BaseCase):
    # static variables
    
    # class variables
    __gragh_name_prefix = "CET-TEST"
    __conn_data = {}
    
    ### ************************Hook Begin***************************   
    # call once before each test suite    
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # init conn data such as hana target schema
        cls._init_conn_data()
        # init graph data such as graph name prefix
        cls._init_graph_data()  
    
    # call once before each test method  
    def setUp(self):
        # init graph name list used to clean running graph
        self._graph_names = []
        # init abap subscription names (abap_conn_name, abap_subscription_name) list used to clean using subscription
        self._abap_subscription_names = []
        # initialize graph execution list, which will be cleaned up in tearDown hook.
        self._graph_executions = []
        pass
    
    # call once after each test method
    def tearDown(self):
        # stop graphs
        graph_executions = self._graph_executions
        while graph_executions:
            graph_execution = graph_executions.pop()
            graph_execution.update()
            if graph_execution.status == GraphStatus.RUNNING:
                graph_execution.stop()

        # erase abap subscription
        while len(self._abap_subscription_names) > 0:
            conn, sub_name = self._abap_subscription_names.pop()
            self.abap_erase_subscription_id_by_name(conn, sub_name)
    ### ************************Hook End***************************
    
    ### Public functions

    def run_graph_until_running(self, 
                    graph_full_name: str, 
                    graph_run_name: str,
                    should_clean_execution: bool = True,
                    max_wait_time: int = GraphExecution.MAX_WAIT_TIME,
                    config_substitutions: dict = {}, 
                    snapshot_config: dict = {}, 
                    auto_restart_config: dict = {},
                    should_immediate_stop: bool = False) -> GraphExecution:
        """
        Start graph and wait until the graph reaches the expected status.

        Parameters
        ----------
        graph_full_name: str
            Full name of a graph.

        graph_run_name: str
            Run name of a graph.
        
        should_clean_execution: bool
            Graph execution will be cleaned up after the execution of all the test cases.

        max_wait_time: int
            Maximum waiting time for a graph to reach running status.

        config_substitutions: dict
            The configuration substitutions of the parameterized graph.
        
        snapshot_config: dict
            The snapshot configuration of a graph.
        
        auto_restart_config: dict
            The auto-restart configuration of a graph.
        
        should_immediate_stop: bool
            Indicate if the execution should be stopped immediately after it is in running status.
        """
        execution = self._run_graph(graph_full_name,
                                    graph_run_name,
                                    should_clean_execution = should_clean_execution,
                                    config_substitutions = config_substitutions, 
                                    snapshot_config = snapshot_config, 
                                    auto_restart_config = auto_restart_config)
        execution.wait_until_running(max_wait_time)
        if should_immediate_stop:
            execution.stop()
        return execution
    
    def run_graph_until_completed(self, 
                    graph_full_name: str, 
                    graph_run_name: str,
                    should_clean_execution: bool = True,
                    max_wait_time: int = GraphExecution.MAX_WAIT_TIME,
                    config_substitutions: dict = {}, 
                    snapshot_config: dict = {}, 
                    auto_restart_config: dict = {}) -> GraphExecution:
        """
        Run graph and wait until the graph reaches the completed status.

        Parameters
        ----------
        graph_full_name: str
            Full name of a graph.

        graph_run_name: str
            Run name of a graph.
        
        should_clean_execution: bool
            Graph execution will be cleaned up after the execution of all the test cases.

        max_wait_time: Optional[int]
            Maximum waiting time for a graph to reach GraphStatus.RUNNING status.

        config_substitutions: dict
            The configuration substitutions of the parameterized graph.
        
        snapshot_config: dict
            The snapshot configuration of a graph.
        
        auto_restart_config: dict
            The auto-restart configuration of a graph.
        """
        execution = self._run_graph(graph_full_name,
                                    graph_run_name,
                                    should_clean_execution = should_clean_execution,
                                    config_substitutions = config_substitutions, 
                                    snapshot_config = snapshot_config, 
                                    auto_restart_config = auto_restart_config)
        execution.wait_until_completed(max_wait_time)
        return execution


    def _get_execution(self, run_name: str, should_clean_execution: True) -> GraphExecution:
        """Returns the first graph execution with a matching run name"""
        graph = self.get_graph()
        executions = graph.get_executions(run_name=run_name)
        if not executions:
            raise RuntimeError(f"Could not get the execution of graph {run_name}")
        execution = executions[0]
        if should_clean_execution:
            self.push_graph_execution(execution)
        return execution
    
    ## *************************Actions Begin****************************
    def get_graph_name(self, graph_full_name, gen_version=None):
        graph_last_name = graph_full_name.split(".")[-1]
        if gen_version is None:
            return ("{0}-{1}").format(self.__gragh_name_prefix, graph_last_name)
        return ("{0}-{1}-{2}").format(self.__gragh_name_prefix, graph_last_name, gen_version)
    
    def get_row_count_from_hana(self, hana_conn_name, hana_table_name):
        hana_table_full_name = self._get_hana_table_full_name(hana_conn_name, hana_table_name)
        return self.get_conn_content_total_count(hana_conn_name, hana_table_full_name)
    
    def push_graph_name(self, graph_name):
        self._graph_names.append(graph_name)
    
    def push_graph_execution(self, graph_execution):
        self._graph_executions.append(graph_execution)
        
    def push_abap_subscription_name(self, abap_conn_name, sub_name):
        self._abap_subscription_names.append((abap_conn_name, sub_name))
         
    ## *************************Actions End***************************  
    
    ## *************************Validation Begin***********************
    def validate_total_count_from_abap_to_hana(self, abap_conn_name, abap_table_name, hana_conn_name, hana_table_name):
        hana_table_full_name = self._get_hana_table_full_name(hana_conn_name, hana_table_name)
        self.validate_conn_content_total_count_from_src_to_target(abap_conn_name, abap_table_name, hana_conn_name, hana_table_full_name)
   
    def validate_row_count_in_hana(self, hana_conn_name, hana_table_name, expected_count):
        hana_table_full_name = self._get_hana_table_full_name(hana_conn_name, hana_table_name)
        self.validate_conn_content_total_count(hana_conn_name, hana_table_full_name, expected_count)
    
    def validate_total_count_from_abap_to_filestore(self, abap_conn_name, abap_table_name, adlv2_conn_name, path, substring='', header_index="infer", is_row_count_same=True):
        self.validate_conn_content_total_count_from_src_to_filestore(abap_conn_name, abap_table_name, adlv2_conn_name, path, substring, header_index, is_row_count_same)
    
    def validate_row_count_in_adlv2(self, expected_count, adlv2_conn_name, path, substring='', header_index = 'infer', is_row_count_same=True):
        self.validate_filestore_conn_content_total_count(expected_count, adlv2_conn_name, path, substring, header_index, is_row_count_same)
    
    def assert_is_running(self, graph_execution: GraphExecution):
        """
        Check if the status of a graph execution is running.

        Parameters
        ----------
        graph_execution: GraphExecution
            Execution object of a graph run.
        """
        self._assert_status(graph_execution, GraphStatus.RUNNING)
    
    def assert_is_completed(self, graph_execution: GraphExecution):
        """
        Check if the status of a graph execution is completed.

        Parameters
        ----------
        graph_execution: GraphExecution
            Execution object of a graph run.
        """
        self._assert_status(graph_execution, GraphStatus.COMPLETED)
    
    def assert_execution_completed_by_graph_run_name(self, run_name: str, should_clean_execution: bool =  True):
        """
        Get the graph execution of the graph run_name and check if it is completed. 

        Parameters
        ----------
        run_name: str
            Run name of a graph
        should_clean_execution: bool
            Indicate if the graph execution should be stopped in tearDown hook.
        """
        execution = self._get_execution(run_name, should_clean_execution)
        self.assert_is_completed(execution)

    def assert_execution_running_by_graph_run_name(self, run_name: str, should_clean_execution: bool = True):
        """
        Get the graph execution of the graph run_name and check if it is running. 

        Parameters
        ----------
        run_name: str
            Run name of a graph
        should_clean_execution: bool
            Indicate if the graph execution should be stopped in tearDown hook.
        """
        execution = self._get_execution(run_name, should_clean_execution)
        self.assert_is_running(execution)

    ## ***************************Validation End************************
    
    
    ## private functions
    @classmethod
    def _init_conn_data(cls):
        if "connData" in cls._descriptor:
            cls.__conn_data = cls._descriptor.get("connData")
    
    @classmethod    
    def _init_graph_data(cls):
        if "graphData" in cls._descriptor and "namePrefix" in cls._descriptor["graphData"]:
            cls.__gragh_name_prefix = cls._descriptor["graphData"]["namePrefix"]
           
    def _get_hana_target_schema(self, hana_conn_name):
        if hana_conn_name in self.__conn_data:
            return self.__conn_data.get(hana_conn_name).get("targetSchema")
        return "CET_TEST"
    
    def _get_hana_table_full_name(self, hana_conn_name, hana_table_name):
        return f"{self._get_hana_target_schema(hana_conn_name)}.{hana_table_name}"
    
    def _run_graph(self,
                   graph_full_name: str, 
                   graph_run_name: str, 
                   should_clean_execution: bool,
                   config_substitutions: dict, 
                   snapshot_config: dict, 
                   auto_restart_config: dict) -> GraphExecution:
        """
        Start graph and return the execution object.

        Parameters
        ----------
        graph_full_name: str
            Full name of a graph.

        graph_run_name: str
            Run name of a graph.
        
        should_clean_execution: bool
            Graph execution will be cleaned up after the execution of all the test cases.

        config_substitutions: dict
            The configuration substitutions of the parameterized graph.
        
        snapshot_config: dict
            The snapshot configuration of a graph.
        
        auto_restart_config: dict
            The auto-restart configuration of a graph.
    
        """
        graph = self.get_graph()
        graph_execution = graph.run_graph(graph_full_name, 
                                          graph_run_name, 
                                          config_substitutions=config_substitutions,
                                          snapshot_config=snapshot_config,
                                          auto_restart_config=auto_restart_config)
        if should_clean_execution:
            self.push_graph_execution(graph_execution)
        return graph_execution
    
    def _assert_status(self, graph_execution: GraphExecution, expected_status: GraphStatus):
        """
        Check if the status of a graph execution matches the expected status.

        Parameters
        ----------
        graph_execution: GraphExecution
            Execution object of a graph run.
        
        expected_status: GraphStatus
            Expected status of a graph.
        """
        graph_execution.update()
        self.assertEqual(expected_status, graph_execution.status, f'The status of graph {graph_execution.run_name} should be {expected_status.value}')
       
        
    