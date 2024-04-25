from time import sleep
from utils.case_pipeline import PipelineCase
from di_qa_e2e.models.graph import GraphStatus

class testCIT_Pipeline_Runtime(PipelineCase):
    __abap_cds_conn = "CIT_S4"
    __abap_slt_conn = "CIT_DMIS"
    __hana_conn = "CIT_HANA"
    __adlv2_conn = "CIT_FILE"

    def test_gen1_start_abap_cds_to_hana_initial_fat(self): 
        graph_full_name = "com.sap.abap-e2e-pip-g1.cds-to-hana-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        subscription = "cds-to-hana-initial-fat"

        self.abap_erase_subscription_id_by_name(self.__abap_cds_conn, subscription)
        self.run_graph_until_running(graph_full_name, graph_run_name, False)
        
    def test_gen1_validate_abap_cds_to_hana_initial_fat(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.cds-to-hana-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        hana_table = "GEN1_DHE2E_CDS_WF_LM_INITIAL"
        abap_table = "WF_LM"
        subscription = "cds-to-hana-initial-small"
        # Store subscription name, to clean it in test teardown
        self.push_abap_subscription_name(self.__abap_cds_conn, subscription)

        self.assert_execution_completed_by_graph_run_name(graph_run_name)
        self.validate_total_count_from_abap_to_hana(self.__abap_cds_conn, abap_table, self.__hana_conn, hana_table)    
        
    def test_gen1_start_abap_cds_to_filestore_initial_fat(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.cds-to-filestore-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        file_directory = "/CET_TEST/cds-to-filestore-initial-fat"
        subscription = "cds-to-filestore-initial-fat"

        self.abap_erase_subscription_id_by_name(self.__abap_cds_conn, subscription)
        # remove file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        self.run_graph_until_running(graph_full_name, graph_run_name, False)

    def test_gen1_validate_abap_cds_to_filestore_initial_fat(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.cds-to-filestore-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        file_directory = "/CET_TEST/cds-to-filestore-initial-fat"
        abap_table = "WF_LM"
        subscription = "cds-to-filestore-initial-fat"

        # Store the subscription, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_cds_conn, subscription)

        self.assert_execution_completed_by_graph_run_name(graph_run_name)
        self.validate_total_count_from_abap_to_filestore(self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, '.json')
   
    def test_gen1_start_abap_cds_to_kafka_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.cds-to-kafka-initial-delta"
        graph_for_check_full_name = "com.sap.abap-int-tools.g1-kafka-to-filestore"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen1-cds")
        subscription = "cds-to-kafka-initial-delta"
        file_directory = "/CET_TEST/cds-to-kafka-initial-delta"
        configSubstitutions = {
            "path": "/CET_TEST/cds-to-kafka-initial-delta/result_file_<counter>.csv",
            "topic": "topic-cds-to-kafka-initial-delta"}
        
        self.abap_erase_subscription_id_by_name(self.__abap_cds_conn, subscription)
        # remove file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        
        self.run_graph_until_running(graph_for_check_full_name, 
                         graph_for_check_run_name,
                         False,
                         config_substitutions=configSubstitutions)
        
        self.run_graph_until_running(graph_full_name, 
                         graph_run_name,
                         False)

    def test_gen1_validate_abap_cds_to_kafka_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.cds-to-kafka-initial-delta"
        graph_for_check_full_name = "com.sap.abap-int-tools.g1-kafka-to-filestore"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen1-cds")
        subscription = "cds-to-kafka-initial-delta"
        file_directory = "/CET_TEST/cds-to-kafka-initial-delta"
        abap_table = "WF_LM"
        
        # Store the subscription name, to clean them in test teardown

        self.push_abap_subscription_name(self.__abap_cds_conn, subscription)

        self.assert_execution_running_by_graph_run_name(graph_run_name)
        self.assert_execution_running_by_graph_run_name(graph_for_check_run_name)
           
        # Validate after initial load
        self.validate_total_count_from_abap_to_filestore(
            self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, 'csv')
       
        abap_rowcount = self.get_conn_content_total_count(self.__abap_cds_conn, abap_table)
        # Insert and delete 5 records for table dhe2e_wf_lm from abap s4 system
        s4_client = self.get_connection_client(self.__abap_cds_conn)
        s4_client.insert_records(abap_table, 5)
        sleep(15)
        self.validate_conn_content_total_count(self.__adlv2_conn, file_directory, abap_rowcount+5 )
        s4_client.delete_records(abap_table, 5)
        sleep(15)
        self.validate_conn_content_total_count(self.__adlv2_conn, file_directory, abap_rowcount+5+5 )
        self.assert_execution_running_by_graph_run_name(graph_run_name, False)
        self.assert_execution_running_by_graph_run_name(graph_for_check_run_name, False)
        
    def test_gen1_start_abap_cds_to_filestore_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.cds-to-filestore-initial-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        subscription = "cds-to-filestore-initial-delta"
        file_directory = "/CET_TEST/cds-to-filestore-initial-delta"
        
        self.abap_erase_subscription_id_by_name(self.__abap_cds_conn, subscription)
        # remove file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        self.run_graph_until_running(graph_full_name, graph_run_name, False)
        
    def test_gen1_validate_abap_cds_to_filestore_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.cds-to-filestore-initial-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        subscription = "cds-to-filestore-initial-delta"
        file_directory = "/CET_TEST/cds-to-filestore-initial-delta"
        abap_table = "WN_LM"
        # Store the subscription, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_cds_conn, subscription)
        self.assert_execution_running_by_graph_run_name(graph_run_name)
        
        # Validate after initial load
        self.validate_total_count_from_abap_to_filestore(
            self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, 'csv')
        self.validate_total_count_from_abap_to_filestore(
            self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, 'json')
        
        # Insert and delete 5 records for table dhe2e_wn_lm from abap s4 system
        s4_client = self.get_connection_client(self.__abap_cds_conn)
        s4_client.insert_records(abap_table, 5)
        # Validation starts here...
        sleep(10)
        self.validate_total_count_from_abap_to_filestore(
            self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, 'csv')
        self.validate_total_count_from_abap_to_filestore(
            self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, 'json')
                   
    def test_gen1_start_abap_slt_to_filestore_initial_fat(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.slt-to-filestore-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        file_directory = "/CET_TEST/slt-to-filestore-initial-fat"
        subscription = "slt-to-filestore-initial-fat"

        self.abap_erase_subscription_id_by_name(self.__abap_slt_conn, subscription)
        # remove file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        self.run_graph_until_running(graph_full_name, graph_run_name, False)
        
        graph_full_name = "com.sap.abap-e2e-pip-g1.slt-to-filestore-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        file_directory = "/CET_TEST/slt-to-filestore-initial-fat"
        subscription = "slt-to-filestore-initial-fat"
        abap_table = "WF_LM"
      
        # Store the subscription, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_slt_conn, subscription)
        self.assert_execution_completed_by_graph_run_name(graph_run_name)
        self.validate_total_count_from_abap_to_filestore(self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, '.json')
   
    def test_gen1_start_abap_slt_to_kafka_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.slt-to-kafka-initial-delta"
        graph_for_check_full_name = "com.sap.abap-int-tools.g1-kafka-to-filestore"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen1-slt")
        subscription = "slt-to-kafka-initial-delta"
        file_directory = "/CET_TEST/slt-to-kafka-initial-delta"
        configSubstitutions = {"path": "/CET_TEST/slt-to-kafka-initial-delta/result_file_<counter>.csv",
                               "topic": "slt-to-kafka-initial-delta"}
        
        self.abap_erase_subscription_id_by_name(self.__abap_slt_conn, subscription)
        # remove file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        
        self.run_graph_until_running(graph_for_check_full_name, 
                                     graph_for_check_run_name,
                                     False,
                                     config_substitutions=configSubstitutions)
        self.run_graph_until_running(graph_full_name, graph_run_name, False)
        
    def test_gen1_validate_abap_slt_to_kafka_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.slt-to-kafka-initial-delta"
        graph_for_check_full_name = "com.sap.abap-int-tools.g1-kafka-to-filestore"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen1-slt")
        subscription = "slt-to-kafka-initial-delta"
        file_directory = "/CET_TEST/slt-to-kafka-initial-delta"
        abap_table = "WS_LM"
        
        # Store the subscription, to clean them in test teardown

        self.push_abap_subscription_name(self.__abap_slt_conn, subscription)

        self.assert_execution_running_by_graph_run_name(graph_run_name)
        self.assert_execution_running_by_graph_run_name(graph_for_check_run_name)
        # Validate after initial load
        self.validate_total_count_from_abap_to_filestore(self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, 'csv', None)
       
        # Insert 5 records for table ws_lm from abap dmis system
        dmis_client = self.get_connection_client(self.__abap_slt_conn)
        dmis_client.insert_records(abap_table, 5)
        
        # validate data here
        sleep(20)
        self.validate_total_count_from_abap_to_filestore(
            self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, 'csv', None)
        self.assert_execution_running_by_graph_run_name(graph_run_name, False)
        self.assert_execution_running_by_graph_run_name(graph_for_check_run_name, False)

    def test_gen1_start_abap_slt_to_filestore_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.slt-to-filestore-initial-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        file_directory = "/CET_TEST/slt-to-filtestore-intial-delta"
        subscription = "slt-to-filestore-initial-delta"

        self.abap_erase_subscription_id_by_name(self.__abap_slt_conn, subscription)
        # remove file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        self.run_graph_until_running(graph_full_name, graph_run_name, False)

    def test_gen1_validate_abap_slt_to_filestore_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.slt-to-filestore-initial-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        file_directory = "/CET_TEST/slt-to-filtestore-intial-delta"
        subscription = "slt-to-filestore-initial-delta"
        abap_table = "WN_LM"
        
        # Store the subscription, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_slt_conn, subscription)
        self.assert_execution_running_by_graph_run_name(graph_run_name)
        # Validate after initial load
        self.validate_total_count_from_abap_to_filestore(self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, 'csv')

        # insert 5 records for table wn_lm from abap dmis system
        dmis_client = self.get_connection_client(self.__abap_slt_conn)
        dmis_client.insert_records(abap_table, 5)
        sleep(30)
        
        # Validation starts here...
        self.validate_total_count_from_abap_to_filestore(self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, 'csv')
       
    def test_gen2_start_abap_cds_to_hana_initial_fat(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.cds-to-hana-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        self.run_graph_until_running(graph_full_name, 
                                     graph_run_name, 
                                     False, 
                                     snapshot_config={"enabled": True, "periodSeconds": 30})

    def test_gen2_validate_abap_cds_to_hana_initial_fat(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.cds-to-hana-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        hana_table = "GEN2_DHE2E_CDS_WF_LM_INITIAL"
        abap_table = "WF_LM"
        
        self.assert_execution_completed_by_graph_run_name(graph_run_name)
        # Some records cound be resented if some errors happened during execution
        # So target records in hana should be greater than or equal to source records in abap
        # Future, might get the count of resented records via SQL to get exact results. 
        src_count = self.get_conn_content_total_count(self.__abap_cds_conn, abap_table)
        target_count = self.get_row_count_from_hana(self.__hana_conn, hana_table)
        self.assertGreaterEqual(target_count, src_count, "target records in hana should be greater than or equal to source records in abap")
        
        #self.validate_total_count_from_abap_to_hana(self.__abap_cds_conn, abap_table, self.__hana_conn, hana_table)    
        
    def test_gen2_start_abap_cds_to_filestore_initial_fat(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.cds-to-filestore-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        file_directory = "/CET_TEST/cds-to-filestore-initial-fat-gen2"
        
        # remove file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        self.run_graph_until_running(graph_full_name, 
                                     graph_run_name,
                                     False, 
                                     snapshot_config={"enabled": True, "periodSeconds": 30})

    def test_gen2_validate_abap_cds_to_filestore_initial_fat(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.cds-to-filestore-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        file_directory = "/CET_TEST/cds-to-filestore-initial-fat-gen2"
        abap_table = "WF_LM"
        self.assert_execution_completed_by_graph_run_name(graph_run_name)
        # Some records cound be resented if some errors happened during execution
        # So target records in hana should be greater than or equal to source records in abap
        # Future, might get the count of resented records via SQL to get exact results. 
        src_count = self.get_conn_content_total_count(self.__abap_cds_conn, abap_table)
        target_count = self.get_adlv2_content_total_count(self.__adlv2_conn, file_directory, 'part','infer', False)
        self.assertGreaterEqual(target_count, src_count, "target records in file should be greater than or equal to source records in abap")
        
        # self.validate_total_count_from_abap_to_filestore(self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, 'part','infer', False)
     
    def test_gen2_start_abap_cds_to_kafka_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.cds-to-kafka-initial-delta"
        graph_for_check_full_name = "com.sap.abap-int-tools.g2-kafka-to-filestore"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen2")
        file_directory = "/CET_TEST/cds-to-kafka-initial-delta-gen2"
        configSubstitutions = {"path": "/CET_TEST/cds-to-kafka-initial-delta-gen2",
                            "topic": "topic-CET-cds-to-kafka-initial-delta-gen2"}
        
        # remove file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        self.run_graph_until_running(graph_for_check_full_name, 
                                     graph_for_check_run_name, 
                                     False,
                                     config_substitutions=configSubstitutions)
        self.run_graph_until_running(graph_full_name, 
                                     graph_run_name, 
                                     False, 
                                     snapshot_config={"enabled": True, "periodSeconds": 30})

    def test_gen2_validate_abap_cds_to_kafka_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.cds-to-kafka-initial-delta"
        graph_for_check_full_name = "com.sap.abap-int-tools.g2-kafka-to-filestore"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen2")
        # file_directory = "/CET_TEST/cds-to-kafka-initial-delta-gen2"
        # abap_table = "WN_LM"

        ''' known issue
        the ABAP operator takes too long to generate data, which ends up causing the kafka connection to be dropped after being idle for more than 30min. So the gragh is dead when get delta record. 
        https://jira.tools.sap/browse/DIBUGS-14521 
        So disable the delta operation in the case below

        graph_status = graph.getStatusByName(graph_run_name)
        self.assertEqual('running', graph_status, 'The status should be running')    
        graph_status = graph.getStatusByName(graph_for_check_run_name)
        self.assertEqual('running', graph_status, 'The status should be running')    
        
        # Insert 5 records for table ws_lm from abap dmis system
        s4_client = self.get_connection_client(self.__abap_cds_conn)
        s4_client.insert_records(abap_table, 5)
        
        sleep(30)
        self.validate_total_count_from_abap_to_filestore(
            self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, 'part', 'infer',False)
        '''
        self.assert_execution_running_by_graph_run_name(graph_run_name)
        self.assert_execution_running_by_graph_run_name(graph_for_check_run_name)
          
    def test_gen2_start_abap_cds_to_filestore_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.cds-to-filestore-intial-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        file_directory = "/CET_TEST/cds-to-filestore-initial-delta-gen2"
        
        # remove file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        self.run_graph_until_running(graph_full_name, 
                                     graph_run_name, 
                                     False, 
                                     snapshot_config={"enabled": True, "periodSeconds": 30})
        
    def test_gen2_validate_abap_cds_to_filestore_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.cds-to-filestore-intial-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        file_directory = "/CET_TEST/cds-to-filestore-initial-delta-gen2"
        abap_table = "WN_LM"
                
        self.assert_execution_running_by_graph_run_name(graph_run_name)
        # update dataset
        s4_client = self.get_connection_client(self.__abap_cds_conn)
        s4_client.insert_records(abap_table, 5)
       
        sleep(30)
        self.validate_total_count_from_abap_to_filestore(
             self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, 'part','infer', False)

    def test_gen2_start_abap_slt_to_filestore_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.slt-to-filestore-initial-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        file_directory = "/CET_TEST/slt-to-filestore-initial-delta-gen2"
        
        # remove file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        self.run_graph_until_running(graph_full_name, 
                                     graph_run_name, 
                                     False,
                                     snapshot_config={"enabled": True, "periodSeconds": 30})
       
    def test_gen2_validate_abap_slt_to_filestore_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.slt-to-filestore-initial-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        file_directory = "/CET_TEST/slt-to-filestore-initial-delta-gen2"
        abap_table = "WS_LM"
                
        self.assert_execution_running_by_graph_run_name(graph_run_name)
        # validate initial load
        self.validate_total_count_from_abap_to_filestore(
            self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, 'part', 'infer', False)
       
        # update dataset
        dmis_client = self.get_connection_client(self.__abap_slt_conn)
        dmis_client.insert_records(abap_table, 5)
        
        sleep(30)
        self.validate_total_count_from_abap_to_filestore(
            self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, 'part', 'infer', False)
    
    def test_gen2_start_abap_slt_to_kafka_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.slt-to-kafka-initial-delta"
        graph_for_check_full_name = "com.sap.abap-int-tools.g2-kafka-to-filestore"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen2")
        kafka_topic = "topic-slt-to-kafka-initial-delta-gen2"
        file_directory = "/CET_TEST/slt-to-kafka-initial-delta-gen2"
        #file_path = file_directory +"/result.csv"

        # delete the file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        # Start the kafka consumer pipeline first to check the data
        self.run_graph_until_running(graph_for_check_full_name, 
                                     graph_for_check_run_name, 
                                     False, 
                                     config_substitutions={"path": file_directory, "topic": kafka_topic})
        # Run the pipeline to transfer data from ABAP to KAFKA with delta load
        self.run_graph_until_running(graph_full_name, 
                                     graph_run_name, 
                                     False, 
                                     snapshot_config={"enabled": True, "periodSeconds": 30})
       
    def test_gen2_validate_abap_slt_to_kafka_initial_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.slt-to-kafka-initial-delta"
        graph_for_check_full_name = "com.sap.abap-int-tools.g2-kafka-to-filestore"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen2")
        abap_table = "WN_LM"
        #kafka_topic = "topic-slt-to-kafka-initial-delta-gen2"
        file_directory = "/CET_TEST/slt-to-kafka-initial-delta-gen2"
        #file_path = file_directory +"/result.csv"
                
        self.assert_execution_running_by_graph_run_name(graph_run_name)
        self.assert_execution_running_by_graph_run_name(graph_for_check_run_name)
                
        # validate initial load
        self.validate_total_count_from_abap_to_filestore(self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, 'part', 'infer', False)
        
        #Below tests delta load
        initial_row_count = self.get_conn_content_total_count(self.__abap_slt_conn, abap_table)
        
        sleep(60)

        dmis_client = self.get_connection_client(self.__abap_slt_conn)
        # insert 5 records in ABAP, then delete 5 records, and check the delta behavior
        dmis_client.insert_records(abap_table, 5)
        sleep(60)

        dmis_client.delete_records(abap_table, 5)
        sleep(60)

        self.validate_row_count_in_adlv2(initial_row_count+5+5, self.__adlv2_conn, file_directory, 'part', 'infer', False)
