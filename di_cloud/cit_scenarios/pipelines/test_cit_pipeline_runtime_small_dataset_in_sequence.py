from time import sleep
from utils.case_pipeline import PipelineCase

class testE2E_Pipeline_Runtime(PipelineCase):
    __abap_cds_conn = "CIT_S4"
    __abap_slt_conn = "CIT_DMIS"
    __hana_conn = "CIT_HANA"
    __adlv2_conn = "CIT_FILE"

    def test_validate_abap_cds_to_hana_initial_small(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.cds-to-hana-initial-small"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        hana_table = "GEN1_DHE2E_CDS_WN_LS_INITIAL"
        abap_table = "WN_LS"
        subscription = "cds-to-hana-initial-small"

        self.abap_erase_subscription_id_by_name(self.__abap_cds_conn, subscription)

        # Store the subscription name, to clean them in test teardown
        
        self.push_abap_subscription_name(self.__abap_cds_conn, subscription)

        self.run_graph_until_completed(graph_full_name, graph_run_name)
        
        self.validate_total_count_from_abap_to_hana(self.__abap_cds_conn, abap_table, self.__hana_conn, hana_table)    

    def test_validate_abap_cds_to_filestore_initial_small(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.cds-to-filestore-initial-small"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        file_directory = "/CET_TEST/cds-to-filestore-initial-small"
        abap_table = "WN_LS"
        subscription = "cds-to-filestore-initial-small"

        self.abap_erase_subscription_id_by_name(self.__abap_cds_conn, subscription)

        # Store the subscription name, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_cds_conn, subscription)
        
        # remove file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        
        self.run_graph_until_completed(graph_full_name, graph_run_name, True)

        self.validate_total_count_from_abap_to_filestore(self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, '.json')
   
    def test_validate_abap_cds_to_kafka_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.cds-to-kafka-delta"
        graph_for_check_full_name = "com.sap.abap-int-tools.g1-kafka-to-filestore"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen1-cds")
        abap_table = "WN_LM"
        subscription = "cds-to-kafka-delta"
        kafka_topic = "topic-cds-to-kafka-delta"
        file_directory = "/CET_TEST/cds-to-kafka-delta"
        file_path = "/CET_TEST/cds-to-kafka-delta/result_file_<counter>.csv"

        self.abap_erase_subscription_id_by_name(self.__abap_cds_conn, subscription)

        self.push_abap_subscription_name(self.__abap_cds_conn, subscription)
        
        # delete the file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        # Start the kafka consumer pipeline first to check the data
        graph_execution_validation = self.run_graph_until_running(graph_for_check_full_name, 
                                                                  graph_for_check_run_name, 
                                                                  config_substitutions={"path": file_path, "topic": kafka_topic})
        # Run the pipeline to transfer data from ABAP to KAFKA with delta load
        graph_execution = self.run_graph_until_running(graph_full_name, graph_run_name)

        sleep(10)


        s4_client = self.get_connection_client(self.__abap_cds_conn)
        # change records in ABAP and check the delta behavior
        s4_client.delete_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_adlv2(5, self.__adlv2_conn, file_directory, 'csv', None)

        s4_client.insert_records(abap_table, 5)
        sleep(60)   
        self.validate_row_count_in_adlv2(5+5, self.__adlv2_conn, file_directory, 'csv', None)
        
        self.assert_is_running(graph_execution)
        self.assert_is_running(graph_execution_validation)
           
    def test_validate_abap_cds_to_filestore_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.cds-to-filestore-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        abap_table = "WN_LM"
        file_directory = "/CET_TEST/cds-to-filestore-delta"
        subscription = "cds-to-filestore-delta"

        self.abap_erase_subscription_id_by_name(self.__abap_cds_conn, subscription)
        
        self.push_abap_subscription_name(self.__abap_cds_conn, subscription)

        # delete the file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        # Run the pipeline to transfer data from ABAP to filestore with delta load
        self.run_graph_until_running(graph_full_name, graph_run_name)

        sleep(10)
        # Update 5 records for table dhe2e_wn_lm from abap s4 hana system
        s4_client = self.get_connection_client(self.__abap_cds_conn)
        # change records in ABAP and check the delta behavior
        s4_client.update_records(abap_table, 5)
        sleep(20)
        self.validate_row_count_in_adlv2(5, self.__adlv2_conn, file_directory, '.json')
        
    def test_validate_abap_slt_to_hana_initial_small(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.slt-to-hana-initial-small"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        hana_table = "GEN1_LTE2E_WN_LS_INITIAL"
        abap_table = "WN_LS"
        subscription = "slt-to-hana-initial-small"

        self.abap_erase_subscription_id_by_name(self.__abap_slt_conn, subscription)

        # Store the subscription name, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_slt_conn, subscription)

        self.run_graph_until_completed(graph_full_name, graph_run_name)

        self.validate_total_count_from_abap_to_hana(self.__abap_slt_conn, abap_table, self.__hana_conn, hana_table)    

    def test_validate_abap_slt_to_hana_initial_fat(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.slt-to-hana-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        hana_table = "GEN1_LTE2E_WF_LS_INITIAL"
        abap_table = "WF_LS"
        subscription = "slt-to-hana-initial-fat"

        self.abap_erase_subscription_id_by_name(self.__abap_slt_conn, subscription)

        # Store the subscription name to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_slt_conn, subscription)

        self.run_graph_until_completed(graph_full_name, graph_run_name)

        self.validate_total_count_from_abap_to_hana(self.__abap_slt_conn, abap_table, self.__hana_conn, hana_table)    

    def test_validate_abap_slt_to_filestore_initial_small(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.slt-to-filestore-initial-small"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        file_directory = "/CET_TEST/slt-to-filestore-initial-small"
        abap_table = "WS_LS"
        subscription = "slt-to-filestore-initial-small"
        
        self.abap_erase_subscription_id_by_name(self.__abap_slt_conn, subscription)

        # Store the subscription name, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_slt_conn, subscription)

        # remove file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        self.run_graph_until_completed(graph_full_name, graph_run_name)
        self.validate_total_count_from_abap_to_filestore(self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, '.json')

    def test_validate_abap_slt_to_kafka_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.slt-to-kafka-delta"
        graph_for_check_full_name = "com.sap.abap-int-tools.g1-kafka-to-filestore"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen1-slt")
        abap_table = "WS_LL"
        kafka_topic ="slt-to-kafka-delta"
        file_directory = "/CET_TEST/slt-to-kafka-delta"
        file_path = "/CET_TEST/slt-to-kafka-delta/result_file_<counter>.csv"
        subscription = "slt-to-kafka-delta"
        
        self.abap_erase_subscription_id_by_name(self.__abap_slt_conn, subscription)

        # Store the subscription name, to clean it in test teardown

        self.push_abap_subscription_name(self.__abap_slt_conn, subscription)

        # delete the file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)

        # Start the kafka consumer pipeline first to check the data
        graph_execution_validation = self.run_graph_until_running(
            graph_for_check_full_name,
            graph_for_check_run_name,
            config_substitutions={"path": file_path, "topic": kafka_topic})

        # Run the pipeline to transfer data from ABAP to KAFKA with delta load
        graph_execution = self.run_graph_until_running(graph_full_name, graph_run_name)

        sleep(10)
        
        # The validaiton is blocked by transcation issue. Comment this part until the issue is fixed.
        """  # Insert and delete 5 records for table LTE2E_WS_LL from abap dmis system """
        dmis_client = self.get_connection_client(self.__abap_slt_conn)
        dmis_client.insert_records(abap_table, 5)
        dmis_client.delete_records(abap_table, 5)
        # Validation starts here...
        sleep(60)
         
        self.validate_row_count_in_adlv2(5+5, self.__adlv2_conn, file_directory, '.csv', None)
        
        self.assert_is_running(graph_execution)
        self.assert_is_running(graph_execution_validation)
        
    def test_validate_abap_slt_to_filestore_delta(self):
        graph_full_name = "com.sap.abap-e2e-pip-g1.slt-to-filestore-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        abap_table = "WN_LL"
        file_directory = "/CET_TEST/slt-to-filtestore-delta"
        subscription = "slt-to-filestore-delta"

        self.abap_erase_subscription_id_by_name(self.__abap_slt_conn, subscription)
        
        # Store the subscription name, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_slt_conn, subscription)

        # delete the file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        # Run the pipeline to transfer data from ABAP to KAFKA with delta load
        self.run_graph_until_running(graph_full_name, graph_run_name)

        # The validaiton is blocked by transcation issue. Comment this part until the issue is fixed.
        """ # Insert and delete 5 records for table LTE2E_WN_LL from abap dmis system"""
         # Update 5 records for table dhe2e_wn_lm from abap s4 hana system
        dmis_client = self.get_connection_client(self.__abap_slt_conn)
        # change records in ABAP and check the delta behavior
        dmis_client.insert_records(abap_table, 5)
        dmis_client.delete_records(abap_table, 5)
        sleep(20)
        self.validate_row_count_in_adlv2(5+5, self.__adlv2_conn, file_directory, '.json')
        
    def test_validate_abap_cds_to_hana_initial_small_gen2(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.cds-to-hana-initial-small"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        hana_table = "GEN2_DHE2E_CDS_WN_LS_INITIAL"
        abap_table = "WN_LS"
        self.run_graph_until_completed(graph_full_name, graph_run_name, snapshot_config={"enabled": True, "periodSeconds": 30})
        self.validate_total_count_from_abap_to_hana(self.__abap_cds_conn, abap_table, self.__hana_conn, hana_table)
 
    def test_validate_abap_cds_to_filestore_initial_small_gen2(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.cds-to-filestore-initial-small"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        file_directory = "/CET_TEST/cds-to-filestore-initial-small-gen2"
        abap_table = "WN_LS"
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        self.run_graph_until_completed(graph_full_name, graph_run_name, snapshot_config={"enabled": True, "periodSeconds": 30})
        self.validate_total_count_from_abap_to_filestore(self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, 'part', 'infer', False)

    def test_validate_abap_cds_to_kafka_delta_gen2(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.cds-to-kafka-delta"
        graph_for_check_full_name = "com.sap.abap-int-tools.g2-kafka-to-filestore"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen2")
        abap_table = "WN_LM"
        kafka_topic = "topic-CET-cds-to-kafka-delta-gen2"
        file_directory = "/CET_TEST/cds-to-kafka-delta-gen2"

        # delete the file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        # Start the kafka consumer pipeline first to check the data
        graph_execution_validation = self.run_graph_until_running(
                        graph_for_check_full_name, 
                        graph_for_check_run_name,
                        config_substitutions={"path": file_directory, "topic": kafka_topic})

        # Run the pipeline to transfer data from ABAP to KAFKA with delta load
        graph_execution = self.run_graph_until_running(
                         graph_full_name, 
                         graph_run_name,
                         snapshot_config={"enabled": True, "periodSeconds": 30})

        sleep(20)
        s4_client = self.get_connection_client(self.__abap_cds_conn)
        # change records in ABAP and check the delta behavior
        s4_client.insert_records(abap_table, 5)
        s4_client.delete_records(abap_table, 5)
        s4_client.update_records(abap_table, 3)
        sleep(60)
        self.validate_row_count_in_adlv2(5+5+3, self.__adlv2_conn, file_directory, 'part', 'infer', False)

        self.assert_is_running(graph_execution)
        self.assert_is_running(graph_execution_validation)

    def test_validate_abap_cds_to_filestore_delta_gen2(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.cds-to-filestore-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        file_directory = "/CET_TEST/cds-to-filestore-delta-gen2"
        abap_table = "WN_LM"
        
        # delete the file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)

        # Start the graph
        graph_execution = self.run_graph_until_running(
                         graph_full_name, 
                         graph_run_name, 
                         snapshot_config={"enabled": True, "periodSeconds": 30})

        sleep(20)
        s4_client = self.get_connection_client(self.__abap_cds_conn)
        # change records in ABAP and check the delta behavior
        s4_client.insert_records(abap_table, 5)
        s4_client.delete_records(abap_table, 5)
        s4_client.update_records(abap_table, 3)
        sleep(30)
        self.validate_row_count_in_adlv2(5+5+3, self.__adlv2_conn, file_directory, 'part', 'infer', False)
        self.assert_is_running(graph_execution)
        
    def test_validate_abap_slt_to_filestore_initial_small_gen2(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.slt-to-filestore-initial-small"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        file_directory = "/CET_TEST/slt-to-filestore-initial-small-gen2"
        abap_table = "WN_LS"
        
        # delete the file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        self.run_graph_until_completed(graph_full_name, graph_run_name, snapshot_config={"enabled": True, "periodSeconds": 30})
        self.validate_total_count_from_abap_to_filestore(self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, 'part', 'infer', False)
        
    def test_validate_abap_slt_to_hana_initial_small_gen2(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.slt-to-hana-initial-small"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        hana_table = "GEN2_LTE2E_WN_LS_INITIAL"
        abap_table ="WN_LS"
        self.run_graph_until_completed(graph_full_name, graph_run_name, snapshot_config={"enabled": True, "periodSeconds": 30})
        self.validate_total_count_from_abap_to_hana(self.__abap_slt_conn, abap_table, self.__hana_conn, hana_table)
       
    def test_validate_abap_slt_to_filestore_delta_gen2(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.slt-to-filestore-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        file_directory = "/CET_TEST/slt-to-filestore-delta-gen2"
        abap_table = "WN_LM"
        
        # delete the file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)

        # Store the graph run name, to clean the graph in test teardown
        self.run_graph_until_running(graph_full_name, graph_run_name, snapshot_config={"enabled": True, "periodSeconds": 30})
               
        sleep(20)
        dmis_client = self.get_connection_client(self.__abap_slt_conn)
        # change records in ABAP and check the delta behavior
        dmis_client.insert_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_adlv2(5, self.__adlv2_conn, file_directory, 'part', 'infer', False)
        # Get AssertionError: 5 != XXX. The count of rows is not fixed, it raises as more insertion/deletion/update is done to table WN_lM.
        # Comment the assertion for now. Need to confirm what the correct behavior is.
        
    def test_validate_abap_slt_to_filestore_initial_fat_gen2(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.slt-to-filestore-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        file_directory = "/CET_TEST/slt-to-filestore-initial-fat-gen2"
        abap_table = "WF_LS"
        
        # delete the file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        self.run_graph_until_completed(graph_full_name, 
                                       graph_run_name,
                                       max_wait_time=1800,
                                       snapshot_config={"enabled": True, "periodSeconds": 30})
        self.validate_total_count_from_abap_to_filestore(self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, 'part', 'infer', False)
        
    def test_validate_abap_slt_to_hana_initial_fat_gen2(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.slt-to-hana-initial-fat"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        hana_table = "GEN2_LTE2E_WF_LS_INITIAL"
        abap_table ="WF_LS"
        
        # Store the graph run name, to clean the graph in test teardown
        self.run_graph_until_completed(graph_full_name, 
                                       graph_run_name,
                                       max_wait_time=1800,
                                       snapshot_config={"enabled": True, "periodSeconds": 30})
        self.validate_total_count_from_abap_to_hana(self.__abap_slt_conn, abap_table, self.__hana_conn, hana_table)
       
    def test_validate_abap_slt_to_kafka_delta_gen2(self):
        graph_full_name = "com.sap.abap-e2e-pip-g2.slt-to-kafka-delta"
        graph_for_check_full_name = "com.sap.abap-int-tools.g2-kafka-to-filestore"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen2")
        abap_table = "WF_LM"
        kafka_topic = "topic-slt-to-kafka-delta-gen2"
        file_directory = "/CET_TEST/slt-to-kafka-delta-gen2"
        
        # delete the file directory
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        # Start the kafka consumer pipeline first to check the data
        self.run_graph_until_running(graph_for_check_full_name, 
                                     graph_for_check_run_name,
                                     config_substitutions={"path": file_directory, "topic": kafka_topic})
        
        # Run the pipeline to transfer data from ABAP to KAFKA with delta load
        graph_execution = self.run_graph_until_running(graph_full_name, 
                                                       graph_run_name,
                                                       snapshot_config={"enabled": True, "periodSeconds": 30})

        sleep(20)
        dmis_client = self.get_connection_client(self.__abap_slt_conn)
        # update records in ABAP and check the delta behavior (note: the insert behavior for table WF_LM does not work, so only cover update)
        dmis_client.update_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_adlv2(5, self.__adlv2_conn, file_directory, 'part', 'infer', False)

        self.assert_is_running(graph_execution)
