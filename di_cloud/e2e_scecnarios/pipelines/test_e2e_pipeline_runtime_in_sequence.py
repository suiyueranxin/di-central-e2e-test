from time import sleep
# from tests.CET.e2e_scecnarios.pipelines.e2e_pipeline_base import E2E_Pipeline_base
from di_qa_e2e.models.graph import GraphStatus
from utils.case_pipeline import PipelineCase
from di_qa_e2e.models.graph import GraphStatus


class testE2E_Pipeline_Runtime(PipelineCase):
    __abap_cds_conn = "CIT_S4"
    __abap_slt_conn = "CIT_DMIS"
    __hana_conn = "CIT_HANA"
    __adlv2_conn = "CIT_FILE"

    def test_gen1_abap_cds_to_hana_initial(self):
        graph_full_name = "test.e2e_scenarios.abap.generation1.cds-to-hana-initial"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        hana_table = "GEN1_DHE2E_CDS_WN_LM_INITIAL"
        abap_table = "WN_LM"
        subscription = "GEN1_CITS4_TO_HC_INITIAL"
        # Store the graph execution and subscription, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_cds_conn, subscription)
        self.run_graph_until_completed(graph_full_name, 
                                       graph_run_name, 
                                       max_wait_time=1800, 
                                       config_substitutions={"RUN_ID": subscription})
        self.validate_total_count_from_abap_to_hana(self.__abap_cds_conn, abap_table, self.__hana_conn, hana_table)    

    def test_gen1_abap_cds_to_hana_delta(self):
        graph_full_name = "test.e2e_scenarios.abap.generation1.cds-to-hana-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        hana_table = "GEN1_DHE2E_CDS_WN_LM_DELTA"
        abap_table = "WN_LM"
        subscription = "GEN1_CITS4_TO_HC_DELTA"

        graph_execution = self.run_graph_until_running(graph_full_name, 
                                                       graph_run_name,
                                                       config_substitutions={"RUN_ID": subscription})

        # Store the subscription, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_cds_conn, subscription)
        sleep(10)

        s4_client = self.get_connection_client(self.__abap_cds_conn)
        # Insert 5 records in ABAP and check the delta behavior
        s4_client.insert_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, 5)
        
        # Update 5 records in ABAP and check the delta behavior
        s4_client.update_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, 10)

        # Delete 5 records in ABAP and check the delta behavior
        s4_client.delete_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, 15)
        self.assert_is_running(graph_execution)
        graph_execution.stop(True)
    
    def test_gen1_abap_cds_to_hana_replication(self):
        graph_full_name = "test.e2e_scenarios.abap.generation1.cds-to-hana-replication"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        hana_table = "GEN1_DHE2E_CDS_WN_LM_REPLICATION"
        abap_table = "WN_LM"
        subscription = "GEN1_CITS4_TO_HC_REPLICATION"
        # Initial load first and check the initial result
        graph_execution = self.run_graph_until_running(graph_full_name, 
                                                       graph_run_name, 
                                                       config_substitutions={"RUN_ID": subscription})
        # Store the subscription, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_cds_conn, subscription)

        sleep(1800)
        self.validate_total_count_from_abap_to_hana(self.__abap_cds_conn, abap_table, self.__hana_conn, hana_table)
        self.assert_is_running(graph_execution)
        #Below tests delta load
        initial_row_count = self.get_row_count_from_hana(self.__hana_conn, hana_table)
        
        s4_client = self.get_connection_client(self.__abap_cds_conn)
        # Insert 5 records in ABAP and check the delta behavior
        s4_client.insert_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, initial_row_count+5)
        
        # Update 5 records in ABAP and check the delta behavior, since HANA operator is set "Upsert", then the HANA table records' number is still the same
        s4_client.update_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, initial_row_count+5)

        # Delete 5 records in ABAP and check the delta behavior, since HANA operator is set "Upsert", then the HANA table records' number is still the same
        s4_client.delete_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, initial_row_count+5)  
      
    def test_gen1_abap_cds_to_kafka_initial(self):
        graph_full_name = "test.e2e_scenarios.abap.generation1.cds-to-kafka-initial"
        graph_for_check_full_name = "test.e2e_scenarios.abap.generation1.kafka-consumer-check-data"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen1_cds")
        abap_table = "WN_LM"
        subscription = "GEN1_CITS4_TO_KAFKA_INITIAL"
        file_directory = "/CET_TEST/KAFKA/gen1-cds-to-kafka-initial"
        file_path = file_directory + "/result_file_<counter>.csv"
        
        # Start the kafka consumer pipeline first to check the data
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)

        graph_execution_check = self.run_graph_until_running(graph_for_check_full_name, 
                                     graph_for_check_run_name, 
                                     config_substitutions={"RUN_ID": subscription, "path": file_path})
        
        # Run the pipeline to transfer data from ABAP to KAFKA with initial load
        self.run_graph_until_completed(graph_full_name, 
                                       graph_run_name, 
                                       max_wait_time=1800, 
                                       config_substitutions={"RUN_ID": subscription})

        # Store the subscription, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_cds_conn, subscription)

        self.assert_is_running(graph_execution_check)
        #Validate result
        self.validate_total_count_from_abap_to_filestore(self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, 'result_file')

    def test_gen1_abap_slt_to_hana_initial(self):
        graph_full_name = "test.e2e_scenarios.abap.generation1.slt-to-hana-initial"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        hana_table = "GEN1_LTE2E_WN_LM_INITIAL"
        abap_table = "WN_LM"
        subscription = "GEN1_CITDMIS_TO_HC_INITIAL"
        mt_id = "014"
        self.run_graph_until_completed(graph_full_name, 
                                       graph_run_name, 
                                       max_wait_time=1800, 
                                       config_substitutions={"MT_ID": mt_id, "RUN_ID": subscription})
        
        # Store the subscription, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_slt_conn, subscription)
        self.validate_total_count_from_abap_to_hana(self.__abap_slt_conn, abap_table, self.__hana_conn, hana_table)
   
    def test_gen1_abap_slt_to_hana_delta(self):
        graph_full_name = "test.e2e_scenarios.abap.generation1.slt-to-hana-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        hana_table = "GEN1_LTE2E_WN_LM_DELTA"
        abap_table = "WN_LM"
        subscription = "GEN1_CITDMIS_TO_HC_DELTA"
        mt_id = "014"
        
        graph_execution = self.run_graph_until_running(graph_full_name, graph_run_name, config_substitutions={"MT_ID": mt_id, "RUN_ID": subscription})
        # Store the subscription, to clean them in test teardown
    
        self.push_abap_subscription_name(self.__abap_slt_conn, subscription)
        
        sleep(10)

        dmis_client = self.get_connection_client(self.__abap_slt_conn)
        # Insert 5 records in ABAP and check the delta behavior
        dmis_client.insert_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, 5)
        
        # Update 5 records in ABAP and check the delta behavior
        dmis_client.update_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, 10)

        # Delete 5 records in ABAP and check the delta behavior
        dmis_client.delete_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, 15)
        self.assert_is_running(graph_execution)
        
    def test_gen1_abap_slt_to_hana_replication(self):
        graph_full_name = "test.e2e_scenarios.abap.generation1.slt-to-hana-replication"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        hana_table = "GEN1_LTE2E_WN_LM_REPLICATION"
        abap_table = "WN_LM"
        subscription = "GEN1_CITDMIS_TO_HC_REPLICATION"
        mt_id = "014"
        
        # Store the subscription, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_slt_conn, subscription)
        # Initial load first and check the initial result
        self.run_graph_until_running(graph_full_name, 
                                     graph_run_name, 
                                     config_substitutions={"MT_ID": mt_id, "RUN_ID": subscription})
        sleep(1800)
        
        self.validate_total_count_from_abap_to_hana(self.__abap_slt_conn, abap_table, self.__hana_conn, hana_table)

        #Below tests delta load
        initial_row_count = self.get_row_count_from_hana(self.__hana_conn, hana_table)
        
        dmis_client = self.get_connection_client(self.__abap_slt_conn)
        # Insert 5 records in ABAP and check the delta behavior
        dmis_client.insert_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, initial_row_count+5)
        
        # Update 5 records in ABAP and check the delta behavior, since HANA operator is set "Upsert", then the HANA table records' number is still the same
        dmis_client.update_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, initial_row_count+5)

        # Delete 5 records in ABAP and check the delta behavior, since HANA operator is set "Upsert", then the HANA table records' number is still the same
        dmis_client.delete_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, initial_row_count+5)


    def test_gen1_abap_slt_to_kafka_initial(self):
        graph_full_name = "test.e2e_scenarios.abap.generation1.slt-to-kafka-initial"
        graph_for_check_full_name = "test.e2e_scenarios.abap.generation1.kafka-consumer-check-data"
        graph_run_name = self.get_graph_name(graph_full_name, "gen1")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen1_slt")
        abap_table = "WN_LM"
        subscription = "GEN1_CITDMIS_TO_KAFKA_INITIAL"
        mt_id = "014"
        file_directory = "/CET_TEST/KAFKA/gen1-slt-to-kafka-initial"
        file_path = file_directory + "/result_file_<counter>.csv"
        
        # Start the kafka consumer pipeline first to check the data
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
        graph_execution_check = self.run_graph_until_running(graph_for_check_full_name, 
                                     graph_for_check_run_name, 
                                     config_substitutions={"RUN_ID": subscription, "path": file_path})
               
        # Run the pipeline to transfer data from ABAP to KAFKA with initial load
        self.run_graph_until_completed(graph_full_name, 
                                       graph_run_name,
                                       max_wait_time=1800,
                                       config_substitutions={"MT_ID": mt_id, "RUN_ID": subscription})
        # Store the graph executions and subscription, to clean them in test teardown
        self.push_abap_subscription_name(self.__abap_slt_conn, subscription)
        self.assert_is_running(graph_execution_check)
        #Validate result
        self.validate_total_count_from_abap_to_filestore(self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, 'result_file')

    def test_gen2_abap_cds_to_hana_initial(self):
        graph_full_name = "test.e2e_scenarios.abap.generation2.cds-to-hana-initial"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        hana_table = "GEN2_DHE2E_CDS_WN_LM_INITIAL"
        abap_table = "WN_LM"
        self.run_graph_until_completed(graph_full_name, 
                                       graph_run_name, 
                                       max_wait_time=1800, 
                                       snapshot_config={"enabled": True, "periodSeconds": 30})
        self.validate_total_count_from_abap_to_hana(self.__abap_cds_conn, abap_table, self.__hana_conn, hana_table)

    def test_gen2_abap_cds_to_hana_delta(self):
        graph_full_name = "test.e2e_scenarios.abap.generation2.cds-to-hana-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        hana_table = "GEN2_DHE2E_CDS_WN_LM_DELTA"
        abap_table = "WN_LM"
        
        graph_execution = self.run_graph_until_running(graph_full_name, 
                                                       graph_run_name, 
                                                       snapshot_config={"enabled": True, "periodSeconds": 30})
        sleep(10)
        
        s4_client = self.get_connection_client(self.__abap_cds_conn)
        # Insert 5 records in ABAP and check the delta behavior
        s4_client.insert_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, 5)
        
        # Update 5 records in ABAP and check the delta behavior
        s4_client.update_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, 10)
        
        # Delete 5 records in ABAP and check the delta behavior
        s4_client.delete_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, 15)

        self.assert_is_running(graph_execution)
        
    def test_gen2_abap_cds_to_hana_replication(self):
        graph_full_name = "test.e2e_scenarios.abap.generation2.cds-to-hana-replication"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        hana_table = "GEN2_DHE2E_CDS_WN_LM_REPLICATION"
        abap_table = "WN_LM"
      
        graph_execution = self.run_graph_until_running(graph_full_name, graph_run_name, snapshot_config={"enabled": True, "periodSeconds": 30})
        
        sleep(1200)
        
        self.validate_total_count_from_abap_to_hana(self.__abap_cds_conn, abap_table, self.__hana_conn, hana_table)
        
        self.assert_is_running(graph_execution)
        
        #Below tests delta load
        initial_row_count = self.get_row_count_from_hana(self.__hana_conn, hana_table)
        
        s4_client = self.get_connection_client(self.__abap_cds_conn)
        # Insert 5 records in ABAP and check the delta behavior
        s4_client.insert_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, initial_row_count+5)
        
        # Update 5 records in ABAP and check the delta behavior, since HANA operator is set "Upsert", then the HANA table records' number is still the same
        s4_client.update_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, initial_row_count+5)

        # Delete 5 records in ABAP and check the delta behavior, since HANA operator is set "Upsert", then the HANA table records' number is still the same
        s4_client.delete_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, initial_row_count+5)

    def test_gen2_abap_cds_to_kafka_initial(self):
        graph_full_name = "test.e2e_scenarios.abap.generation2.cds-to-kafka-initial"
        graph_for_check_full_name = "test.e2e_scenarios.abap.generation2.kafka-consumer-check-data"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen2_cds")
        abap_table = "WN_LM"
        kafka_topic = "topic-gen2-cds-to-kafka-initial"
        file_directory = "/CET_TEST/KAFKA/gen2-cds-to-kafka-initial"
        
        # Start the kafka consumer pipeline first to check the data
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)
     
        graph_execution_check = self.run_graph_until_running(graph_for_check_full_name, 
                                     graph_for_check_run_name, 
                                     config_substitutions={"path": file_directory, "topic": kafka_topic})
        
        # Run the pipeline to transfer data from ABAP to KAFKA with initial load
        self.run_graph_until_completed(graph_full_name,
                                       graph_run_name, 
                                       max_wait_time=1800,
                                       config_substitutions={"topic": kafka_topic}, 
                                       snapshot_config={"enabled": True, "periodSeconds": 30})
        sleep(600)
        self.assert_is_running(graph_execution_check)
        
        #Validate result
        self.validate_total_count_from_abap_to_filestore(self.__abap_cds_conn, abap_table, self.__adlv2_conn, file_directory, 'part', 'infer', False)

    def test_gen2_abap_slt_to_hana_initial(self):
        graph_full_name = "test.e2e_scenarios.abap.generation2.slt-to-hana-initial"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        hana_table = "GEN2_LTE2E_WN_LM_INITIAL"
        abap_table = "WN_LM"
        
        self.run_graph_until_completed(graph_full_name, 
                                       graph_run_name, 
                                       max_wait_time=1800,
                                       snapshot_config={"enabled": True, "periodSeconds": 30})
        self.validate_total_count_from_abap_to_hana(self.__abap_slt_conn, abap_table, self.__hana_conn, hana_table)

    def test_gen2_abap_slt_to_hana_delta(self):
        graph_full_name = "test.e2e_scenarios.abap.generation2.slt-to-hana-delta"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        hana_table = "GEN2_LTE2E_WN_LM_DELTA"
        abap_table = "WN_LM"

        graph_execution = self.run_graph_until_running(graph_full_name, 
                                                       graph_run_name, 
                                                       snapshot_config={"enabled": True, "periodSeconds": 30})

        sleep(10)
        dmis_client = self.get_connection_client(self.__abap_slt_conn)
        # Insert 5 records in ABAP and check the delta behavior
        dmis_client.insert_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, 5)
        
        # Update 5 records in ABAP and check the delta behavior
        dmis_client.update_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, 10)

        # Delete 5 records in ABAP and check the delta behavior
        dmis_client.delete_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, 15)
        self.assert_is_running(graph_execution)

    def test_gen2_abap_slt_to_hana_replication(self):
        graph_full_name = "test.e2e_scenarios.abap.generation2.slt-to-hana-replication"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        hana_table = "GEN2_LTE2E_WN_LM_REPLICATION"
        abap_table = "WN_LM"
        

        # Initial load first and check the initial result
        graph_execution = self.run_graph_until_running(graph_full_name, 
                                                       graph_run_name, 
                                                       snapshot_config={"enabled": True, "periodSeconds": 30})
        sleep(1200)
        
        self.validate_total_count_from_abap_to_hana(self.__abap_slt_conn, abap_table, self.__hana_conn, hana_table)
        self.assert_is_running(graph_execution)
        #Below tests delta load
        initial_row_count = self.get_row_count_from_hana(self.__hana_conn, hana_table)
        
        dmis_client = self.get_connection_client(self.__abap_slt_conn)
        # Insert 5 records in ABAP and check the delta behavior
        dmis_client.insert_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, initial_row_count+5)
        
        # Update 5 records in ABAP and check the delta behavior, since HANA operator is set "Upsert", then the HANA table records' number is still the same
        dmis_client.update_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, initial_row_count+5)

        # Delete 5 records in ABAP and check the delta behavior, since HANA operator is set "Upsert", then the HANA table records' number is still the same
        dmis_client.delete_records(abap_table, 5)
        sleep(60)
        self.validate_row_count_in_hana(self.__hana_conn, hana_table, initial_row_count+5)

    def test_gen2_abap_slt_to_kafka_initial(self):
        graph_full_name = "test.e2e_scenarios.abap.generation2.slt-to-kafka-initial"
        graph_for_check_full_name = "test.e2e_scenarios.abap.generation2.kafka-consumer-check-data"
        graph_run_name = self.get_graph_name(graph_full_name, "gen2")
        graph_for_check_run_name = self.get_graph_name(graph_for_check_full_name, "gen2_slt")        
        abap_table = "WN_LM"
        kafka_topic = "topic-gen2-slt-to-kafka-initial"
        file_directory = "/CET_TEST/KAFKA/gen2-slt-to-kafka-initial"
        
        # Start the kafka consumer pipeline first to check the data
        self.get_connection_client(self.__adlv2_conn).delete_directory(file_directory)

        graph_execution_check = self.run_graph_until_running(graph_for_check_full_name, 
                                     graph_for_check_run_name, 
                                     config_substitutions={"path": file_directory, "topic": kafka_topic})
        # Run the pipeline to transfer data from ABAP to KAFKA with initial load

        self.run_graph_until_completed(graph_full_name, 
                                       graph_run_name,
                                       max_wait_time=1800,
                                       config_substitutions={"topic": kafka_topic}, 
                                       snapshot_config={"enabled": True, "periodSeconds": 30})

        # Store the graph executions and subscription, to clean them in test teardown
 

        sleep(600)
        self.assert_is_running(graph_execution_check)
        #Validate result
        self.validate_total_count_from_abap_to_filestore(self.__abap_slt_conn, abap_table, self.__adlv2_conn, file_directory, 'part', 'infer', False)