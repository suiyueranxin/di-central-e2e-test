from utils.case_base import BaseTest
from di_qa_e2e.models.graph import GraphStatus

class DataFlowTest(BaseTest):

    def test_df_from_hana_on_prem_to_datasphere(self):
        df_name = "DF_HANAOP_TO_DATASPHERE"

        # run graph
        graph = self.safe_run_graph(df_name)
        graph.wait_until_completed()

        # source and target info
        source_connection_name = "IM_HANA_OnPrem"
        source_table = "S_ALL_TYPES_TB"
        target_connection_name = "DUMMY_Datasphere"
        target_table = "NZDM_DF_T_FROM_HANAOP_S_ALL_TYPES_TB"

        # check data
        src_count = self.get_table_total_count_from_data_server(source_connection_name, source_table)
        target_count = self.get_table_total_count_from_data_server(target_connection_name, target_table)
        self.validate_table_total_count_by_src_and_target(source_connection_name, source_table, target_connection_name, target_table, src_count, target_count)
    
    # Long running data flow, it will cross three upgrade stages. 
    # In pre-upgrade stage, we start the graph; in upgrade stage, we check the graph still running; in post-upgrade, we check data after graph completed.
    def test_df_from_s4h_cds_to_hana_cloud(self):
        df_name = "DF_S4H_CDS_to_HANAonCloud"

        # source and target info
        source_connection_name = "CIT_S4H_2021_ABAP"
        source_table = "DHE2E_CDS_WN_LX"
        src_abap_table_name = "WN_LX"
        target_connection_name = "CIT_HANA_Cloud"
        target_table = "NZDM_DF_T_FROM_S4H_DHE2E_CDS_WN_LX"

        if self.is_pre_upgrade:
            # run graph
            graph = self.safe_run_graph(df_name,auto_clean=False)
            graph.wait_until_running()

        if self.is_during_ugprade:
            self.validate_graph_status(df_name, GraphStatus.RUNNING)
            # check data during upgrade
            src_count = self.get_table_total_count_from_data_server(source_connection_name, src_abap_table_name)
            target_count = self.get_table_total_count_from_data_server(target_connection_name, target_table)
            self.validate_table_total_count_by_src_and_target_during_upgrade(source_connection_name, source_table, target_connection_name, target_table, src_count, target_count)
        
        if self.is_post_upgrade:
            graph = self.get_graph_execution_by_name(df_name)
            graph.wait_until_completed()
            # check data after upgrade
            src_count = self.get_table_total_count_from_data_server(source_connection_name, src_abap_table_name)
            target_count = self.get_table_total_count_from_data_server(target_connection_name, target_table)
            self.validate_table_total_count_by_src_and_target(source_connection_name, source_table, target_connection_name, target_table, src_count, target_count)

    def test_df_from_abap_slt_to_datasphere(self):
        df_name = "DF_ABAP_SLT_TO_DATASPHERE"

        # run graph
        graph = self.safe_run_graph(df_name)
        graph.wait_until_completed(5000)

        # source and target info
        source_connection_name = "CIT_DMIS_2018_SLT"
        source_table = "LTE2E_WS_LL"
        src_abap_table_name = "WS_LL"
        target_connection_name = "DUMMY_Datasphere"
        target_table = "NZDM_DF_T_FROM_DMIS_LTE2E_WS_LL"

        # check data
        src_count = self.get_table_total_count_from_data_server(source_connection_name, src_abap_table_name)
        target_count = self.get_table_total_count_from_data_server(target_connection_name, target_table)
        self.validate_table_total_count_by_src_and_target(source_connection_name, source_table, target_connection_name, target_table, src_count, target_count)