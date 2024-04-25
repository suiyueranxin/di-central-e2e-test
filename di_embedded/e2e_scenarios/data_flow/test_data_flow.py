
from utils.case_base import BaseTest
from time import sleep
from unittest import skip

DEPRECATED_NOTE = "Deprecated as not support non-dwc target"
class DataFlowTest(BaseTest):
    
    
    __dwc_conn = "DWC"
    __s4_op_conn = "CIT_S4H_2021 (ABAP)"
    __hana_op_conn = "HANA_OnPrem"
    __hana_oc_conn = "CIT_HANA_Cloud"
    __hdl_db_conn = "HDL_DB"
    
    
    def test_df_from_hana_on_premise_to_local_repo_with_append_mode(self):
        df_name = 'DF_HANAOP_to_LocalRepo'
        src_table_name = 'S_ALL_TYPES_TB'
        target_table_name = 'DF_T_FROM_HANAOP_S_ALL_TYPES_TB'
        
        initial_target_row_count = self.get_table_total_count_from_dwc(table_name=target_table_name)

        # run data flow 
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)

        # fetch the count and verify append mode
        actual_target_row_count = self.get_table_total_count_from_dwc(table_name=target_table_name)
        expected_target_row_count = initial_target_row_count + self.get_table_total_count_from_data_server(self.__hana_op_conn, src_table_name) 
        self.validate_table_total_count(self.__dwc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)
    
    @skip(DEPRECATED_NOTE)
    def test_df_from_hana_on_premise_to_hana_on_cloud_with_truncate_mode(self):
        df_name = 'DF_HANAOP_to_HANAonCloud'
        target_table_name = 'DF_T_FROM_HANAOP_CUSTOMER_SALES'

        # run data flow 
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)

        # fetch the count and verify
        actual_target_row_count = self.get_table_total_count_from_data_server(self.__hana_oc_conn, target_table_name)
        expected_target_row_count = 18
        self.validate_table_total_count(self.__hana_oc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)
    
    @skip(DEPRECATED_NOTE)
    def test_df_from_s4hana_on_premise_to_hana_on_cloud_with_truncate_mode(self):
        df_name = 'DF_S4HANAOP_to_HANAonCloud'
        src_table_name = 'WN_LS'
        target_table_name = 'DF_T_FROM_S4HOP_DHE2E_CDS_WN_LS'

        # run data flow 
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)

        # fetch the count and verify
        actual_target_row_count = self.get_table_total_count_from_data_server(self.__hana_oc_conn, target_table_name)
        expected_target_row_count = self.get_table_total_count_from_data_server(self.__s4_op_conn, src_table_name)
        self.validate_table_total_count(self.__hana_oc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)

    def test_df_from_s4hana_on_premise_to_local_repo_with_append_mode(self):
        df_name = 'DF_S4HANAOP_to_LocalRepo'
        src_table_name = 'WS_LS'
        target_table_name = 'DF_T_LOCAL_FROM_DHE2E_WS_LS'
        
        # run data flow 
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)

        # fetch the count and verify
        # Even the mode is append, the isnert type is upsert, so the target records would be same or more than source records 
        actual_target_row_count = self.get_table_total_count_from_dwc(table_name=target_table_name)
        expected_target_row_count = self.get_table_total_count_from_data_server(self.__s4_op_conn, src_table_name)
        #self.validate_table_total_count(self.__dwc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)
        self.assertGreaterEqual(actual_target_row_count,expected_target_row_count)


    # @skip(DEPRECATED_INFO)
    def test_df_from_hdl_db_to_hana_on_cloud_with_overwrite_mode(self):
        df_name = "DF_HDL_DB_to_HANAonCloud"
        src_table_name = "S_ALL_TYPES_TB"
        target_table_name = "DF_T_FROM_HDL_DB_S_ALL_TYPES_TB"
        
        # run data flow 
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)

        # fetch the count and verify
        actual_target_row_count = self.get_table_total_count_from_data_server(self.__hana_oc_conn, target_table_name)
        expected_target_row_count = self.get_table_total_count_from_data_server(self.__hdl_db_conn, src_table_name) 
        self.validate_table_total_count(self.__hana_oc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)
    
    @skip(DEPRECATED_NOTE)
    def test_df_from_hdl_files_to_hana_cloud_with_delete_mode(self):
        df_name = "DF_HDL_Files_to_HANAonCloud"
        target_table_name = "DF_T_FROM_HDL_FILES_S_ALL_TYPES_TB"
        
        # run data flow 
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)

        # fetch the count and verify
        actual_target_row_count = self.get_table_total_count_from_data_server(table_name=target_table_name)
        expected_target_row_count = 2
        self.validate_table_total_count(self.__hana_oc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)
    
    @skip(DEPRECATED_NOTE)
    def test_df_from_local_repo_to_hana_on_cloud_with_truncate_mode(self):
        df_name = 'DF_LocalRepo_to_HANAonCloud'
        target_table_name = 'DF_T_FROM_DWC_CORP1_INVOICE_PRODUCT'

        # run data flow 
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)

        # fetch the count and verify
        actual_target_row_count = self.get_table_total_count_from_data_server(self.__hana_oc_conn, target_table_name)
        expected_target_row_count = 80
        self.validate_table_total_count(self.__hana_oc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)
    
    def test_df_from_local_repo_to_local_repo_with_truncate_mode(self):
        df_name = 'DF_LocalRepo_to_LocalRepo'
        target_table_name = 'DF_T_From_Corp1_Invoice_Product'

        # run data flow 
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)

        # fetch the count and verify
        actual_target_row_count = self.get_table_total_count_from_dwc(table_name=target_table_name)
        expected_target_row_count = 90
        self.validate_table_total_count(self.__dwc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)

    def test_df_from_local_repo_to_local_repo_Aggregation_with_truncate_mode(self):
        df_name = 'DF_LocalRepo_to_LocalRepo_Aggregation'
        target_table_name = 'DF_T_FROM_DWC_S_CORP1_INVOICE_AGGREGATION'

        # run data flow 
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)

        # fetch the count and verify
        actual_target_row_count = self.get_table_total_count_from_dwc(table_name=target_table_name)
        expected_target_row_count = 8
        self.validate_table_total_count(self.__dwc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)

    @skip(DEPRECATED_NOTE)
    def test_df_from_hana_on_cloud_to_hana_on_cloud_using_view_with_overwrite_mode(self):
        df_name = 'DF_View_HANAonCloud_to_HANAonCloud'
        target_table_name = 'DF_T_FROM_HANACLOUD_VIEW_S_CORP1_INVOICE_PRODUCT'

        # run data flow 
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)

        # fetch the count and verify
        actual_target_row_count = self.get_table_total_count_from_data_server(self.__hana_oc_conn, target_table_name)
        expected_target_row_count = 90
        self.validate_table_total_count(self.__hana_oc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)

    @skip(DEPRECATED_NOTE)
    def test_df_from_hana_on_cloud_to_hana_on_cloud_with_truncate_mode(self):
        df_name = 'DF_HANAonCloud_to_HANAonCloud'
        # src_table_name = "S_ALL_TYPES_TB"
        target_table_name = 'DF_T_FROM_HANACLOUD_S_ALL_TYPES_TB'

        # run data flow
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)

        # fetch the count and verify
        actual_target_row_count = self.get_table_total_count_from_data_server(self.__hana_oc_conn, target_table_name)
        expected_target_row_count = 2 # the expected count is 2 due to the filter
        self.validate_table_total_count(self.__hana_oc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)

    def test_df_from_hana_on_cloud_to_local_repo_with_truncate_mode(self):
        df_name = 'DF_HANAonCloud_to_LocalRepo'
        # src_table_name = "S_ALL_TYPES_TB"
        target_table_name = 'DF_T_FROM_HANACLOUD_S_ALL_TYPES_TB'

        # run data flow
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)

        # fetch the count and verify
        actual_target_row_count = self.get_table_total_count_from_dwc(table_name=target_table_name)
        expected_target_row_count = 3 # the expected count is 3 due to the filter
        self.validate_table_total_count(self.__dwc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)
        
    def test_df_from_s4hana_on_cloud_to_local_repo_with_append_mode(self):
        df_name = 'DF_S4HANAonCloud_to_LocalRepo'
        target_table_name = 'DF_T_FROM_S4HCC8_BUSEVTL'
        
        # run data flow 
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete(1200)
        sleep(10)

        # fetch the count and verify
        # Even the mode is append, the isnert type is upsert, so the target records would be same or more than source records 
        actual_target_row_count = self.get_table_total_count_from_dwc(table_name=target_table_name)
        expected_target_row_count = 100000
        self.assertGreaterEqual(actual_target_row_count,expected_target_row_count)

    @skip(DEPRECATED_NOTE)    
    def test_df_from_s4hana_on_cloud_to_hana_on_cloud_with_append_mode(self):
        df_name = 'DF_S4HANAonCloud_to_HANAonCloud2'
        src_table_name = 'WA_LS'
        target_table_name = 'DF_T_FROM_HANACLOUD_WA_LS'
        initial_target_row_count = self.get_table_total_count_from_data_server(self.__hana_oc_conn, target_table_name)    
          
        # run data flow 
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        sleep(10)
        
        # fetch the count and verify
        # Even the mode is append, the isnert type is upsert, so the target records would be same or more than source records 
        actual_target_row_count = self.get_table_total_count_from_data_server(self.__hana_oc_conn, target_table_name)
        expected_target_row_count = initial_target_row_count + 90
        self.validate_table_total_count(self.__hana_oc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)
