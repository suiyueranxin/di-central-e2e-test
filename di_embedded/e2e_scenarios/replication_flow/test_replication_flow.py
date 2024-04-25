
from utils.case_base import BaseTest
from time import sleep
import os

class ReplicationFlowTest(BaseTest):
    
    __dwc_conn = "DWC"
    __s4_op_conn = "CIT_S4H_2021 (ABAP)"
    __hdl_files_conn = "HDL_FILES"
    __dmis_2018_slt_conn = "CIT_DMIS_2018_SLT (ABAP)"
    __dmis_2018_odp_conn = "CIT_DMIS_2018_ODP (ABAP)"
    __hana_op_conn = "HANA_OnPrem"
    __hana_oc_conn = "CIT_HANA_Cloud"
    __adl_v2 = "IM_ADL_V2"
    __s3 = "IM_S3"
    __gcs = "IM_GCS"
    __gbq_conn = "IM_GBQ"
    
    
    # RF+DF
    def test_start_rf_from_abap_cds_to_hdl_files_delta_then_df_to_local_repo(self):
        rf_name = "RF_S4HOP_CDS_TO_HDL_FILES_DELTA"
        src_abap_table_name = "WN_LS"
        
        # run replication flow 
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        
        # Insert, update and delete 5 records in ABAP and check the delta behavior
        s4_client = self.get_connection_session(self.__s4_op_conn)
        sleep(60) # wait for the initial load done
        
        s4_client.insert_records(src_abap_table_name, 3)
        sleep(30)
        s4_client.update_records(src_abap_table_name, 1)
        sleep(30)
        s4_client.delete_records(src_abap_table_name, 1)
        sleep(30)

    # RF+DF
    def test_validate_rf_from_abap_cds_to_hdl_files_delta_then_df_to_local_repo(self):
        # store replicaiton flow names and clean them in tear down
        rf_name = "RF_S4HOP_CDS_TO_HDL_FILES_DELTA"
        self.push_replication_flow(rf_name)
        # fetch count and verify rf
        abap_cds_remote_table_name = "RT_DHE2E_WN_LS_FOR_VALIDATION"
        hdl_files_folder = "CET_DWC_TEST/RF_T_FROM_S4HOP_DHE2E_CDS_WN_LS"
        
        # abap source with filter, use remote table to validate data
        abap_cds_count = self.get_table_total_count_from_dwc(table_name=abap_cds_remote_table_name, is_remote=True)
        hdl_files_initial_row_count = self.get_non_table_total_count_from_data_server(self.__hdl_files_conn, f"{hdl_files_folder}/initial")
        hdl_files_delta_row_count = self.get_non_table_total_count_from_data_server(self.__hdl_files_conn, f"{hdl_files_folder}/delta")
        # insert 3, upsert 1, delete 1
        # verify initial folder
        self.validate_table_total_count(self.__hdl_files_conn, hdl_files_folder, hdl_files_initial_row_count, abap_cds_count-2)
        # verify delta folder
        self.validate_table_total_count(self.__hdl_files_conn, hdl_files_folder, hdl_files_delta_row_count, 5)
        
        df_name = "DF_HDL_FILES_TO_DWC"
        target_local_table_name = "DF_T_FROM_HDL_FILES_DHE2E_CDS_WN_LS"
        
        # run data flow
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        
        # fetch count and verify df
        target_row_count = self.get_table_total_count_from_dwc(table_name=target_local_table_name)
        hdl_files_total_row_count = hdl_files_initial_row_count+hdl_files_delta_row_count
        self.validate_table_total_count(self.__dwc_conn, target_local_table_name, target_row_count, hdl_files_total_row_count)

    def test_rf_from_abap_slt_to_local_repo_initial(self):
        rf_name = "RF_ABAP_SLT_to_DWC_INITIAL"
        target_table_name_1 = "RF_T_FROM_DMIS_LTE2E_WS_LS_INITIAL"
        target_table_name_2 = "RF_T_FROM_DMIS_LTE2E_WN_LS_INITIAL"
        target_table_name_3 = "RF_T_FROM_DMIS_LTE2E_WF_LS_INITIAL"
        remote_table_name_1 = "RT_LTE2E_WS_LS_FOR_VALIDATION"
        remote_table_name_2 = "RT_LTE2E_WN_LS_FOR_VALIDATION"
        src_abap_table_name_3 = "WF_LS"
        
        # run replication flow 
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_completed()
        
        # fetch count and verify
        # Since the task for WS_LS has filter, use remote table to validate the data 
        target_row_count_1 = self.get_table_total_count_from_dwc(table_name=target_table_name_1)
        remote_table_row_count_1 = self.get_table_total_count_from_dwc(table_name=remote_table_name_1, is_remote=True)
        self.validate_table_total_count(self.__dwc_conn, target_row_count_1, actual_count=target_row_count_1, expected_count=remote_table_row_count_1)
        # Since the task for WN_LS has filter, use remote table to validate the data 
        target_row_count_2 = self.get_table_total_count_from_dwc(table_name=target_table_name_2)
        remote_table_row_count_2 = self.get_table_total_count_from_dwc(table_name=remote_table_name_2, is_remote=True)
        self.validate_table_total_count(self.__dwc_conn, target_row_count_2, actual_count=target_row_count_2, expected_count=remote_table_row_count_2)
        # Since the task for WF_LS has no filter, use original abap table to validate the data 
        target_row_count_3 = self.get_table_total_count_from_dwc(table_name=target_table_name_3)
        source_row_count_3 = self.get_table_total_count_from_data_server(self.__dmis_2018_slt_conn, src_abap_table_name_3)
        self.validate_table_total_count(self.__dwc_conn, target_table_name_3, actual_count=target_row_count_3, expected_count=source_row_count_3)

    def test_start_rf_from_abap_slt_to_local_repo_delta(self):
        rf_name = "RF_ABAP_SLT_to_DWC_DELTA"
        src_table_name = "WN_LM"

        # run replication flow
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        sleep(60) # wait for the initial load done

        # do delta scenarios:
        dmis_client = self.get_connection_session(self.__dmis_2018_slt_conn)

        dmis_client.insert_records(src_table_name, 3)
        sleep(10)
        dmis_client.update_records(src_table_name, 1)
        sleep(10)
        dmis_client.delete_records(src_table_name, 2)

    def test_validate_rf_from_abap_slt_to_local_repo_delta(self):
        src_table_name = "WN_LM"
        target_table_name = "RF_T_FROM_DMIS_LTE2E_WN_LM_DELTA"

        target_row_count = self.get_table_total_count_from_dwc(table_name=target_table_name)
        expected_count = self.get_table_total_count_from_data_server(conn_name=self.__dmis_2018_slt_conn, table_name=src_table_name)
        self.validate_table_total_count(self.__dwc_conn, target_table_name, target_row_count, expected_count)

    def test_rf_from_hana_on_cloud_to_hana_on_prem_initial(self):
        src_schema = "CET_DWC_TEST"
        rf_name = "RF_HANAonCloud_to_HANAOP_INITIAL"
        src_table_names = ["S_BASIC_TYPES_TB","S_CHARS_TB","S_FLOATS_TB"]
        target_table_names =["RF_T_FROM_HANACLOUD_S_BASIC_TYPES_TB","RF_T_FROM_HANACLOUD_S_CHARS_TB","RF_T_FROM_HANACLOUD_S_FLOATS_TB"]
        filters = ["INT_C1 > 3","INT_C1 <= 3", "(INT_C1 BETWEEN 2 AND 10) AND  REAL_C3 > 0"]

        hana_client = self.get_connection_session(self.__hana_oc_conn)

        # run replication flow
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_completed()

        for i in range(len(target_table_names)):
            target_row_count = self.get_table_total_count_from_data_server(self.__hana_op_conn, target_table_names[i])
            expected_count = len(hana_client.get_dataset(src_schema, src_table_names[i], filters[i]))
            self.validate_table_total_count(self.__hana_op_conn, target_table_names[i], target_row_count, expected_count)

    def test_start_rf_from_s4h_odp_to_hana_on_cloud_delta(self):
        rf_name = "RF_ODPS4H_To_HanaCloud_DELTA"
        src_table_name = "WN_LS"

        # run replication flow
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        sleep(60) # wait for the initial load done

        # do delta scenarios:
        s4_client = self.get_connection_session(self.__s4_op_conn)

        s4_client.insert_records(src_table_name, 2)
        sleep(90)
        s4_client.update_records(src_table_name, 5)
        sleep(90)
        # ODP doesn't support delete

    def test_validate_rf_from_s4h_odp_to_hana_on_cloud_delta(self):
        src_table_name = "WN_LS"
        target_table_name = "RF_T_FROM_ODP_DHE2E_WN_LS"

        target_row_count = self.get_table_total_count_from_data_server(conn_name=self.__hana_oc_conn, table_name=target_table_name)
        expected_count = self.get_table_total_count_from_data_server(conn_name=self.__s4_op_conn, table_name=src_table_name)
        self.validate_table_total_count(self.__hana_oc_conn, target_table_name, target_row_count, expected_count)

    def test_start_rf_from_dmis_odp_to_local_repo_delta(self):
        rf_name = "RF_ODP_To_DWC"
        src_table_name = "WN_LS"

        # run replication flow
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        sleep(60) # wait for the initial load done

        # do delta scenarios:
        s4_client = self.get_connection_session(self.__dmis_2018_odp_conn)

        s4_client.insert_records(src_table_name, 3)
        sleep(90)
        s4_client.update_records(src_table_name, 6)
        sleep(90)
        # ODP doesn't support delete

    def test_validate_rf_from_dmis_odp_to_local_repo_delta(self):
        src_table_name = "WN_LS"
        target_table_name = "RF_T_FROM_ODP_LTE2E_WN_LS"

        target_row_count = self.get_table_total_count_from_dwc(table_name=target_table_name)
        expected_count = self.get_table_total_count_from_data_server(conn_name=self.__dmis_2018_odp_conn, table_name=src_table_name)
        self.validate_table_total_count(self.__dwc_conn, target_table_name, target_row_count, expected_count)

    def test_start_rf_from_hana_on_premise_to_hana_on_cloud_delta(self):
        src_schema = "CET_DWC_TEST"
        rf_name = "RF_HANAOP_to_HANAonCloud_DELTA"
        src_table_names = ["S_ALL_TYPES_TB","S_FLOATS_TB","S_DATETIMES_TB","S_NUMBERS_TB","S_CHARS_TB"]
        columns_list = [["INT_C1", "BIGINT_C2", "SMALLINT_C3", "TINYINT_C4", "FLOAT_C5", "REAL_C6", "DOUBLE_C7", "DECIMAL_C8", "DECIMAL_C9", "DATE_C10", "TIME_C11", "SECONDDATE_C12", "TIMESTAMP_C13", "VARCHAR_C14", "NVARCHAR_C15", "ALPHANUM_C16", "VARBINARY_C17", "CLOB_C18", "NCLOB_C19", "BLOB_C20"],
                        ["INT_C1", "FLOAT_C2", "REAL_C3", "DOUBLE_C4", "DECIMAL_C5"],
                        ["INT_C1", "DATE_C2", "TIME_C3", "SECONDDATE_C4", "TIMESTAMP_C5"],
                        ["INT_C1", "BIGINT_C2", "SMALLINT_C3", "TINYINT_C4", "DECIMAL_C5"],
                        ["INT_C1", "VARCHAR_C2", "NVARCHAR_C3", "ALPHANUM_C4"]]
        insert_list = [[(1006, 1, 1, 1, 1, 1, 1, 1, 1, '20150114','19:59:00', '1999-12-31 00:00:00', '1999-12-31 23:59:59.999999','char', 'varchar', 'unichar123', '123456', 'text', 'unitext', '123456'),(1007, -9223372036854775808, -32768, 0, -1E29+1, -1E17+1, -3.40E38, -3.40E38,-3.40E18, '00010101','12:00:00 AM', '0001/01/01 12:00:00', '0001/01/01 12:00:00.0000000','var char', 'n  varchar','alphanum 123', 'ffffff', 'clob ','nclob', '1234'),(-1006, 1, 1, 1, 1, 1, 1, 1, 1, '20150114','19:59:00', '1999-12-31 00:00:00', '1999-12-31 23:59:59.999999','char', 'varchar', 'unichar123', '123456', 'text', 'unitext', '123456')],
                       [(1021, 1.00, 1.00, 1.00, 1.00), (1022, 0, 0.00, 0.000, 0.0000), (1023, -1, -1, -1, -1),(-1020, -1, -1, -1, -1),(-1021, 0, 0, 0, 0)],
                       [(1014, '20340101', '12:00:00.000 AM', '2033-01-01 12:00:00', '0001-01-01 12:00:00.000001'),(1015, '99991231', '23:59:59.999', '9999/12/31 23:59:59', '9999/12/31 23:59:59.9999999'),(1016, '19991231', '23:59:59.999', '1999-12-31 23:59:59', '1999-12-31 23:59:59.9999999'),],
                       [(1012, 0, 0, 0, 0), (1013, -1, -1, 0, -1), (-1014, 2, 3, 4, 5.00123)],
                       [(1011, 'gggg', 'ggggg', 'gggggg'),(1012, 'varchar column', 'nvarchar column', 'alphanum column'),(-1011, 'gggg', 'ggggg', 'gggggg')]]
        update_list = [[({"BIGINT_C2": 100000, "SMALLINT_C3" : 1000, "TINYINT_C4" : 254}, "INT_C1 = 1006"),({"SECONDDATE_C12": '2015-09-28 01:01:01'}, "INT_C1 > 1000")],
                       [({"FLOAT_C2" : -0.01 },"INT_C1 < 0"),({"REAL_C3" : 0.02}, "INT_C1 > 1000")],
                       [({"TIMESTAMP_C5": '2014-12-31 12:00:00', "SECONDDATE_C4" : '2078-01-01 00:00:01'}, "INT_C1 >1000")],
                       [({"BIGINT_C2" : 100000 },"INT_C1 < 0"),({"DECIMAL_C5" : 1.000001}, "INT_C1 BETWEEN 1000 AND 2000")],
                       [({"VARCHAR_C2" : 'D' },"INT_C1 = 1012"),({"NVARCHAR_C3" : 'ccc'}, "VARCHAR_C2 LIKE 'gg%' ")]]
        delete_list = [["INT_C1 < 0 "],
                       ["INT_C1 > 1000"],
                       ["INT_C1 < 0 "],
                       ["INT_C1 > 1000 "],
                       ["INT_C1 < 0 "]]

        hana_client = self.get_connection_session(self.__hana_op_conn)

        recover_conition = "INT_C1 < 0 OR INT_C1 > 1000"
        for i in range(len(src_table_names)):
            hana_client.delete_rows(src_schema, src_table_names[i], recover_conition)

        #run replication flow
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        sleep(60) # wait for the initial load done

        # DO DELTA CHANGE
        for i in range(len(src_table_names)):
            for insert_tuple in insert_list[i]:
                hana_client.insert_rows(src_schema, src_table_names[i], columns_list[i], insert_tuple)
            sleep(10)
            for update_tuple in update_list[i]:
                hana_client.update_rows(src_schema, src_table_names[i], update_tuple[0], update_tuple[1])
            sleep(10)
            for delete_condition in delete_list[i]:
                hana_client.delete_rows(src_schema, src_table_names[i], delete_condition)
            sleep(10)

    def test_validate_rf_from_hana_on_premise_to_hana_on_cloud_delta(self):
        src_schema = "CET_DWC_TEST"
        src_table_names = ["S_ALL_TYPES_TB","S_FLOATS_TB","S_DATETIMES_TB","S_NUMBERS_TB","S_CHARS_TB"]
        target_table_names = ["RF_T_FROM_HANAOP_S_ALL_TYPES_TB","RF_T_FROM_HANAOP_S_FLOATS_TB","RF_T_FROM_HANAOP_S_DATETIMES_TB","RF_T_FROM_HANAOP_S_NUMBERS_TB","RF_T_FROM_HANAOP_S_CHARS_TB"]
        filters = ["INT_C1 >= 4","INT_C1 <= 4", "DATE_C2 >= '2023-02-01'","INT_C1 > 1", None]

        hana_client = self.get_connection_session(self.__hana_op_conn)

        for i in range(len(src_table_names)):
            target_row_count = self.get_table_total_count_from_data_server(conn_name=self.__hana_oc_conn, table_name=target_table_names[i])
            if filters[i] != None:
                expected_count = len(hana_client.get_dataset(src_schema, src_table_names[i], filters[i]))
            else:
                expected_count = self.get_table_total_count_from_data_server(conn_name=self.__hana_op_conn, table_name=src_table_names[i])
            self.validate_table_total_count(self.__hana_oc_conn, target_table_names[i], target_row_count, expected_count)

    # RF+DF
    def test_rf_from_S4HANAOP_to_hana_on_cloud_initial_then_df_to_local_repo(self):
        rf_name = "RF_S4HOP_HANAonCloud_initial_small"
        
        # run replication flow 
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        sleep(20)
        # store replicaiton flow names and clean them in tear down
        src_table_name = "WS_LS"
        target_table_name = 'RF_T_S4H_DHE2E_CDS_WS_LS'
        rf_name = "RF_S4HOP_HANAonCloud_initial_small"
        self.push_replication_flow(rf_name)
        
        # fetch count and verify rf
        actual_target_row_count = self.get_table_total_count_from_data_server(self.__hana_oc_conn, target_table_name)
        expected_target_row_count = self.get_table_total_count_from_data_server(conn_name=self.__s4_op_conn, table_name=src_table_name)
        self.validate_table_total_count(self.__hana_oc_conn, target_table_name, actual_count=actual_target_row_count, expected_count=expected_target_row_count)

        df_name = "DF_HANAonCloud_DWC_initial_small"
        target_local_table_name = "DF_T_FROM_HANAOC_WS_LS"
        
        # run data flow
        data_flow_execution = self.safe_run_data_flow(df_name)
        data_flow_execution.wait_until_complete()
        
        # fetch the count and verify
        actual_target_row_count2 = self.get_table_total_count_from_dwc(table_name=target_local_table_name)
        expected_target_row_count2 = self.get_table_total_count_from_data_server(self.__hana_oc_conn, target_table_name)
        self.validate_table_total_count(self.__dwc_conn, target_local_table_name, actual_count=actual_target_row_count2, expected_count=expected_target_row_count2)
        
    def test_rf_from_S4HCLOUDCC8_to_local_repo_initial(self):
        rf_name = "RF_S4HCLOUDCC8_to_DWC_INITIAL"
        
        # run replication flow 
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running(900)
        
        # store replicaiton flow names and clean them in tear down
        target_table_name = 'RF_CC8_to_DWCC_BUSEVTLOGEVENTDEX_2'
        rf_name = "RF_S4HCLOUDCC8_to_DWC_INITIAL"
        self.push_replication_flow(rf_name)
        
        # fetch count and verify rf
        actual_target_row_count = self.get_table_total_count_from_dwc(table_name=target_table_name)
        expected_target_row_count = 100000
        self.assertGreaterEqual(actual_target_row_count,expected_target_row_count)

    def test_start_rf_from_hana_on_cloud_to_local_repo_via_delta_enabled_local_table_delta(self):
        rf_name = "RF_HANAOC_to_DWC_DELTA_via_Delta_Local_Table"
        src_schema = "CET_DWC_TEST"
        src_table_name = "S_CORP1_INVOICE"
        insert_values = ('INV999999','CS200130 Buy P100110','CS200130','Hudson Technologies','US','IN','20200714',45,'20200828','20200715',-45,'In Due','USD',6.58,'Indirect','P100110','Lenovo T450 Touch i5 2x8GB 240GB SSD','191 - PC',9580,14,1.18,1717.99,24051.91,158261.6)
        delete_condition = "\"Invoice\" = 'INV999999'"

        # run replication flow
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        sleep(60) # wait for the initial load done

        # do delta change
        hana_client = self.get_connection_session(self.__hana_oc_conn)

        hana_client.insert_rows_all_columns(src_schema, src_table_name, insert_values)
        sleep(90)
        hana_client.delete_rows(src_schema, src_table_name, delete_condition)
        sleep(90)

    def test_validate_rf_from_hana_on_cloud_to_local_repo_via_delta_enabled_local_table_delta(self):
        src_table_name = "S_CORP1_INVOICE"
        target_table_name = "RF_T_FROM_HANA_OC_S_CORP1_INVOICE_DELTA"

        target_row_count = self.get_table_total_count_from_dwc(table_name=target_table_name)
        # Due to transfer via delta-enabled local table (delta-captured), the action of insert and delete will be captured and 2 more rows are transferred
        expected_count = self.get_table_total_count_from_data_server(self.__hana_oc_conn, src_table_name) + 1 
        self.validate_table_total_count(self.__dwc_conn, target_table_name, target_row_count, expected_count)

    def test_start_rf_from_odp_to_adl_v2_delta(self):
        rf_name = "RF_DMIS_ODP_TO_ADL_V2_DELTA"
        src_table_name = "WF_LS"

        # run replication flow
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        sleep(60) # wait for the initial load done

        # do delta scenarios:
        odp_client = self.get_connection_session(self.__dmis_2018_odp_conn)

        odp_client.insert_records(src_table_name, 3)
        sleep(10)
        odp_client.update_records(src_table_name, 1)
        sleep(10)
        # ODP doesn't support delete

    def test_validate_rf_from_odp_to_adl_v2_delta(self):
        rf_name = "RF_DMIS_ODP_TO_ADL_V2_DELTA"
        self.push_replication_flow(rf_name)

        src_table_name = "WF_LS"
        target_table_name = "RF_T_FROM_ODP_LTE2E_WF_LS"
        adl_v2_folder = f"CET_DWC_TEST/{target_table_name}"

        expected_row_count = self.get_table_total_count_from_data_server(conn_name=self.__dmis_2018_odp_conn, table_name=src_table_name)
        adl_v2_initial_row_count = self.get_non_table_total_count_from_data_server(conn_name=self.__adl_v2, folder_path=f"{adl_v2_folder}/initial", substring='part-', is_row_count_same=False)
        adl_v2_delta_row_count = self.get_non_table_total_count_from_data_server(conn_name=self.__adl_v2, folder_path=f"{adl_v2_folder}/delta", substring='part-', is_row_count_same=False)
        # insert 3, upsert 1, delete 1
        # verify initial folder
        self.validate_table_total_count(self.__adl_v2, adl_v2_folder, adl_v2_initial_row_count, expected_row_count-3)
        # verify delta folder
        self.validate_table_total_count(self.__adl_v2, adl_v2_folder, adl_v2_delta_row_count, 4)

    def test_rf_from_slt_to_adl_v2_initial(self):
        rf_name = "RF_DMIS_SLT_TO_ADL_V2_INITIAL"
        src_table_name = "WS_LM"
        target_table_name = "RF_T_FROM_DMIS_SLT_LTE2E_WS_LM"
        adl_v2_folder = f"CET_DWC_TEST/{target_table_name}"

        # run replication flow
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_completed(1500)

        # fetch the count and verify
        expected_row_count = self.get_table_total_count_from_data_server(conn_name=self.__dmis_2018_slt_conn, table_name=src_table_name)
        acutal_row_count = self.get_non_table_total_count_from_data_server(conn_name=self.__adl_v2, folder_path=f"{adl_v2_folder}/initial", substring='part-', is_row_count_same=False)
        self.validate_table_total_count(self.__adl_v2, adl_v2_folder, acutal_row_count, expected_row_count)
    
    def test_start_rf_from_S4_hanaOP_cds_to_S3_Delta(self):
        rf_name = "RF_S4HANAOP_CDS_to_S3_DELTA"
        src_table_name = "WF_LS"
        
        # run replication flow 
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        
        # Insert, update and delete records 
        s4_client = self.get_connection_session(self.__s4_op_conn)
        sleep(60) # wait for the initial load done
        
        s4_client.insert_records(src_table_name, 3)
        sleep(150)
        s4_client.update_records(src_table_name, 1)
        sleep(150)
        s4_client.delete_records(src_table_name, 3)
        sleep(150)
        
    def test_validate_rf_from_S4_hanaOP_cds_to_S3_Delta(self):
        # store replicaiton flow names and clean them in tear down
        rf_name = "RF_S4HANAOP_CDS_to_S3_DELTA"
        self.push_replication_flow(rf_name)
        # fetch count and verify rf
        S3_files_folder = "CET_DWC_TEST/RF_S4HANAOP_CDS_to_S3_DELTA/DHE2E_CDS_WF_LS"
        
        # get initial and delta count from target data server
        S3_files_initial_row_count = self.get_table_total_count_from_S3_server(self.__s3, f"{S3_files_folder}/initial")
        S3_files_delta_row_count = self.get_table_total_count_from_S3_server(self.__s3, f"{S3_files_folder}/delta")
      
        # verify initial folder, initial num is 100
        self.validate_table_total_count(self.__s3, S3_files_folder, S3_files_initial_row_count, 103)
        # verify delta folder. insert 3, upsert 1, delete 3. 
        self.validate_table_total_count(self.__s3, S3_files_folder, S3_files_delta_row_count, 3+1+3)

    def test_start_rf_from_abap_slt_to_GCS_Delta(self):
        rf_name = "RF_ABAP_SLT_to_GCS_DELTA"
        src_table_name = "WX_LS"
        
        # run replication flow 
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        
        # do delta scenarios:
        dmis_client = self.get_connection_session(self.__dmis_2018_slt_conn)
        sleep(60) # wait for the initial load done
        
        dmis_client.insert_records(src_table_name, 3)
        sleep(10)
        dmis_client.update_records(src_table_name, 1)
        sleep(10)
        dmis_client.delete_records(src_table_name, 3)
        sleep(10) 


    def test_validate_rf_from_abap_slt_to_GCS_Delta(self):
        # store replicaiton flow names and clean them in tear down
        rf_name = "RF_ABAP_SLT_to_GCS_DELTA"
        self.push_replication_flow(rf_name)
        # fetch count and verify rf
        GCS_files_folder = "CET_DWC_TEST/RF_ABAP_SLT_to_GCS_DELTA/LTE2E_WX_LS"
        
        # get initial and delta count from target data server
        GCS_files_initial_row_count = self.get_table_total_count_from_GCS_server(self.__gcs, f"{GCS_files_folder}/initial")
        GCS_files_delta_row_count = self.get_table_total_count_from_GCS_server(self.__gcs, f"{GCS_files_folder}/delta")
      
        # verify initial folder, initial num is 100
        self.validate_table_total_count(self.__gcs, GCS_files_folder, GCS_files_initial_row_count, 100)
        # verify delta folder. insert 3, upsert 1, delete 3. 
        self.validate_table_total_count(self.__gcs, GCS_files_folder, GCS_files_delta_row_count, 3+1+3)  

    def test_start_rf_hanaop_to_s3_delta(self):
        rf_name = "RF_HANAOP_to_S3_DELTA"
        src_schema = "CET_DWC_TEST"
        src_table_name = "S_BASIC_TYPES_TB_2"
        insert_list = [(1001, 'AAAA', 123456789012345678, '2015-02-09 15:00:00', 'AAAA', 'AAAA', 'AAAA', '1010'),(1002, 'char column mixed with empty space', 123.4506, '2015-01-14 09:29:23', 'AA', 'univarchar column mixed with empty space', 'univarchar column mixed with empty space', 'ffffffffffffffff'),(1003, 'varchar', 999999999999999999.5807, '9999/12/31 23:59:59.999', 'ff', 'clob', 'clob', 'ff'),(1004, '   ', -1E15+1, '1753-01-01 00:00:00.001', 'ffffff',  '  ', '  ', '123456789abcdeff'),(1005, '*&%^/\~!@', -10E3+0.9, '1753-01-01 00:00:00.001', '0101', 'NULL', 'NULL', 'NULL')]
        update_list = [({"VARCHAR_C2": 'UPDATE', "DEC_C3" : 1000}, "INT_C1 = 1002"),({"TIMESTAMP_C4": '2015-09-28 01:01:01'}, "INT_C1 > 1003")]
        delete_list = ["INT_C1 = 1001", "INT_C1 >= 1004 "]

        hana_client = self.get_connection_session(self.__hana_op_conn)

        recover_conition = "INT_C1 < 0 OR INT_C1 > 1000"
        hana_client.delete_rows(src_schema, src_table_name, recover_conition)

        #run replication flow
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        sleep(60) # wait for the initial load done

        for insert_tuple in insert_list:
            hana_client.insert_rows_all_columns(src_schema, src_table_name, insert_tuple)
        sleep(150)
        for update_tuple in update_list:
            hana_client.update_rows(src_schema, src_table_name, update_tuple[0], update_tuple[1])
        sleep(150)
        for delete_condition in delete_list:
            hana_client.delete_rows(src_schema, src_table_name, delete_condition)
        sleep(150)

    def test_validate_rf_hanaop_to_s3_delta(self):
        # store replicaiton flow names and clean them in tear down
        rf_name = "RF_HANAOP_to_S3_DELTA"
        self.push_replication_flow(rf_name)
        # fetch count and verify rf

        # get initial and delta count from target data server
        S3_files_folder = "CET_DWC_TEST/RF_T_FROM_HANAOP_S_BASIC_TYPES_TB_2"
        S3_files_initial_row_count = self.get_table_total_count_from_data_server(self.__s3, f"{S3_files_folder}/initial")
        S3_files_delta_row_count = self.get_table_total_count_from_data_server(self.__s3, f"{S3_files_folder}/delta")

        # verify initial folder, initial num is 10
        self.validate_table_total_count(self.__s3, S3_files_folder, S3_files_initial_row_count, 10)
        # verify delta folder. insert 5, upsert 3, delete 3.
        self.validate_table_total_count(self.__s3, S3_files_folder, S3_files_delta_row_count, 5+3+3)

    def test_rf_hanaoncloud_to_gcs_initial(self):
        rf_name = "RF_HANAonCloud_to_GCS_INITIAL"
        src_schema = "CET_DWC_TEST"
        src_table_name = "S_ALL_TYPES_TB"

        hana_client = self.get_connection_session(self.__hana_oc_conn)

        recover_conition = "INT_C1 < 0 OR INT_C1 > 1000"
        hana_client.delete_rows(src_schema, src_table_name, recover_conition)

        #run replication flow
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_completed()
        sleep(60) # wait for the initial load done

        # get initial and delta count from target data server
        gcs_files_folder = "CET_DWC_TEST/RF_T_FROM_HANACLOUD_S_ALL_TYPES_TB"
        gcs_files_initial_row_count = self.get_table_total_count_from_data_server(self.__gcs, f"{gcs_files_folder}/initial")

        # verify initial folder, initial num is 5
        self.validate_table_total_count(self.__gcs, gcs_files_folder, gcs_files_initial_row_count, 5)
        
    def test_rf_from_DMIS_SLT_TO_GBQ_initial(self):
        rf_name = "RF_DMIS_SLT_TO_GBQ_INITIAL"
        #os.environ['https_proxy'] = 'http://proxy.sin.sap.corp:8080/'
        src_table_name="WF_LM"
        target_table_name = "RF_T_FROM_DMIS_LTE2E_WF_LM"
        rf_name = "RF_DMIS_SLT_TO_GBQ_INITIAL"
        
        # truncate GBQ target table before run task
        gbq_client = self.get_connection_session(self.__gbq_conn)
        gbq_client.truncate_table('CET_TEST',target_table_name)
        
        # run replication flow 
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_completed(1500)
        
        # fetch count and verify rf
        expected_row_count = self.get_table_total_count_from_data_server(conn_name=self.__dmis_2018_slt_conn, table_name=src_table_name)
        acutal_row_count = self.get_table_total_count_from_gbq(conn_name=self.__gbq_conn, table_name=target_table_name)
        self.validate_table_total_count(self.__gbq_conn, target_table_name, actual_count=acutal_row_count, expected_count=expected_row_count)

    def test_start_rf_from_S4HOP_TO_GBQ_DELTA(self):
        rf_name = "RF_S4HOP_TO_GBQ_DELTA"
        #os.environ['https_proxy'] = 'http://10.33.130.231:8080/'
        src_table_name="WS_LM"
        target_table_name = "RF_T_FROM_S4H_DHE2E_CDS_WS_LM"
        
        # truncate GBQ target table before run task
        gbq_client = self.get_connection_session(self.__gbq_conn)
        gbq_client.truncate_table('CET_TEST',target_table_name)
        
        # run replication flow 
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        sleep(60) # wait for the initial load done

        # Insert, update and delete 5 records in ABAP and check the delta behavior
        s4_client = self.get_connection_session(self.__s4_op_conn)
        sleep(60) # wait for the initial load done
        
        s4_client.insert_records(src_table_name, 3)
        sleep(30)
        s4_client.update_records(src_table_name, 1)
        sleep(30)
        s4_client.delete_records(src_table_name, 1)
        sleep(30)

    def test_validate_rf_from_S4HOP_TO_GBQ_DELTA(self):
        rf_name = "RF_S4HOP_TO_GBQ_DELTA"
        #os.environ['https_proxy'] = 'http://10.33.130.231:8080/'
        self.push_replication_flow(rf_name)

        src_table_name="WS_LM"
        target_table_name = "RF_T_FROM_S4H_DHE2E_CDS_WS_LM"

        expected_row_count = self.get_table_total_count_from_data_server(conn_name=self.__s4_op_conn, table_name=src_table_name)
        acutal_row_count = self.get_table_total_count_from_gbq(conn_name=self.__gbq_conn, table_name=target_table_name)
        
        self.validate_table_total_count(self.__gbq_conn, target_table_name, actual_count=acutal_row_count, expected_count=expected_row_count+3)

    def test_start_rf_from_HANA_CLOUD_TO_GBQ_DELTA(self):
        rf_name = "RF_HANA_CLOUD_TO_GBQ_DELTA"
        #os.environ['https_proxy'] = 'http://10.33.130.231:8080/'
        target_table_name = "RF_T_FROM_HC_LTE2E_WS_LS"
        src_schema = "CET_DWC_TEST"
        src_table_name = "LTE2E_WS_LS"
        columns = ["CLIENT","REC_ID","FIELD_NUMC01","FIELD_NUMC06","FIELD_NUMC10","FIELD_CHAR60"]
        insert_values = ('300','4137498018','9','547382','0000000000','Elko')
        delete_condition = "REC_ID = '4137498018'"
        
        # truncate GBQ target table before run task
        gbq_client = self.get_connection_session(self.__gbq_conn)
        gbq_client.truncate_table('CET_TEST',target_table_name)
        
        # run replication flow 
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_running()
        sleep(60) # wait for the initial load done

        # Insert and delete 5 records in ABAP and check the delta behavior
        # do delta change
        hana_client = self.get_connection_session(self.__hana_oc_conn)

        hana_client.insert_rows(src_schema, src_table_name, columns, insert_values)
        sleep(10)
        hana_client.delete_rows(src_schema, src_table_name, delete_condition)
        sleep(10)

    def test_validate_rf_from_HANA_CLOUD_TO_GBQ_DELTA(self):
        rf_name = "RF_HANA_CLOUD_TO_GBQ_DELTA"
        #os.environ['https_proxy'] = 'http://10.33.130.231:8080/'
        self.push_replication_flow(rf_name)

        src_table_name="LTE2E_WS_LS"
        target_table_name = "RF_T_FROM_HC_LTE2E_WS_LS"

        expected_row_count = self.get_table_total_count_from_data_server(conn_name=self.__hana_oc_conn, table_name=src_table_name)
        acutal_row_count = self.get_table_total_count_from_gbq(conn_name=self.__gbq_conn, table_name=target_table_name)
        
        self.validate_table_total_count(self.__gbq_conn, target_table_name, actual_count=acutal_row_count, expected_count=expected_row_count+2)
        
    def test_rf_from_Delta_capture_local_table_to_HANAOnPrem_initial(self):
        rf_name = "RF_Delta_capture__local_table_to_HANAOnPrem"
        target_table_name = "RF_T_FROM_DELTA_CAPTURE_LOCAL_TABLE_S_NUMBERS_TB"
        
        # run replication flow 
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_completed()
        
        # fetch count and verify rf
        acutal_row_count = self.get_table_total_count_from_data_server(conn_name=self.__hana_op_conn, table_name=target_table_name)
        self.validate_table_total_count(self.__gbq_conn, target_table_name, actual_count=acutal_row_count, expected_count=27)
        
        
    def test_rf_from_Delta_capture_local_table_to_HDL_Files_initial(self):
        rf_name = "RF_Delta_capture__local_table_to_HDL_Files2"
        hdl_files_folder = "CET_DWC_TEST/DELTA_CAPTURE_LOCAL_INITIAL"
        
        # run replication flow 
        replication_flow_execution = self.safe_run_replication_flow(rf_name)
        replication_flow_execution.wait_until_completed()
        
        # verify initial folder
        hdl_files_initial_row_count = self.get_non_table_total_count_from_data_server(self.__hdl_files_conn, f"{hdl_files_folder}/initial")
        self.validate_table_total_count(self.__hdl_files_conn, hdl_files_folder, hdl_files_initial_row_count, 19)

