from rich import print
from di_embedded_api_test.components.rms.task import TaskEntity, TaskStatus
from utils.case_base import BaseTest
from time import sleep
import os


class ReplicationFlowTest(BaseTest):

    def test_rf_from_hana_on_prem_to_datasphere_initial(self):
        rf_name = "RF_FROM_HANAOP_TO_DATASPHERE_INITIAL"

        task_entity: TaskEntity = self.safe_create_rf_task(rf_name)
        task_entity.wait_until_completed(max_retries=self._max_retries, timeout=2)

        # source and target info
        source_connection_name = "IM_HANA_OnPrem"
        source_table = "S_CORP1_INVOICE"
        target_connection_name = "DUMMY_Datasphere"
        target_table = "NZDM_RF_T_FROM_HANAOP_S_CORP1_INVOICE"

        # check initial part
        abap_count = self.get_table_total_count_from_data_server(source_connection_name, source_table)
        hana_count = self.get_table_total_count_from_data_server(target_connection_name, target_table)
        self.validate_table_total_count_by_src_and_target(source_connection_name, source_table, target_connection_name, target_table, abap_count, hana_count)

    # Long running replication flow, it will cross three upgrade stages. 
    # In pre-upgrade stage, we start the replication flow; in pre-upgrade, upgrade and post-upgrade stages, we check status, do delta and check data.
    def test_rf_from_abap_cds_to_hana_cloud_delta(self):
        rf_name = "RF_FROM_ABAP_CDS_TO_HANA_CLOUD_REPLICATE"

        # source and target info
        source_connection_name = "CIT_S4H_2021_ABAP"
        source_table = "DHE2E_CDS_WN_LS"
        src_abap_table_name = "WN_LS"
        target_connection_name = "CIT_HANA_Cloud"
        target_table = "NZDM_RF_T_FROM_ABAP_CDS_DHE2E_CDS_WN_LS_REPLICATE"
        
        if self.is_pre_upgrade:
            task_entity: TaskEntity = self.safe_create_rf_task(rf_name, auto_clean=False)
            task_entity.wait_until_delta_running(max_retries=self._max_retries, timeout=2)

            # check initial part
            abap_count = self.get_table_total_count_from_data_server(source_connection_name, src_abap_table_name)
            hana_count = self.get_table_total_count_from_data_server(target_connection_name, target_table)
            self.validate_table_total_count_by_src_and_target(source_connection_name, source_table, target_connection_name, target_table, abap_count, hana_count)

        # check rf status is running
        self.validate_rf_task_status(rf_name, TaskStatus.DELTA_RUNNING)
        
        # insert 3
        s4_client = self.get_connection_session(source_connection_name)
        s4_client.insert_records(src_abap_table_name, 3)
        sleep(10)

        # check delta part
        abap_count = self.get_table_total_count_from_data_server(source_connection_name, src_abap_table_name)
        hana_count = self.get_table_total_count_from_data_server(target_connection_name, target_table)
        self.validate_table_total_count(target_connection_name, target_table, hana_count, abap_count)


    def test_rf_from_dmis_slt_to_gbq_initial(self):
        rf_name = "RF_FROM_DMIS_SLT_TO_GBQ_INITIAL"
        # os.environ['https_proxy'] = 'http://proxy.sin.sap.corp:8080/'
        # source and target info
        source_connection_name = "CIT_DMIS_2018_SLT"
        source_table = "LTE2E_WN_LX"
        src_abap_table_name = "WN_LX"
        target_connection_name = "IM_GBQ"
        target_table = "NZDM_RF_T_FROM_DMIS_SLT_LTE2E_WN_LX_INITIAL"
        
        # truncate GBQ target table before run task
        gbq_client = self.get_connection_session(target_connection_name)
        gbq_client.truncate_table('CET_TEST',target_table) 
        
        task_entity: TaskEntity = self.safe_create_rf_task(rf_name)
        task_entity.wait_until_completed(max_retries=self._max_retries, timeout=2)

        # check initial part
        abap_count = self.get_table_total_count_from_data_server(source_connection_name, src_abap_table_name)
        gbq_count = self.get_table_total_count_from_gbq(target_connection_name, target_table)
        self.validate_table_total_count_by_src_and_target(source_connection_name, source_table, target_connection_name, target_table, abap_count, gbq_count)
      
    
    def test_rf_from_abap_cds_to_adlv2_delta(self):
        rf_name = "RF_FROM_ABAP_CDS_TO_ADLV2_REPLICATE"
        
        task_entity: TaskEntity = self.safe_create_rf_task(rf_name)
        task_entity.wait_until_delta_running(max_retries=self._max_retries, timeout=2)

        # source and target info

        source_connection_name = "CIT_S4H_2021_ABAP"
        source_table = "DHE2E_CDS_WS_LX"
        src_abap_table_name = "WS_LX"
        target_connection_name = "IM_ADL_V2"
        target_table = "NZDM_WS_LX_REPLICATE"
        adl_v2_folder = f"CET_DWC_TEST/{target_table}"

        # check initial part
        abap_count = self.get_table_total_count_from_data_server(source_connection_name, src_abap_table_name)
        adl_v2_initial_row_count = self.get_non_table_total_count_from_data_server(target_connection_name, folder_path=f"{adl_v2_folder}/initial", substring='part-', is_row_count_same=False)
        
        self.validate_table_total_count(target_connection_name, adl_v2_folder, adl_v2_initial_row_count, abap_count)

        # insert 3
        s4_client = self.get_connection_session(source_connection_name)
        s4_client.insert_records(src_abap_table_name, 3)
        sleep(10)

        # check delta part
        adl_v2_delta_row_count = self.get_non_table_total_count_from_data_server(target_connection_name, folder_path=f"{adl_v2_folder}/delta", substring='part-', is_row_count_same=False)
        self.validate_table_total_count(target_connection_name, adl_v2_folder, adl_v2_delta_row_count, 3)
        
    def test_rf_from_dmis_odp_to_hdl_initial(self):
        rf_name = "RF_FROM_DMIS_ODP_TO_HDL_INITIAL"
        task_entity: TaskEntity = self.safe_create_rf_task(rf_name)
        task_entity.wait_until_completed(max_retries=self._max_retries, timeout=2)

        # source and target info
        source_connection_name = "CIT_DMIS_2018_ODP"
        source_table = "LTE2E_WS_LS"
        src_abap_table_name = "WS_LS"
        target_connection_name = "HDL_FILES"
        target_table = "NZDM_LTE2E_WS_LS"
        hdl_files_folder = f"CET_DWC_TEST/{target_table}"

        # check initial part
        abap_count = self.get_table_total_count_from_data_server(source_connection_name, src_abap_table_name)
        hdl_files_initial_row_count = self.get_non_table_total_count_from_data_server(target_connection_name, folder_path=f"{hdl_files_folder}/initial", substring='part-', is_row_count_same=False)
        self.validate_table_total_count_by_src_and_target(source_connection_name, source_table, target_connection_name, target_table, abap_count, hdl_files_initial_row_count)

    def test_rf_from_hana_on_cloud_to_s3_initial(self):
        rf_name = "RF_FROM_HANAOC_TO_S3_INITIAL"
        # os.environ['https_proxy'] = 'http://proxy.sin.sap.corp:8080/'
 
        # check initial part
        task_entity: TaskEntity = self.safe_create_rf_task(rf_name, False)
        task_entity.wait_until_completed(max_retries=self._max_retries, timeout=2)
 
        # source and target info
        source_connection_name = "CIT_HANA_Cloud"
        source_table = "S_CORP1_CUSTOMER"
        target_connection_name = "IM_S3"
        S3_files_folder = "CET_DWC_TEST/NZDM_RF_T_FROM_HANAOC_S_CORP1_CUSTOMER"
        
        # check result
        # source_count = 18
        source_count = self.get_table_total_count_from_data_server(source_connection_name, source_table)
        S3_target_count = self.get_table_total_count_from_S3_server(target_connection_name, f"{S3_files_folder}/initial")
        self.validate_table_total_count_by_src_and_target(source_connection_name, source_table, target_connection_name, f"{S3_files_folder}/initial", source_count, S3_target_count)

    def test_rf_from_abap_cds_to_gcs_initial(self):
        rf_name = "RF_FROM_ABAP_CDS_TO_GCS_INITIAL"
        # os.environ['https_proxy'] = 'http://proxy.sin.sap.corp:8080/'
        # check initial part
        task_entity: TaskEntity = self.safe_create_rf_task(rf_name)
        task_entity.wait_until_completed(max_retries=self._max_retries, timeout=2)
 
        # source and target info
        source_connection_name = "CIT_S4H_2021_ABAP"
        source_table = "DHE2E_CDS_WS_LS"
        src_abap_table_name = "WS_LS"
        target_connection_name = "IM_GCS"
        GCS_target_table = "CET_DWC_TEST/NZDM_RF_T_FROM_DHE2E_CDS_WS_LS"

        # check result
        abap_count = self.get_table_total_count_from_data_server(source_connection_name, src_abap_table_name)
        GCS_target_count = self.get_table_total_count_from_GCS_server(target_connection_name, f"{GCS_target_table}/initial")
        self.validate_table_total_count_by_src_and_target(source_connection_name, source_table, target_connection_name, f"{GCS_target_table}/initial", abap_count, GCS_target_count)


    