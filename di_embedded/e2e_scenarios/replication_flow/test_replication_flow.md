## Description
This test suite covers replication flow scenarios which transfer data from ABAP/HANA to HANA/DWC/HDL. Refer to https://wiki.one.int.sap/wiki/display/DWAAS/E2E+Scenarios+-+Replication+Flows.

## Connections
1. Source
    - ABAP
    - HANA
2. Target
    - HANA
    - DWC
    - HDL

## Tests
1. test_validate_rf_from_abap_cds_to_hdl_files_delta_then_df_to_local_repo: Transfer data from ABAP CDS view to HDL file using replication flow, then transfer data from HDL file to DWC local repository with append mode.
    - RF Name: RF_S4HOP_CDS_TO_HDL_FILES_DELTA
    - DF Name: DF_HDL_FILES_TO_DWC
    - Connections: CIT_S4H_2021 (ABAP), HDL_FILES
    - RF tasks: 1
    - RF filter: 1
    - RF load tpye: delta
    - DF operator: Union
    - DF mode: Truncate
2. test_rf_from_abap_slt_to_local_repo_initial: Transfer data from ABAP SLT table to DWC local repository.
    - RF Name: RF_ABAP_SLT_to_DWC_INITIAL
    - Connections: CIT_DMIS_2018_SLT (ABAP)
    - RF tasks: 3
    - RF filter: 2
    - RF load tpye: initial
3. test_validate_rf_from_abap_slt_to_local_repo_delta: Transfer data from ABAP SLT table to DWC local repository.
    - RF Name: RF_ABAP_SLT_to_DWC_DELTA
    - Connections: CIT_DMIS_2018_SLT (ABAP)
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: delta
4. test_rf_from_hana_on_cloud_to_hana_on_prem_initial: Transfer data from HANA on cloud to HANA on prem.
    - RF Name: RF_HANAonCloud_to_HANAOP_INITIAL
    - Connections: HANA_OnPrem, CIT_HANA_Cloud
    - RF tasks: 3
    - RF filter: 0
    - RF load tpye: initial
5. test_validate_rf_from_s4h_odp_to_hana_on_cloud_delta: Transfer data from S4H ODP to HANA on cloud.
    - RF Name: RF_ODPS4H_To_HanaCloud_DELTA
    - Connections: CIT_S4H_2021 (ABAP)
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: delta
6. test_validate_rf_from_dmis_odp_to_local_repo_delta: Transfer data from DMIS ODP to DWC local repository.
    - RF Name: RF_ODP_To_DWC
    - Connections: CIT_DMIS_2018_SLT (ABAP)
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: delta
7. test_validate_rf_from_hana_on_premise_to_hana_on_cloud_delta: Transfer data from HANA on prem to HANA on cloud.
    - RF Name: RF_HANAOP_to_HANAonCloud_DELTA
    - Connections: HANA_OnPrem, CIT_HANA_Cloud
    - RF tasks: 5
    - RF filter: 4
    - RF load tpye: delta
8. test_validate_rf_from_S4HANAOP_to_hana_on_cloud_then_df_to_local_repo_initial: Transfer data from S4HANAonPrem to hana_on_cloud using replication flow, then transfer data from hana_on_cloud to DWC local repository.
    - RF Name: RF_S4HOP_HANAonCloud_initial_small
    - DF Name: DF_HANAonCloud_DWC_initial_small
    - Connections: HANA_OnPrem,CIT_HANA_Cloud
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: initial
    - DF mode: Append
9. test_rf_from_S4HCLOUDCC8_to_local_repo_initial: Transfer data from S/4 HANA Cloud (CDS) to DWC local repository
    - RF Name: RF_S4HCLOUDCC8_to_DWC_INITIAL
    - Connections: CIT_S4H_Cloud (CC8, ABAP)
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: initial
10. test_validate_rf_from_hana_on_cloud_to_local_repo_via_delta_enabled_local_table_delta: Transfer data from HANA on Cloud to DWC local repository via delta-enabled local table.
    - RF Name: RF_HANAOC_to_DWC_DELTA_via_Delta_Local_Table
    - Connections: CIT_HANA_Cloud
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: delta
11. test_validate_rf_from_odp_to_adl_v2_delta: Transfer data from DMIS ODP to ADL_V2.
    - RF Name: RF_DMIS_ODP_TO_ADL_V2_DELTA
    - Connections: CIT_DMIS_2018_SLT (ABAP), IM_ADL_V2
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: delta
12. test_rf_from_slt_to_adl_v2_initial: Transfer data from ABAP SLT table to ADL_V2 
    - RF Name: RF_FROM_DMIS_SLT_TO_ADL_V2_INITIAL
    - Connections: CIT_DMIS_2018_SLT (ABAP), IM_ADL_V2
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: initial
13. test_validate_rf_from_S4_hanaOP_cds_to_S3_Delta.
    - RF Name: RF_S4HANAOP_CDS_to_S3_DELTA
    - Connections: CIT_S4H_2021 (ABAP), IM_S3
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: delta
14. test_validate_rf_from_abap_slt_to_GCS_Delta.
    - RF Name: RF_ABAP_SLT_to_GCS_DELTA
    - Connections: CIT_DMIS_2018_SLT (ABAP), IM_GCS
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: delta    
15. test_validate_rf_hanaop_to_s3_delta.
    - RF Name: RF_HANAOP_to_S3_DELTA
    - Connections: HANA_OnPrem, IM_S3
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: delta
16. test_start_rf_hanaoncloud_to_gcs_initial.
    - RF Name: RF_HANAonCloud_to_GCS_INITIAL
    - Connections: CIT_HANA_Cloud, IM_GCS
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: initial
17. test_rf_from_DMIS_SLT_TO_GBQ_initial.
    - RF Name: RF_DMIS_SLT_TO_GBQ_INITIAL
    - Connections: CIT_DMIS_2018_SLT (ABAP), IM_GBQ
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: initial
18. test_validate_rf_from_S4HOP_TO_GBQ_DELTA.
    - RF Name: RF_S4HOP_TO_GBQ_DELTA
    - Connections: CIT_S4H_2021_ABAP (ABAP), IM_GBQ
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: delta
19. test_validate_rf_from_HANA_CLOUD_TO_GBQ_DELTA.
    - RF Name: RF_HANA_CLOUD_TO_GBQ_DELTA
    - Connections: CIT_HANA_Cloud (HANA), IM_GBQ
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: delta
20. test_rf_from_Delta_capture_local_table_to_HANAOnPrem_initial.
    - RF Name: RF_Delta_capture__local_table_to_HANAOnPrem
    - Connections: HANA_OnPrem, Delta_capture_local_table
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: initial
21. test_rf_from_Delta_capture_local_table_to_HDL_Files_initial.
    - RF Name: RF_Delta_capture__local_table_to_HDL_Files2
    - Connections: HDL_FILES, Delta_capture_local_table
    - RF tasks: 1
    - RF filter: 0
    - RF load tpye: initial