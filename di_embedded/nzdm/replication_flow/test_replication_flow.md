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
1. test_rf_from_hana_on_prem_to_datasphere_initial: Transfer data from HANA on_prem to datasphere using replication flow.
    - Connections: IM_HANA_OnPrem, DUMMY_Datasphere
    - Tables: S_CORP1_INVOICE, NZDM_RF_T_FROM_HANAOP_S_CORP1_INVOICE
    - RF tasks: 1
    - RF load tpye: initial
2. test_rf_from_abap_cds_to_hana_cloud_delta: Transfer data from ABAP CDS view to HANA Cloud using replication flow.
    - Connections: CIT_S4H_2021_ABAP, CIT_HANA_Cloud
    - Tables: DHE2E_CDS_WN_LS, NZDM_RF_T_FROM_ABAP_CDS_DHE2E_CDS_WN_LS_REPLICATE
    - RF tasks: 1
    - RF load tpye: delta
3. test_rf_from_hana_on_cloud_to_s3_initial: Transfer data from HANA on_cloud to s3 using replication flow.
    - Connections: CIT_HANA_Cloud, IM_S3
    - Tables: S_CORP1_CUSTOMER, NZDM_RF_T_FROM_HANAOC_S_CORP1_CUSTOMER
    - RF tasks: 1
    - RF load tpye: initial
4. test_rf_from_abap_cds_to_gcs_initial: Transfer data from ABAP CDS view to gcs using replication flow.
    - Connections: CIT_S4H_2021_ABAP, IM_GCS
    - Tables: DHE2E_CDS_WS_LS, NZDM_RF_T_FROM_DHE2E_CDS_WS_LS
    - RF tasks: 1
    - RF load tpye: initial
5. test_rf_from_dmis_slt_to_gbq_initial: Transfer data from DMIS SLT to GBQ using replication flow.
    - Connections: CIT_DMIS_2018_SLT, IM_GBQ
    - Tables: LTE2E_WN_LX, NZDM_RF_T_FROM_DMIS_SLT_LTE2E_WN_LX_INITIAL
    - RF tasks: 1
    - RF load tpye: initial
6. test_rf_from_abap_cds_to_adlv2_delta: Transfer data from ABAP CDS view to ADLV2 using replication flow.
    - Connections: CIT_S4H_2021_ABAP, IM_ADL_V2
    - Tables: DHE2E_CDS_WS_LX, NZDM_WS_LX_REPLICATE
    - RF tasks: 1
    - RF load tpye: delta
7. test_rf_from_dmis_odp_to_hdl_initial: Transfer data from ABAP CDS view to gcs using replication flow.
    - Connections: CIT_DMIS_2018_ODP, HDL_FILES
    - Tables: LTE2E_WS_LS, NZDM_LTE2E_WS_LS
    - RF tasks: 1
    - RF load tpye: initial