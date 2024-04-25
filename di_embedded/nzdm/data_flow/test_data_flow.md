## Description
This test suite covers data flow scenarios which transfer data from ABAP/HANA/DWC/HDL to HANA/DWC. Refer to https://wiki.one.int.sap/wiki/display/DWAAS/Data+Flows+-+E2E+Scenarios.

## Connections
1. Source
    - ABAP
    - HANA
2. Target
    - Datasphere

## Tests
1. test_df_from_hana_on_prem_to_datasphere: Transfer data from HANA on-prem to Datasphere with truncate mode.
    - DF Name: DF_HANAOP_TO_DATASPHERE
    - Connections: IM_HANA_OnPrem, DUMMY_Datasphere
    - Tables: S_ALL_TYPES_TB, NZDM_DF_T_FROM_HANAOP_S_ALL_TYPES_TB
    - Operator: 
    - Mode: truncate
2. test_df_from_s4h_cds_to_hana_cloud: Transfer data from ABAP CDS view to Datasphere with truncate mode.
    - DF Name: DF_S4H_CDS_to_HANAonCloud
    - Connections: CIT_S4H_2021_ABAP, CIT_HANA_Cloud
    - Tables: DHE2E_CDS_WN_LM, NZDM_DF_T_FROM_S4H_DHE2E_CDS_WN_LM
    - Operator: 
    - Mode: truncate