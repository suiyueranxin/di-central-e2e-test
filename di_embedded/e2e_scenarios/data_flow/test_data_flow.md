## Description
This test suite covers data flow scenarios which transfer data from ABAP/HANA/DWC/HDL to HANA/DWC. Refer to https://wiki.one.int.sap/wiki/display/DWAAS/Data+Flows+-+E2E+Scenarios.

## Connections
1. Source
    - ABAP
    - HANA
    - DWC
    - HDL
2. Target
    - HANA
    - DWC

## Tests
1. test_df_from_hana_on_premise_to_local_repo_with_append_mode: Transfer data from HANA on-premise to DWC local repository with append mode.
    - DF Name: DF_HANAOP_to_LocalRepo
    - Connections: HANA_OnPrem
    - Operator: 
    - Mode: Append
2. test_df_from_hana_on_premise_to_hana_on_cloud_with_truncate_mode: Transfer data from HANA on-premise to HANA on-cloud with truncate mode.
    - DF Name: DF_HANAOP_to_HANAonCloud
    - Connections: HANA_OnPrem, CIT_HANA_Cloud
    - Operator: Join, Projection, Aggregation
    - Mode: Truncate
3. test_df_from_s4hana_on_premise_to_hana_on_cloud_with_truncate_mode: Transfer data from S4HANA on-premise to HANA on-cloud with truncate mode.
    - DF Name: DF_S4HANAOP_to_HANAonCloud
    - Connections: CIT_S4H_2021 (ABAP), CIT_HANA_Cloud
    - Operator: Projection, Script
    - Mode: Truncate
4. test_df_from_s4hana_on_premise_to_local_repo_with_append_mode: Transfer data from S4HANA on-premise to DWC local repository with append mode.
    - DF Name: DF_S4HANAOP_to_LocalRepo
    - Connections: CIT_S4H_2021 (ABAP)
    - Operator: 
    - Mode: Append
5. test_df_from_hdl_db_to_hana_on_cloud_with_overwrite_mode: Transfer data from HANA DATALAKE table to HANA on-cloud with overwrite mode.
    - DF Name: DF_HDL_DB_to_HANAonCloud
    - Connections: HDL_DB, CIT_HANA_Cloud
    - Operator: Projection
    - Mode: Overwrite
6. test_df_from_hdl_files_to_hana_cloud_with_delete_mode: Transfer data from HANA DATALAKE files to HANA on-cloud with delete mode.
    - DF Name: DF_HDL_DB_to_HANAonCloud
    - Connections: HDL_DB, CIT_HANA_Cloud
    - Operator: Projection
    - Mode: Delete
7. test_df_from_local_repo_to_hana_on_cloud_with_truncate_mode: Transfer data from DWC to HANA on-cloud with truncate mode.
    - DF Name: DF_LocalRepo_to_HANAonCloud
    - Connections: DWC, CIT_HANA_Cloud
    - Operator: Join
    - Mode: Truncate
8. test_df_from_local_repo_to_local_repo_with_truncate_mode: Transfer data from DWC to DWC with truncate mode.
    - DF Name: DF_LocalRepo_to_LocalRepo
    - Connections: DWC
    - Operator: Union, Join, Projection
    - Mode: Truncate
9. test_df_from_local_repo_to_local_repo_Aggregation_with_truncate_mode: Calculate aggregation data from DWC to DWC with truncate mode.
    - DF Name: DF_LocalRepo_to_LocalRepo_Aggregation
    - Connections: DWC
    - Operator: Aggregation, Projection
    - Mode: Truncate
10. test_df_from_hana_on_cloud_to_hana_on_cloud_using_view_with_overwrite_mode: Transfer data from HANA on-cloud to HANA on-cloud using view with overwrite mode.
    - DF Name: DF_View_HANAonCloud_to_HANAonCloud
    - Connections: CIT_HANA_Cloud
    - Operator: Union, Join
    - Mode: Overwrite
11. test_df_from_hana_on_cloud_to_hana_on_cloud_with_truncate_mode: Transfer data from HANA on-cloud to HANA on-cloud with truncate mode.
    - DF Name: DF_HANAonCloud_to_HANAonCloud
    - Connections: CIT_HANA_Cloud
    - Operator: Projection
    - Mode: Truncate
12. test_df_from_hana_on_cloud_to_local_repo_with_truncate_mode: Transfer data from HANA on-cloud to DWC local repository with truncate mode.
    - DF Name: DF_HANAonCloud_to_LocalRepo
    - Connections: CIT_HANA_Cloud
    - Operator: Join, Projection
    - Mode: Truncate
13. test_df_from_s4hana_on_cloud_to_local_repo_with_append_mode: Transfer data from S/4 HANA on Cloud (CWR) to DWC local repository with append mode.
    - DF Name: DF_S4HANAonCloud_to_LocalRepo
    - Connections: CIT_S4H_Cloud_CC8_ABAP
    - Operator: Projection
    - Mode: Append
14. test_df_from_s4hana_on_cloud_to_hana_on_cloud_with_append_mode: Transfer data from S/4 HANA on Cloud (CWR) to HANA on-cloud with append mode.
    - DF Name: DF_S4HANAonCloud_to_HANAonCloud2
    - Connections: CIT_S4H_Cloud_CWR_ABAP,CIT_HANA_Cloud
    - Operator: Projection,Aggregation,AVG/MAX/MIN
    - Mode: Append
