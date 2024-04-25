## Description
This test suite covers E2E pipelines in customer issues.

## Operators
1. Generation 1
    - Connectivity: OpenAPI Client
    - Data Workflows: Pipeline, Workflow Trigger, Worflow Terminator
    - HANA: Read HANA Table
    - File: Write File, Read File
    - Processing: Python3 Operator, Data Generator
    - Utilities: Decode Table
2. Generation 2
    - ABAP: Read Data From SAP System
    - HANA: Write HANA Table
    - Processing: Python3 Operator
    - Structured Data Operators: Table Consumer
    - Utilities: Table to Binary

## Tests
1. test_dibugs_10531: [Gen2] Operator "Table to Binary" doesn't work with static vType input.
2. test_dibugs_11570: [Gen1] Working using Pipeline operator should pop up all substitutions when run/run as
3. test_dibugs_12139: [Gen1] HANA operators output specific column names with quotes
4. test_dibugs_13121: [Gen1] OpenAPI Client OAuth Flow with auth type cnode_no_auth, openapi_no_auth, oauth2
5. test_dibugs_11222: [Gen1] Monitor Files SFTP not closing connections
6. test_dibugs_13282: [Gen1] Multiple FTP connections remain open after data upload via FTP
7. test_dibugs_11143: [Gen1] Service Unavailable for HDLFS connection
8. test_dibugs_14309: [Gen1] Write Table is not fully compatibale with Decode Table
9. test_dibugs_18399: [Gen1/Gen2] Write data to ADL_V2 with auth type SAS (share access signatures) cannot work
10. test_statuscheck_ADLV2_oauth2_client: Status check of ADL_V2 connections with client credential method
11. test_statuscheck_ADLV2_oauth2_user: Status check of ADL_V2 connections with username password method
12. test_dibugs_18291: [Gen1] SFTP connection with RSA key
13. test_dibugs_18249: [Gen1] Transfer data with SFTP on windows
14. test_dibugs_18162: [Gen1/Gen2] OAuth2 check in ODATA, OpenAPI and HTTP connections