## Description
This test suite covers E2E pipelines which transfer small size data from ABAP (CDS view, SLT table) to HANA/File/KAFKA with initial/replication/delta load.

## Operators
1. Generation 1
    - ABAP: ABAP CDS Reader V1, ABAP Converter, SAP ABAP Operator (Eraser), SLT Connector
    - HANA: SAP HANA Client, Write HANA Table
    - KAFKA: Kafka Producer, Kafka Consumer
    - File: Write File
    - Custom Operator: ABAP Message Controller (Python)
2. Generation 2

## Tests
### CDS Gen 1
1. test_validate_abap_cds_to_hana_initial_small: Gen 1 Transfer data from ABAP CDS to HANA cloud with initial load (ABAP Data: DHE2E_CDS_WN_LS).
2. test_validate_abap_cds_to_filestore_initial_small: Gen 1 Transfer data from ABAP CDS to Filestore with initial load (ABAP Data: DHE2E_CDS_WN_LS).
3. test_validate_abap_cds_to_kafka_delta: Gen 1 Transfer data from ABAP CDS to KAFKA with delta load (ABAP Data: DHE2E_CDS_WN_LM).
4. test_validate_abap_cds_to_filestore_delta: Gen 1 Transfer data from ABAP CDS to Filestore with delta load (ABAP Data: DHE2E_CDS_WN_LM).
### SLT Gen 1
1. test_validate_abap_slt_to_hana_initial_small: Gen 1 Transfer data from ABAP SLT to HANA cloud with initial load (ABAP Data: LTE2E_WN_LS).
2. test_validate_abap_slt_to_hana_initial_fat: Gen 1 Transfer data from ABAP SLT to HANA cloud with initial load (ABAP Data: LTE2E_WF_LS).
3. test_validate_abap_slt_to_kafka_delta: Gen 1 Transfer data from ABAP SLT to KAFKA with delta load (ABAP Data: LTE2E_WS_LL).
4. test_validate_abap_slt_to_filestore_initial_small: Gen 1 Transfer data from ABAP SLT to Filestore with initial load (ABAP Data: LTE2E_WS_LS).
5. test_validate_abap_slt_to_filestore_delta:Gen 1 Transfer data from ABAP SLT to Filestore with delta load (ABAP Data: LTE2E_WN_LL).
### SLT Gen 2
- test_validate_abap_slt_to_filestore_initial_small_gen2: Gen 2 Transfer data from ABAP SLT to Filestore with initial load (ABAP Data: LTE2E_WN_LS).
- test_validate_abap_slt_to_hana_initial_small_gen2: Gen 2 Transfer data from ABAP SLT to HANA cloud with initial load (ABAP Data: LTE2E_WN_LS).
- test_validate_abap_slt_to_filestore_delta_gen2: Gen 2 Gen 2 Transfer data from ABAP SLT to Filestore with delta load (ABAP Data: LTE2E_WN_LM).
- test_validate_abap_slt_to_filestore_initial_fat_gen2: Gen 2 Gen 2 Transfer data from ABAP SLT to Filestore with initial load (ABAP Data: LTE2E_WF_LS).
- test_validate_abap_slt_to_hana_initial_fat_gen2: Gen 2 Transfer data from ABAP SLT to HANA cloud with initial load (ABAP Data: LTE2E_WF_LS).
- test_validate_abap_slt_to_kafka_delta_gen2: Gen 2 Gen 2 Transfer data from ABAP SLT to Kafka with delta load (ABAP Data: LTE2E_WF_LM).