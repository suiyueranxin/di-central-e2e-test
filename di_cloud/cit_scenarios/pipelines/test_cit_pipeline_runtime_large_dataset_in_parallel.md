## Description
This test suite covers E2E pipelines which transfer large size data from ABAP (CDS view, SLT table) to HANA/File/KAFKA with initial/replication/delta load.

As the pipeline will take long time for the execution of initial load with large datasize, each test case will include 1 start test(for initial load execution) and 1 validate test(validation and detla load excution).

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
1. test_gen1_start_abap_cds_to_hana_initial_fat and test_gen1_validate_abap_cds_to_hana_initial_fat: Gen 1 Transfer data from ABAP CDS to HANA cloud with initial load (ABAP Data: DHE2E_CDS_WF_LM).
2. test_gen1_start_abap_cds_to_filestore_initial_fat and test_gen1_validate_abap_cds_to_filestore_initial_fat: Gen 1 Transfer data from ABAP CDS to Filestore with initial load (ABAP Data: DHE2E_CDS_WF_LM).
3. test_gen1_start_abap_cds_to_kafka_initial_delta and test_gen1_validate_abap_cds_to_kafka_initial_delta: Gen 1 Transfer data from ABAP CDS to KAFKA with initial and delta load (ABAP Data: DHE2E_CDS_WF_LM).
4. test_gen1_start_abap_cds_to_filestore_initial_delta and test_gen1_validate_abap_cds_to_filestore_initial_delta: Gen 1 Transfer data from ABAP CDS to Filestore with initial and delta load (ABAP Data: DHE2E_CDS_WN_LM).
### SLT Gen 1
1. test_gen1_start_abap_slt_to_filestore_initial_fat and 
test_gen1_validate_abap_slt_to_filestore_initial_fat:Gen 1 Transfer data from ABAP SLT to Filestore with initial load(ABAP Data:LTE2E_WF_LM)
2. test_gen1_start_abap_slt_to_kafka_initial_delta and
test_gen1_validate_abap_slt_to_kafka_initial_delta:Gen 1 Transfer data from ABAP SLT to KAFKA with initial and delta load(ABAP Data:LTE2E_WS_LM)
3. test_gen1_start_abap_slt_to_filestore_initial_delta and
test_gen1_validate_abap_slt_to_filestore_initial_delta:Gen 1 Transfer data from ABAP SLT to Filestore with initial and delta load(ABAP Data:LTE2E_WN_LM)
### SLT Gen 2
- test_gen2_start_abap_slt_to_filestore_initial_delta / test_gen2_validate_abap_slt_to_filestore_initial_delta: Gen 2 Transfer data from ABAP SLT to Filestore with initial and delta load (ABAP Data: LTE2E_WS_LM).