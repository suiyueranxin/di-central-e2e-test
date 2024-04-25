## Description
This test suite covers E2E pipelines which transfer small size data from ABAP (CDS view, SLT table) to HANA/File/KAFKA with initial/replication/delta load.

## Operators
1. Generation 1
    - ABAP: ABAP CDS Reader V1, ABAP Converter, SAP ABAP Operator (Eraser), SLT Connector
    - HANA: SAP HANA Client
    - KAFKA: Kafka Producer, Kafka Consumer
    - File: Write File
    - Custom Operator: ABAP Message Controller (Python)
2. Generation 2
    - ABAP: Read Data From SAP System
    - HANA: Write HANA Table
    - KAFKA: Kafka Producer, Kafka Consumer
    - File: Binary File Producer
    - Utinities: Table to Binary
    - Processing: Python3 Operator

## Tests
1. test_gen1_abap_cds_to_hana_initial: Transfer data from ABAP CDS to HANA cloud with initial load (ABAP Data: DHE2E_CDS_WN_LM).
2. test_gen1_abap_cds_to_hana_delta: Transfer data from ABAP CDS to HANA cloud with delta load (ABAP Data: DHE2E_CDS_WN_LM). 
3. test_gen1_abap_cds_to_hana_replication: Transfer data from ABAP CDS to HANA cloud with delta load (ABAP Data: DHE2E_CDS_WN_LM). 
4. test_gen1_abap_cds_to_kafka_initial: Transfer data from ABAP CDS to KAFKA with initial load (ABAP Data: DHE2E_CDS_WN_LM). 
5. test_gen1_abap_slt_to_hana_initial: Transfer data from ABAP CDS to HANA cloud with initial load (ABAP Data: LTE2E_WN_LM).
6. test_gen1_abap_slt_to_hana_delta: Transfer data from ABAP CDS to HANA cloud with delta load (ABAP Data: LTE2E_WN_LM). 
7. test_gen1_abap_slt_to_hana_replication: Transfer data from ABAP CDS to HANA cloud with delta load (ABAP Data: LTE2E_WN_LM). 
8. test_gen1_abap_slt_to_kafka_initial: Transfer data from ABAP CDS to KAFKA with initial load (ABAP Data: LTE2E_WN_LM). 
9. test_gen2_abap_cds_to_hana_initial: Transfer data from ABAP CDS to HANA cloud with initial load (ABAP Data: DHE2E_CDS_WN_LM).
10. test_gen2_abap_cds_to_hana_delta: Transfer data from ABAP CDS to HANA cloud with delta load (ABAP Data: DHE2E_CDS_WN_LM). 
11. test_gen2_abap_cds_to_hana_replication: Transfer data from ABAP CDS to HANA cloud with delta load (ABAP Data: DHE2E_CDS_WN_LM).
12. test_gen2_abap_cds_to_kafka_initial: Transfer data from ABAP CDS to KAFKA with initial load (ABAP Data: DHE2E_CDS_WN_LM).
13. test_gen2_abap_slt_to_hana_initial: Transfer data from ABAP CDS to HANA cloud with initial load (ABAP Data: LTE2E_WN_LM).
14. test_gen2_abap_slt_to_hana_delta: Transfer data from ABAP CDS to HANA cloud with delta load (ABAP Data: LTE2E_WN_LM). 
15. test_gen2_abap_slt_to_hana_replication: Transfer data from ABAP CDS to HANA cloud with delta load (ABAP Data: LTE2E_WN_LM). 
16. test_gen2_abap_slt_to_kafka_initial: Transfer data from ABAP CDS to KAFKA with initial load (ABAP Data: LTE2E_WN_LM). 