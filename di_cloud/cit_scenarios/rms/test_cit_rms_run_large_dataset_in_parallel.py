from unittest.case import skip
import time

from di_qa_e2e.rms import ChangerequeststatusStatus, Rms
from di_qa_e2e.models.replication import ReplicationLoadtype, ReplicationSpaceFileCompression, ReplicationSpaceFileType, ReplicationSpaceGroupDeltaBy
from cit_rms_base import CIT_RMS_base


class testCIT_RMS_run(CIT_RMS_base):

    # ABAP CDS to HC
    def test_run_abap_cds_to_hc_fat(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-hc-fat'
        self.execute_to_init_done(replication_name)

    def test_validate_abap_cds_to_hc_fat(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-hc-fat'
        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name)

    # ABAP SLT to HC
    def test_run_abap_slt_to_hc_fat(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-hc-fat'
        self.execute_to_init_done(replication_name)

    def test_validate_abap_slt_to_hc_fat(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-hc-fat'
        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name)

    # ABAP CDS to ADL2
    def test_run_abap_cds_to_adlv2_fat(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-adlv2-fat'
        self.execute_to_init_done(replication_name)

    def test_validate_abap_cds_to_adlv2_fat(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-adlv2-fat'
        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name, folder_mode = 'initial')

    # ABAP SLT to ADL2
    def test_run_abap_slt_to_adlv2_fat(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-adlv2-fat'
        self.execute_to_init_done(replication_name)

    def test_validate_abap_slt_to_adlv2_fat(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-adlv2-fat'
        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name, folder_mode = 'initial')

    # ABAP ODP to ADL2
    def test_run_abap_odp_to_adlv2_fat_csv(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-fat-csv'
        self.execute_to_init_done(replication_name)

    def test_validate_abap_odp_to_adlv2_fat_csv(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-fat-csv'
        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name, folder_mode = 'initial', filetype = ReplicationSpaceFileType.CSV, header_index = None)

    # ABAP ODP to ADL2
    def test_run_abap_odp_to_adlv2_fat_json(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-fat-json'
        self.execute_to_init_done(replication_name)

    def test_validate_abap_odp_to_adlv2_fat_json(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-fat-json'
        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name, folder_mode = 'initial', filetype = ReplicationSpaceFileType.JSON)

    # ABAP ODP to ADL2
    def test_run_abap_odp_to_adlv2_fat_jlines(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-fat-jlines'
        self.execute_to_init_done(replication_name)

    def test_validate_abap_odp_to_adlv2_fat_jlines(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-fat-jlines'
        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name, folder_mode = 'initial', filetype = ReplicationSpaceFileType.JSONLINES)