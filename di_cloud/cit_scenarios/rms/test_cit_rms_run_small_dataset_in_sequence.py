from unittest.case import skip
import time

from di_qa_e2e.rms import ChangerequeststatusStatus, Rms
from di_qa_e2e.models.replication import ReplicationLoadtype, ReplicationSpaceFileCompression, ReplicationSpaceFileType, ReplicationSpaceGroupDeltaBy
from cit_rms_base import CIT_RMS_base
from di_qa_e2e_validation.datalake.DatalakeClient import DatalakeClient


class testCIT_RMS_run(CIT_RMS_base):

    # ABAP CDS to HC
    def test_abap_cds_to_hc_small(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-hc-small'
        self.execute_to_init_done(replication_name)

        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name)

    def test_abap_cds_to_hc_deltaSmall(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-hc-deltaSmall'
        self.execute_to_init_done(replication_name)

        self.check_and_query_status(replication_name, expected_status = ['DELTA_RUNNING'])
        self.validate_and_query_replication(replication_name)

        self.execute_delta_for_hana(replication_name)

    # ABAP SLT to HC
    def test_abap_slt_to_hc_small(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-hc-small'
        self.execute_to_init_done(replication_name)

        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name)

    def test_abap_slt_to_hc_deltaSmall(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-hc-deltaSmall'
        self.execute_to_init_done(replication_name)

        self.check_and_query_status(replication_name, expected_status = ['DELTA_RUNNING'])
        self.validate_and_query_replication(replication_name)

        self.execute_delta_for_hana(replication_name)

    # ABAP CDS to ADL2
    def test_abap_cds_to_adlv2_small(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-adlv2-small'
        self.execute_to_init_done(replication_name)

        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name, folder_mode = 'initial')

    def test_abap_cds_to_adlv2_deltaSmall(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-adlv2-deltaSmall'
        self.execute_to_init_done(replication_name)

        self.check_and_query_status(replication_name, expected_status = ['DELTA_RUNNING'])
        self.validate_and_query_replication(replication_name, folder_mode = 'initial')

        self.execute_delta_for_datalake(replication_name)

    # ABAP SLT to ADL2
    def test_abap_slt_to_adlv2_small(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-adlv2-small'
        self.execute_to_init_done(replication_name)

        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name, folder_mode = 'initial')

    def test_abap_slt_to_adlv2_deltaSmall(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-adlv2-deltaSmall'
        self.execute_to_init_done(replication_name)

        self.check_and_query_status(replication_name, expected_status = ['DELTA_RUNNING'])
        self.validate_and_query_replication(replication_name, folder_mode = 'initial')

        self.execute_delta_for_datalake(replication_name)

    # ABAP ODP to FILE
    def test_abap_odp_to_adlv2_small_csv(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-small-csv'
        self.execute_to_init_done(replication_name)

        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name, folder_mode = 'initial', filetype = ReplicationSpaceFileType.CSV, header_index = None)

    def test_abap_odp_to_adlv2_small_json(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-small-json'
        self.execute_to_init_done(replication_name)

        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name, folder_mode = 'initial', filetype = ReplicationSpaceFileType.JSON)

    def test_abap_odp_to_adlv2_small_jsonlines(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-small-jlines'
        self.execute_to_init_done(replication_name)

        self.check_and_query_status(replication_name)
        self.validate_and_query_replication(replication_name, folder_mode = 'initial', filetype = ReplicationSpaceFileType.JSONLINES)