from unittest.case import skip

from di_qa_e2e.rms import ChangerequeststatusStatus, Rms
from di_qa_e2e.models.replication import ReplicationLoadtype, ReplicationSpaceFileCompression, ReplicationSpaceFileType, ReplicationSpaceGroupDeltaBy, ReplicationSpaceFileDelimiter, ReplicationSpaceFileCompression, ReplicationSpaceFileOrient
from cit_rms_base import CIT_RMS_base


class testCIT_RMS_design(CIT_RMS_base):

    # ABAP CDS to HC
    def test_abap_cds_to_hc_small(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-hc-small'
        self.clear_replication(replication_name)
        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description('Replication of Small CDS View to HANA DB')

        replication.set_sourcespace(
            self.ABAP_CDS_HC, self.ABAP_CDS_HC_CONTAINER)
        replication.set_targetspace(self.HANA_TARGET, self.HANA_CONTAINER)
        task = replication.create_task('DHE2E_CDS_WS_LS_0')

        replication.save()

    def test_abap_cds_to_hc_fat(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-hc-fat'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description('Replication of Fat CDS View to HANA DB')

        replication.set_sourcespace(
            self.ABAP_CDS_HC, self.ABAP_CDS_HC_CONTAINER)
        replication.set_targetspace(self.HANA_TARGET, self.HANA_CONTAINER)
        replication.create_task('DHE2E_CDS_WF_LM')

        replication.save()

    def test_abap_cds_to_hc_deltaSmall(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-hc-deltaSmall'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description(
            'Replication of Small CDS View to HANA DB (incl. delta)')

        replication.set_sourcespace(
            self.ABAP_CDS_HC, self.ABAP_CDS_HC_CONTAINER)
        replication.set_targetspace(self.HANA_TARGET, self.HANA_CONTAINER)
        task = replication.create_task('DHE2E_CDS_WN_LS_0')
        task.set_loadtype(ReplicationLoadtype.INITIAL_AND_DELTA)

        replication.save()

    # ABAP SLT to HC
    def test_abap_slt_to_hc_small(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-hc-small'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description(
            'Replication of Table LTE2E_WS_LS to HANA DB')

        replication.set_sourcespace(
            self.ABAP_SLT_HC, self.ABAP_SLT_HC_CONTAINER)
        replication.set_targetspace(self.HANA_TARGET, self.HANA_CONTAINER)
        replication.create_task('LTE2E_WS_LS')

        replication.save()

    def test_abap_slt_to_hc_fat(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-hc-fat'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description(
            'Replication of Table LTE2E_WF_LM to HANA DB')

        replication.set_sourcespace(
            self.ABAP_SLT_HC, self.ABAP_SLT_HC_CONTAINER)
        replication.set_targetspace(self.HANA_TARGET, self.HANA_CONTAINER)
        replication.create_task('LTE2E_WF_LM')

        replication.save()

    def test_abap_slt_to_hc_deltaSmall(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-hc-deltaSmall'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description(
            'Replication of Table LTE2E_WS_LS to HANA DB (incl. delta)')

        replication.set_sourcespace(
            self.ABAP_SLT_HC, self.ABAP_SLT_HC_CONTAINER)
        replication.set_targetspace(self.HANA_TARGET, self.HANA_CONTAINER)
        task = replication.create_task('LTE2E_WN_LS')
        task.set_loadtype(ReplicationLoadtype.INITIAL_AND_DELTA)

        replication.save()

    # ABAP CDS to ADL2
    def test_abap_cds_to_adlv2_small(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-adlv2-small'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description('Replication of Small CDS View to ADLv2')

        replication.set_sourcespace(
            self.ABAP_CDS_ADL, self.ABAP_CDS_ADL_CONTAINER)
        targetspace = replication.set_targetspace(
            self.ADLV2_TARGET, self.ADLV2_CONTAINER)
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)
        replication.create_task('DHE2E_CDS_WS_LS_1')

        replication.save()

    def test_abap_cds_to_adlv2_fat(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-adlv2-fat'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description('Replication of fat CDS View to ADLv2')

        replication.set_sourcespace(
            self.ABAP_CDS_ADL, self.ABAP_CDS_ADL_CONTAINER)
        targetspace = replication.set_targetspace(
            self.ADLV2_TARGET, self.ADLV2_CONTAINER)
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)
        replication.create_task('DHE2E_CDS_WF_LL_1')

        replication.save()

    def test_abap_cds_to_adlv2_deltaSmall(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-cds-to-adlv2-deltaSmall'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description(
            'Replication of small CDS View to ADLv2 (incl. delta)')

        replication.set_sourcespace(
            self.ABAP_CDS_ADL, self.ABAP_CDS_ADL_CONTAINER)
        targetspace = replication.set_targetspace(
            self.ADLV2_TARGET, self.ADLV2_CONTAINER)
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)

        task = replication.create_task('DHE2E_CDS_WN_LS_1')
        task.set_loadtype(ReplicationLoadtype.INITIAL_AND_DELTA)

        replication.save()

    # ABAP SLT to ADL2
    def test_abap_slt_to_adlv2_small(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-adlv2-small'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description(
            'Replication of Table LTE2E_WS_LS to ADLv2')

        replication.set_sourcespace(
            self.ABAP_SLT_ADL, self.ABAP_SLT_ADL_CONTAINER)
        targetspace = replication.set_targetspace(
            self.ADLV2_TARGET, self.ADLV2_CONTAINER)
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)

        replication.create_task('LTE2E_WS_LS')

        replication.save()

    def test_abap_slt_to_adlv2_fat(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-adlv2-fat'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description(
            'Replication of Table LTE2E_WL_LF to ADLv2')

        replication.set_sourcespace(
            self.ABAP_SLT_ADL, self.ABAP_SLT_ADL_CONTAINER)
        targetspace = replication.set_targetspace(
            self.ADLV2_TARGET, self.ADLV2_CONTAINER)
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)
        replication.create_task('LTE2E_WF_LL')

        replication.save()

    def test_abap_slt_to_adlv2_deltaSmall(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-slt-to-adlv2-deltaSmall'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description(
            'Replication of Table LTE2E_WS_LS to ADLv2')

        replication.set_sourcespace(
            self.ABAP_SLT_ADL, self.ABAP_SLT_ADL_CONTAINER)
        targetspace = replication.set_targetspace(
            self.ADLV2_TARGET, self.ADLV2_CONTAINER)
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.PARQUET)
        targetspace.set_file_compression(ReplicationSpaceFileCompression.NONE)
        task = replication.create_task('LTE2E_WN_LS')
        task.set_loadtype(ReplicationLoadtype.INITIAL_AND_DELTA)

        replication.save()

    def test_abap_odp_to_adlv2_small_csv(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-small-csv'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description('ODP to Filestore - Initial Load - Small - csv')

        replication.set_sourcespace(
            self.ABAP_ODP_ADL, self.ABAP_ODP_ADL_CONTAINER)
        targetspace = replication.set_targetspace(
            self.ADLV2_TARGET, self.ADLV2_ODP_CONTAINER + '/' + ReplicationSpaceFileType.CSV.value)
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.CSV)
        targetspace.set_file_delimiter(ReplicationSpaceFileDelimiter.COMMA)
        targetspace.set_file_header(False)
        replication.create_task('LTE2E_WS_LS')

        replication.save()

    def test_abap_odp_to_adlv2_fat_csv(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-fat-csv'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description('ODP to Filestore - Initial Load - Fat - csv')

        replication.set_sourcespace(
            self.ABAP_ODP_ADL, self.ABAP_ODP_ADL_CONTAINER)
        targetspace = replication.set_targetspace(
            self.ADLV2_TARGET, self.ADLV2_ODP_CONTAINER + '/' + ReplicationSpaceFileType.CSV.value)
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.CSV)
        targetspace.set_file_delimiter(ReplicationSpaceFileDelimiter.COMMA)
        targetspace.set_file_header(False)
        replication.create_task('LTE2E_WS_LL')

        replication.save()

    def test_abap_odp_to_adlv2_small_json(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-small-json'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description('ODP to Filestore - Initial Load - Small - json')

        replication.set_sourcespace(
            self.ABAP_ODP_ADL, self.ABAP_ODP_ADL_CONTAINER)
        targetspace = replication.set_targetspace(
            self.ADLV2_TARGET, self.ADLV2_ODP_CONTAINER + '/' + ReplicationSpaceFileType.JSON.value)
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.JSON)
        targetspace.set_file_orient(ReplicationSpaceFileOrient.RECORDS)
        replication.create_task('LTE2E_WS_LS')

        replication.save()

    def test_abap_odp_to_adlv2_fat_json(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-fat-json'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description('ODP to Filestore - Initial Load - Fat - json')

        replication.set_sourcespace(
            self.ABAP_ODP_ADL, self.ABAP_ODP_ADL_CONTAINER)
        targetspace = replication.set_targetspace(
            self.ADLV2_TARGET, self.ADLV2_ODP_CONTAINER + '/' + ReplicationSpaceFileType.JSON.value)
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.JSON)
        targetspace.set_file_orient(ReplicationSpaceFileOrient.RECORDS)
        replication.create_task('LTE2E_WS_LL')

        replication.save()

    def test_abap_odp_to_adlv2_small_jlines(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-small-jlines'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description('ODP to Filestore - Initial Load - Small - jsonlines')

        replication.set_sourcespace(
            self.ABAP_ODP_ADL, self.ABAP_ODP_ADL_CONTAINER)
        targetspace = replication.set_targetspace(
            self.ADLV2_TARGET, self.ADLV2_ODP_CONTAINER + '/' + ReplicationSpaceFileType.JSONLINES.value)
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.JSONLINES)
        replication.create_task('LTE2E_WS_LS')

        replication.save()

    def test_abap_odp_to_adlv2_fat_jlines(self):
        replication_name = self.REPLICATION_PREFIX + 'abap-odp-to-adlv2-fat-jlines'

        self.clear_replication(replication_name)

        replication = self.cluster.modeler.replications.create_replication(
            replication_name)

        replication.set_description('ODP to Filestore - Initial Load - Fat - jsonlines')

        replication.set_sourcespace(
            self.ABAP_ODP_ADL, self.ABAP_ODP_ADL_CONTAINER)
        targetspace = replication.set_targetspace(
            self.ADLV2_TARGET, self.ADLV2_ODP_CONTAINER + '/' + ReplicationSpaceFileType.JSONLINES.value)
        targetspace.set_groupdeltaby(ReplicationSpaceGroupDeltaBy.DATE)
        targetspace.set_file_type(ReplicationSpaceFileType.JSONLINES)
        replication.create_task('LTE2E_WS_LL')

        replication.save()