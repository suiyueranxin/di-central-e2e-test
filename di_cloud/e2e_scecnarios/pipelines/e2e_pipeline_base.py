import unittest
import os

from di_qa_e2e.cluster import Cluster
from di_qa_e2e.connections.connection_data import ConnectionData
from di_qa_e2e_validation.abap.CitAbapClient import CitAbapClient
from di_qa_e2e_validation.hana.HanaClient import HanaClient
from di_qa_e2e_validation.datalake.DatalakeClient import DatalakeClient

test_path = os.path.join(os.path.dirname(__file__), '../../connection_data')


class E2E_Pipeline_base(unittest.TestCase):
    """This is the base class for the automated E2E tests of the CET Pipeline
    scenarios."""

    # Pipeline Prefix

    PIPELINE_PREFIX = 'E2E-PIPELINE-'

    # Target Systems

    HANA = 'CIT_HANA'

    GRAPH_NAME = {
        'gen1-cds-to-hana-initial': PIPELINE_PREFIX + 'gen1-cds-to-hana-initial',
        'gen1-cds-to-hana-delta': PIPELINE_PREFIX + 'gen1-cds-to-hana-delta',
        'gen1-cds-to-hana-replication': PIPELINE_PREFIX + 'gen1-cds-to-hana-replication',
        'gen1-cds-to-kafka-initial': PIPELINE_PREFIX + 'gen1-cds-to-kafka-initial',
        'gen1-kafka-consumer-check-data': PIPELINE_PREFIX + 'gen1-kafka-consumer-check-data',
        'gen1-slt-to-hana-initial': PIPELINE_PREFIX + 'gen1-slt-to-hana-initial',
        'gen1-slt-to-hana-delta': PIPELINE_PREFIX + 'gen1-slt-to-hana-delta',
        'gen1-slt-to-hana-replication': PIPELINE_PREFIX + 'gen1-slt-to-hana-replication',
        'gen1-slt-to-kafka-initial': PIPELINE_PREFIX + 'gen1-slt-to-kafka-initial',
        'gen2-cds-to-hana-initial': PIPELINE_PREFIX + 'gen2-cds-to-hana-initial',
        'gen2-cds-to-hana-delta': PIPELINE_PREFIX + 'gen2-cds-to-hana-delta',
        'gen2-cds-to-hana-replication': PIPELINE_PREFIX + 'gen2-cds-to-hana-replication',
        'gen2-cds-to-kafka-initial': PIPELINE_PREFIX + 'gen2-cds-to-kafka-initial',
        'gen2-kafka-consumer-check-data': PIPELINE_PREFIX + 'gen2-kafka-consumer-check-data',
        'gen2-slt-to-hana-initial': PIPELINE_PREFIX + 'gen2-slt-to-hana-initial',
        'gen2-slt-to-hana-delta': PIPELINE_PREFIX + 'gen2-slt-to-hana-delta',
        'gen2-slt-to-hana-replication': PIPELINE_PREFIX + 'gen2-slt-to-hana-replication',
        'gen2-slt-to-kafka-initial': PIPELINE_PREFIX + 'gen2-slt-to-kafka-initial',
    }
    # Cluster
    CLUSTER = "CET"

    # Pipeline Target HANA Schema

    PIPELINE_TARGET_HANA_SCHEMA = 'CET_TEST'

    HANA = 'CIT_HANA'

    ABAP_S4 = "S4H_2021"

    ABAP_DMIS = "DMIS_2018"

    CIT_FILE = "CIT_FILE"

    cluster: Cluster = None  # type: ignore

    hana: HanaClient = None  # type: ignore

    datalake: DatalakeClient = None  # type: ignore

    abap_s4: CitAbapClient = None  # type: ignore

    abap_dmis: CitAbapClient = None  # type: ignore

    cit_file: DatalakeClient = None   # type: ignore

    subscription_objects = None

    @classmethod
    def get_cluster_connection_data(cls):
        connection_data = ConnectionData.for_cluster(cls.CLUSTER, test_path)
        return connection_data

    @classmethod
    def get_abap_s4_connection_data(cls):
        connection_data = ConnectionData.for_abap(cls.ABAP_S4, test_path)
        return connection_data

    @classmethod
    def get_abap_dmis_connection_data(cls):
        connection_data = ConnectionData.for_abap(cls.ABAP_DMIS, test_path)
        return connection_data

    @classmethod
    def get_hana_connection_data(cls):
        connection_data = ConnectionData.for_hana(cls.HANA, test_path)
        return connection_data

    @classmethod
    def get_cit_file_connection_data(cls):
        connection_data = ConnectionData.for_datalake(cls.CIT_FILE, test_path)
        return connection_data

    @classmethod
    def reconnect_to_abap_s4_connection(cls):
        connection_data = cls.get_abap_s4_connection_data()
        cls.abap_s4 = CitAbapClient.connect_to(connection_data)

    """
    Get a list of the subscription objects from a ABAP system.
    """
    @classmethod
    def get_subscription_objects(cls, abap_connection_id: str) -> list[dict]:
        url = f"/app/axino-service/ape/v1/{abap_connection_id}/operator/com.sap.abap.subscr.eraser/subscription"
        response = cls.cluster.api_get(url)
        subscription_objects = response.json()
        return subscription_objects

    @classmethod
    def erase_subscription_name(cls, abap_connection_id: str, subscription_name: str):
        """
        Sometimes the subscription name is not erased due to the fact that
        there is graph with delta/replication loading, which will result in
        the graph failure for the next execution, so we need to erase these 
        subscription names before starting the test cases.
        """
        id = ""
        subscription_objects = cls.subscription_objects
        if subscription_objects:
            for subscription_object in subscription_objects:
                if subscription_name == subscription_object["Subscription Name"]:
                    id = subscription_object["Subscription ID"]
                    break
            if id:
                graph = cls.cluster.modeler.graphs
                execution = graph.run_graph('com.sap.abap-int-tools.Erase_SUBSCRIBER_ID', f"Erase SN {subscription_name}", {
                               "subscription_name": id, "abap_sys_id": abap_connection_id})
                execution.wait_until_completed()
            else:
                print(
                    f"Could not find id for the specified subscription name {subscription_name}")

    @classmethod
    def get_all_subscription_objects(cls):
        S4_subscription_objects = cls.get_subscription_objects("CIT_S4")
        DMIS_subscription_objects = cls.get_subscription_objects("CIT_DMIS")
        if S4_subscription_objects and DMIS_subscription_objects:
            cls.subscription_objects = S4_subscription_objects + DMIS_subscription_objects
        elif S4_subscription_objects:
            cls.subscription_objects = S4_subscription_objects
        elif DMIS_subscription_objects:
            cls.subscription_objects = DMIS_subscription_objects
        else:
            cls.subscription_objects = []

    def get_row_count_from_hana(self, hana_table_name):
        hana_rowcount = self.hana.get_rowcount(
            self.PIPELINE_TARGET_HANA_SCHEMA, hana_table_name)
        return hana_rowcount

    def get_row_count_from_adl(self, path, substring='', is_row_count_same=True):
        """ Calculate the total row count of files under a specified folder.

        Parameters
        ----------
        path: str
            The directory of the files located.

        substring: str
            The contained string of the files to be filtered and calculated.

        is_row_count_same: boolean
            The identifier to indicate if the files contain same row count or not.
            If set TRUE, will use convenient way to calcute the total row count: row count of each file * files count.
            If set FALSE, will caculate the total row count by add the row count of each file.
        """
        files = self.cit_file.get_fileNames(path, substring)
        filescount = len(files)
        rowcount = 0
        if filescount != 0:
            if is_row_count_same:
                if '.json' in files[0]:
                    rowcount = len(self.cit_file.get_jsonfile_content(path, files[0]))*(filescount-1) \
                        + len(self.cit_file.get_jsonfile_content(path,
                                                                 files[filescount-1]))
                else:
                    rowcount = self.cit_file.get_csv_file_rowcount(path, files[0])*(filescount-1) \
                        + self.cit_file.get_csv_file_rowcount(path, files[filescount-1])
            else:
                if '.json' in files[0]:
                    for file in files:
                        rowcount += len(self.cit_file.get_jsonfile_content(path, file))
                else:
                    for file in files:
                        rowcount += self.cit_file.get_csv_file_rowcount(
                            path, file)

        return rowcount

    def validate_total_count_from_abap_s4_to_hana(self, abap_table_id, hana_table_name):
        abap_rowcount = self.abap_s4.get_rowcount(abap_table_id)
        hana_rowcount = self.hana.get_rowcount(
            self.PIPELINE_TARGET_HANA_SCHEMA, hana_table_name)
        self.assertEqual(abap_rowcount, hana_rowcount,
                         f"The record number should be the same between ABAP S4 source {abap_table_id} and hana target table {hana_table_name}")

    def validate_total_count_from_abap_dmis_to_hana(self, abap_table_id, hana_table_name):
        abap_rowcount = self.abap_dmis.get_rowcount(abap_table_id)
        hana_rowcount = self.hana.get_rowcount(
            self.PIPELINE_TARGET_HANA_SCHEMA, hana_table_name)
        self.assertEqual(abap_rowcount, hana_rowcount,
                         f"The record number should be the same between ABAP DMIS source {abap_table_id} and hana target table {hana_table_name}")

    def validate_row_count_in_hana(self, hana_table_name, expect_count):
        hana_rowcount = self.get_row_count_from_hana(hana_table_name)
        self.assertEqual(hana_rowcount, expect_count,
                         f"The record number in HANA table {hana_table_name} should be {expect_count}.")

    def validate_total_count_from_adap_s4_to_filestore(self, abap_table_id, path, substring='', is_row_count_same=True):
        abap_rowcount = self.abap_s4.get_rowcount(abap_table_id)
        rowcount = self.get_row_count_from_adl(
            path, substring, is_row_count_same)
        self.assertEqual(abap_rowcount, rowcount,
                         f"The record number should be the same between ABAP S4 source {abap_table_id} and ADL_V2 target files {path}")

    def validate_total_count_from_adap_dmis_to_filestore(self, abap_table_id, path, substring='', is_row_count_same=True):
        abap_rowcount = self.abap_dmis.get_rowcount(abap_table_id)
        rowcount = self.get_row_count_from_adl(
            path, substring, is_row_count_same)
        self.assertEqual(abap_rowcount, rowcount,
                         f"The record number should be the same between ABAP DMIS source {abap_table_id} and ADL_V2 target files {path}")

    @classmethod
    def setUpClass(cls):
        if cls.cluster is None:
            connection_data = cls.get_cluster_connection_data()
            cls.cluster = Cluster.connect_to(connection_data)

        if cls.hana is None:
            connection_data = cls.get_hana_connection_data()
            cls.hana = HanaClient.connect_to(connection_data)

        if cls.abap_s4 is None:
            connection_data = cls.get_abap_s4_connection_data()
            cls.abap_s4 = CitAbapClient.connect_to(connection_data)

        if cls.abap_dmis is None:
            connection_data = cls.get_abap_dmis_connection_data()
            cls.abap_dmis = CitAbapClient.connect_to(connection_data)

        if cls.cit_file is None:
            connection_data = cls.get_cit_file_connection_data()
            cls.cit_file = DatalakeClient.connect_to(connection_data)

        cls.assertIsNotNone(cls.cluster, 'Connection to cluster failed!')
        cls.get_all_subscription_objects()
        # FIXME
        cls.cluster.modeler.graphs.archiveAll()

    @classmethod
    def tearDownClass(cls):
        cls.cluster = None  # type: ignore
