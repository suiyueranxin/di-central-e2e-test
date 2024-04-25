import os
import unittest
import time
from unittest.case import skip

from azure.core.exceptions import ResourceNotFoundError

from di_qa_e2e.cluster import Cluster
from di_qa_e2e.rms import ChangerequeststatusStatus
from di_qa_e2e.connections.connection_data import ConnectionData
from di_qa_e2e_validation.abap.CitAbapClient import CitAbapClient
from di_qa_e2e_validation.datalake.DatalakeClient import DatalakeClient
from di_qa_e2e_validation.hana.HanaClient import HanaClient
from di_qa_e2e.models.replication import ReplicationSpaceFileType


class CIT_RMS_base(unittest.TestCase):
    test_path = os.path.join(os.path.dirname(__file__), '../../connection_data')

    """This is the base class for the automated E2E tests of the CIT RMS
    scenarios."""

    # Replication Prefix
    REPLICATION_PREFIX = 'CIT-RMS-'

    # Cluster
    CLUSTER = "CET_DI"
    CLUSTER_VERSION = "2202.09"

    MTID_SELECTOR = {'CIT_DMIS': {
        'CET': {'HC': '00W', 'ADL': '00V'}}}

    # Source Systems
    ABAP_S4 = "CET_S4H_2021"

    ABAP_DMIS = "CET_DMIS_2018"

    ABAP_CDS_HC = 'CIT_S4'
    ABAP_CDS_HC_CONTAINER = '/CDS'

    ABAP_CDS_ADL = 'CIT_S4'
    ABAP_CDS_ADL_CONTAINER = '/CDS'

    ABAP_SLT_HC = 'CIT_DMIS'
    ABAP_SLT_HC_CONTAINER = f'/SLT/{MTID_SELECTOR[ABAP_SLT_HC]["CET"]["HC"]}'

    ABAP_SLT_ADL = 'CIT_DMIS'
    ABAP_SLT_ADL_CONTAINER = f'/SLT/{MTID_SELECTOR[ABAP_SLT_ADL]["CET"]["ADL"]}'

    ABAP_ODP_ADL = 'CIT_DMIS_ODP'
    ABAP_ODP_ADL_CONTAINER = '/ODP_SAPI'

    # Target Systems
    HANA = 'CET_HANA_OC'
    HANA_TARGET = 'CIT_HANA'
    HANA_CONTAINER = f'/CET_TEST'

    ADLV2 = 'CET_FILE'
    ADLV2_TARGET = 'CIT_FILE'
    ADLV2_CONTAINER = f'/CET_TEST'


    ADLV2_ODP_CONTAINER = f'/CET_TEST/ODPExtraction'

    cluster = None
    hana = None
    datalake = None
    cds_abap = None
    dmis_abap = None

    @classmethod
    def get_cluster_connection_data(self):
        url = os.environ.get("url")
        if url is None or len(url) == 0: # read cluster config from file if not set by env
            connection_data = ConnectionData.for_cluster(self.CLUSTER, self.test_path)
        else: # read cluster config from env
            os.environ.setdefault(f"{self.CLUSTER}_BASEURL", url)
            os.environ.setdefault(f"{self.CLUSTER}_TENANT", os.environ.get("tenant"))
            connection_data = ConnectionData.for_cluster(self.CLUSTER)

        return connection_data

    @classmethod
    def get_hana_connection_data(self):
        connection_data = ConnectionData.for_hana(self.HANA, self.test_path)
        return connection_data

    @classmethod
    def get_datalake_connection_data(self):
        connection_data = ConnectionData.for_datalake(self.ADLV2, self.test_path)
        return connection_data

    @classmethod
    def get_abap_s4_connection_data(self):
        connection_data = ConnectionData.for_abap(self.ABAP_S4, self.test_path)
        return connection_data

    @classmethod
    def get_abap_dmis_connection_data(self):
        connection_data = ConnectionData.for_abap(self.ABAP_DMIS, self.test_path)
        return connection_data

    @classmethod
    def setUpClass(self) -> None:
        # Overwirte CLUSTER from environment if set
        cluster_from_env = os.environ.get('cluster', None)
        if cluster_from_env is not None:
            self.CLUSTER = cluster_from_env

        if self.cluster is None:
            connection_data = self.get_cluster_connection_data()
            self.cluster = Cluster.connect_to(connection_data)

        if self.hana is None:
            connection_data = self.get_hana_connection_data()
            self.hana = HanaClient.connect_to(connection_data)

        if self.datalake is None:
            connection_data = self.get_datalake_connection_data()
            self.datalake = DatalakeClient.connect_to(connection_data)
        
        if self.cds_abap is None:
            connection_data = self.get_abap_s4_connection_data()
            self.cds_abap = CitAbapClient.connect_to(connection_data)

        if self.dmis_abap is None:
            connection_data = self.get_abap_dmis_connection_data()
            self.dmis_abap = CitAbapClient.connect_to(connection_data)

    def setUp(self) -> None:
        self.assertIsNotNone(self.cluster, 'Connection to cluster failed!')
        self.assertIsNotNone(self.hana, 'Connection to HANA database failed!')
        self.assertIsNotNone(
            self.datalake, 'Connection to Azure Datalake failed!')

        print(20 * '-' + " test execution " + self._testMethodName + ' begin. ' + 20 * '-')

    def tearDown(self) -> None:
        print(20 * '-' + " test execution " + self._testMethodName + ' end. ' + 20 * '-')

    def _is_replicationflow_completed(self, replicationname: str) -> bool:
        """Checks whether all tasks of the replicationflow for the replication
        with the given name are completed.

        If no matching replicationflow is found this function still returns True
        """

        monitor = self.cluster.monitoring.replications.get_monitor(
            replicationname)

        if monitor is None:
            return True

        taskmetrics = monitor.taskmetrics
        if taskmetrics == None:
            return False

        return taskmetrics.completed == taskmetrics.total

    def clear_replication(self, replication_name: str, is_delete_replication = True):
        """
        This makes sure that we are starting with certain well defined
        boundary conditions. Before we clear a certain scenario we need to check
        the runtime state of the replication.

        This is the rough outline:
        - check runtime state of replication flow. if state is not COMPLETED abort.
        - undeploy replication flow.
        - delete replication from repository.
        - drop target table or remove files in target file system.

        """

        # if not self._is_replicationflow_completed(replication_name):
        #     self.skipTest(
        #         'Associated replication flow has not completed yet...')

        replications = self.cluster.modeler.replications
        replication = replications.open_replication(replication_name)
        if replication is not None:
            try:
                replicationflow = replication.undeploy()
            except:
                replicationflow = None

            if replicationflow is not None:
                changerequeststatus = replicationflow.wait_while_busy()
                self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                                 changerequeststatus.status, changerequeststatus)

            if is_delete_replication:
                replications.delete_replication(replication_name)

            connectiontype = replication.targetspace._ccmconnectiontype

            if connectiontype == 'HANA':
                table_name = replication.tasks[0].targetdataset
                schema_name = replication._targetspace._container.lstrip('/')
                try:
                    self.hana.drop_table(schema_name, table_name)
                except:
                    pass

            if connectiontype == 'ADL_GEN2':
                data_directory_name = replication.tasks[0].targetdataset
                root_directory_name = replication._targetspace._container.lstrip(
                    '/')
                directory_name = root_directory_name + '/' + data_directory_name
                self.datalake.delete_directory(directory_name)

    def get_target_type(self, replication_name: str):
        replications = self.cluster.modeler.replications
        replication = replications.open_replication(replication_name)
        if replication is not None:
            target_type = replication.targetspace._ccmconnectiontype

        return target_type

    def get_source_identifier(self, replication_name: str):
        replications = self.cluster.modeler.replications
        replication = replications.open_replication(replication_name)
        if replication is not None:
            source_dataset_name = replication.tasks[0].sourcedataset
            if source_dataset_name.startswith("DHE2E"):
                source_identifier = replication.tasks[0].sourcedataset[-7:-2]
            else:
                source_identifier = replication.tasks[0].sourcedataset[-5:]
        
        return source_identifier

    def get_source_abap(self, replication_name: str):
        replications = self.cluster.modeler.replications
        replication = replications.open_replication(replication_name)
        if replication is not None:
            source_system = replication.sourcespace._connectionid
            if source_system == self.ABAP_CDS_HC: 
                abap = self.cds_abap 
            elif source_system == self.ABAP_SLT_HC or source_system == self.ABAP_ODP_ADL:
                abap = self.dmis_abap
        
        return abap

    def get_hana_table_schema(self, replication_name: str):
        replications = self.cluster.modeler.replications
        replication = replications.open_replication(replication_name)
        table_name = replication.tasks[0].targetdataset
        schema_name = replication._targetspace._container.lstrip('/')
        return table_name, schema_name

    def get_datalake_target(self, replication_name: str, folder_mode = 'initial', search_str ='.parquet'):
        replications = self.cluster.modeler.replications
        replication = replications.open_replication(replication_name)
        data_directory_name = replication.tasks[0].targetdataset
        root_directory_name = replication._targetspace._container.lstrip('/')

        dir_file_name_list = []
        if folder_mode is not None:
            dirctory_name = '/' + root_directory_name + '/' + data_directory_name + '/' + folder_mode
            current_file_list = self.get_all_specific_file_names(dirctory_name, search_str)

            for dir_file in current_file_list:
                # print("Current file is " + str(dir_file))
                raw_filename = dir_file[-1]
                dir_file.remove(dir_file[-1])
                raw_dirname = '/'.join([dirctory_name] + dir_file)
                dir_file_name_list.append([raw_dirname + '/', raw_filename])

            print("All dir_and_file numbers: " + str(len(dir_file_name_list)))

        else:
            self.fail("missing delta/initial mode")

        return dir_file_name_list

    def get_all_specific_file_names(self, directory_name: str, search_str):
        file_list = self.datalake.get_fileNames(directory_name)
        dir_file_list =[]
        for file in file_list:
            if file.find(search_str) > -1:
                if file.find('/') > -1:
                    dir_file_list.append(file.split('/'))
                else:
                    dir_file_list.append([file])

        return dir_file_list

    def get_rowcount_from_task(self, replication_name):
        '''
        Use task count number in monitor directly instead of query from ADV due to the performance issue of large data.
        '''
        rowCount = 0
        taskmonitors = self.cluster.monitoring.replications.get_taskmonitors(replication_name)
        if len(taskmonitors) == 0:
            self.skipTest('No information available!')

        for taskmonitor in taskmonitors:
            rowCount += taskmonitor.numberOfRecordsTransferred

        return rowCount

    def validate_and_query_replication(self, replication_name: str, check_timeout = 120, interval = 10, folder_mode = None, expectedCount = -1, filetype = ReplicationSpaceFileType.PARQUET, header_index = 'infer'):
        """
        This makes sure that the data in the target matches with the data in the
        source. This is done by:

        1. Comparing the number of entries in the target against the number of
           records in the source.

        """
        rtime=0
        allow_greater = False
        target_type = self.get_target_type(replication_name)
        abap = self.get_source_abap(replication_name)
        source_count = expectedCount
        if expectedCount == -1:
            source_identifier = self.get_source_identifier(replication_name)
            source_count = abap.get_rowcount(source_identifier)

        if target_type == 'HANA':
            table_name, schema_name = self.get_hana_table_schema(replication_name)

        while rtime < check_timeout:
            target_count = 0
            if target_type == 'HANA':
                target_count = self.hana.get_rowcount(schema_name, table_name)

            if target_type == 'ADL_GEN2':
                search_str = '.' + filetype.value.lower()
                if filetype == ReplicationSpaceFileType.JSONLINES:
                    search_str = '.jsonl'
                dir_file_name_list = self.get_datalake_target(replication_name, folder_mode, search_str)
                if len(dir_file_name_list) > 100:
                    target_count = self.get_rowcount_from_task(replication_name)
                    allow_greater = True
                else:
                    for dir_file in dir_file_name_list:
                        if filetype == ReplicationSpaceFileType.PARQUET:
                            single_file_count = self.datalake.get_rowcount_of_parquet(dir_file[0],dir_file[1])
                        elif filetype == ReplicationSpaceFileType.JSON:
                            single_file_count = self.datalake.get_jsonfile_rowcount(dir_file[0],dir_file[1])
                        elif filetype == ReplicationSpaceFileType.JSONLINES:
                            single_file_count = self.datalake.get_jsonlinefile_rowcount(dir_file[0],dir_file[1])
                        elif filetype == ReplicationSpaceFileType.CSV:
                            single_file_count = self.datalake.get_csv_file_rowcount(dir_file[0],dir_file[1],header_index)
                        target_count += single_file_count
                
            print("Source Count: %s, Target Count: %s, wait for %s s, allow greater: %s" % (str(source_count), str(target_count), str(rtime), str(allow_greater)))
            # the count number get from task may be larger than the source count
            if source_count == target_count or allow_greater and source_count <= target_count:
                break
            else:
                time.sleep(interval)
                rtime = rtime + interval       
                if rtime >= check_timeout:
                    print("Validate row count timeout: %s" % (str(check_timeout)))

        if allow_greater:
            self.assertGreaterEqual(target_count, source_count)
        else:
            self.assertEqual(source_count, target_count)

    def check_and_query_status(self, replication_name: str, expected_status = ['COMPLETED'], check_timeout = 120, interval = 10):
        """Checks whether the replicationflow for the replication with the given
        name has been run without errors."""

        rtime=0
        result = False
        while rtime < check_timeout:
            monitor = self.cluster.monitoring.replications.get_monitor(
                replication_name)

            if monitor is None:
                self.skipTest(
                    f'Monitor for replication flow {replication_name} could not be retrieved!')

            taskmetrics = monitor.taskmetrics

            taskmonitors = self.cluster.monitoring.replications.get_taskmonitors(
                replication_name)

            if taskmetrics == None and len(taskmonitors) == 0:
                self.skipTest('No information available!')
 
            has_error = taskmetrics != None and taskmetrics.error != 0

            if has_error:
                message = str(monitor)
                for taskmonitor in taskmonitors:
                    message = message + '\n' + 'Task Status Information: ' + taskmonitor.statusinfo
                    message = message + '\n' + 'Additional information:'
                    for partition in taskmonitor.partitions:
                        if partition.status == 'Error':
                            message = message + \
                                f'\n{partition.id}: Last response received at: {partition.lastErrorAt}, {partition.statusInfo}'
                break
            status_list=[]
            for taskmonitor in taskmonitors: 
                status_list.append(taskmonitor.status.name)

            print("Current Status:" + str(status_list) + " ; Expected Status: " + str(expected_status) + ", wait for " + str(rtime) + " s")
            if status_list == expected_status:
                result = True
                break
            else:
                time.sleep(interval)
                rtime = rtime + interval
                if rtime >= check_timeout:
                    print("Query status timeout: %s" % (str(check_timeout)))
                    message = 'Qeury time out error: Expected Status should be %s, but get the status %s' % (str(expected_status), str(status_list))

        if not result:
            print("Check Status Failed")
            self.fail(message)
        else:
            print("Check Status Passed")


    def execute_to_init_done(self, replication_name: str):
        """
        This is main execution of rms 

        """
        replication = self.cluster.modeler.replications.open_replication(
            replication_name)
        try:
            replicationflow = replication.undeploy()
        except:
            replicationflow = None
        if replicationflow is not None:
            changerequeststatus = replicationflow.wait_while_busy()
            self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                            changerequeststatus.status, changerequeststatus)

        self.clear_replication(replication_name, False)

        replicationflow = replication.deploy()
        self.assertIsNotNone(
            replicationflow, f'Deployment of replication {replication.name} failed!')

        changerequeststatus = replicationflow.wait_while_busy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.status, changerequeststatus)

        replicationflow.run_or_resume()

        changerequeststatus = replicationflow.wait_while_busy()
        self.assertEqual(ChangerequeststatusStatus.COMPLETED,
                         changerequeststatus.status, changerequeststatus)


    def execute_delta_for_hana(self, replication_name: str, check_timeout = 120, interval = 10, delta_row_count = 5):
        """
        This is main delta execution of rms on HANA

        """
        abap = self.get_source_abap(replication_name)
        table_name = self.get_source_identifier(replication_name)

        abap.insert_records(table_name, delta_row_count)
        self.validate_and_query_replication(replication_name, check_timeout, interval)
        abap.update_records(table_name, delta_row_count)
        self.validate_and_query_replication(replication_name, check_timeout, interval)
        abap.delete_records(table_name, delta_row_count)
        self.validate_and_query_replication(replication_name, check_timeout, interval)

    def execute_delta_for_datalake(self, replication_name: str, check_timeout = 120, interval = 10, delta_row_count = 5):
        """
        This is main delta execution of rms on datalake
        the

        """
        abap = self.get_source_abap(replication_name)
        table_name = self.get_source_identifier(replication_name)

        abap.insert_records(table_name, delta_row_count)
        self.validate_and_query_replication(replication_name, check_timeout, interval, 'delta', delta_row_count)
        abap.update_records(table_name, delta_row_count)
        self.validate_and_query_replication(replication_name, check_timeout, interval, 'delta', delta_row_count * 2)
        abap.delete_records(table_name, delta_row_count)
        self.validate_and_query_replication(replication_name, check_timeout, interval, 'delta', delta_row_count * 3)
