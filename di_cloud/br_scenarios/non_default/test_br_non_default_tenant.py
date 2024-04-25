from time import sleep
from unittest import skip
from utils.case_pipeline import PipelineCase
from di_qa_e2e.metadata_explorer import ScoreCard
from di_qa_e2e.models.graph import GraphStatus

from utils.component.connection_management import ConnectionManagement as cm


class TestBRNonDefaultTenant(PipelineCase):

    # @skip("DIBUGS-")
    def test_pre_backup_01_change_metadata_explorer_property(self):
        property_catalog_memory_limit = "4096"
        property_preparation_memory_limit = "2048"
        property_json = [{
            "apiVersion": "v2",
            "id": "datahub.app.metadata.memorylimit",
            "description": "Metadata Explorer: Metadata catalog memory usage limit (MB)",
            "type": "integer",
            "defaultValue": "8192",
            "customValue": property_catalog_memory_limit
        }, {
            "apiVersion": "v2",
            "id": "datahub.app.preparation.memorylimit",
            "description": "Applications: Preparation memory usage limit (MB)",
            "type": "integer",
            "defaultValue": "4096",
            "customValue": property_preparation_memory_limit
        }]
        sm = self.get_system_management()
        sm.change_application_property(property_json)
        sm.restart_application("datahub-app-data")
        sleep(30)
        
        # validate in system management
        self.validate_application_property("datahub.app.metadata.memorylimit", property_catalog_memory_limit)
        self.validate_application_property("datahub.app.preparation.memorylimit", property_preparation_memory_limit)
        
        # validate in metadata explorer
        sleep(3)
        self.validate_metadata_memory_limit(int(property_catalog_memory_limit), int(property_preparation_memory_limit))
        
        
    def test_post_restore_01_check_metadata_explorer_property(self):
        property_catalog_memory_limit = "4096"
        property_preparation_memory_limit = "2048"
        
        # validate in system management
        self.validate_application_property("datahub.app.metadata.memorylimit", property_catalog_memory_limit)
        self.validate_application_property("datahub.app.preparation.memorylimit", property_preparation_memory_limit)
        
        # validate in metadata explorer
        self.validate_metadata_memory_limit(int(property_catalog_memory_limit), int(property_preparation_memory_limit))
        
    def test_pre_backup_02_add_policy(self):
        sm = self.get_system_management()
        policy_name = "t1_backup_policy"
        built_in_policies = ["sap.dh.developer", 
                             "app.datahub-app-data.fullAccess",
                             "sap.dh.connectionContentAllManage"]
        sm.delete_policy(policy_name)
        sleep(3)
        sm.add_policy(policy_name, built_in_policies, "Custom policy. Give access to Modeler and Metadata Explorer.")
        sleep(3)
        self.validate_policy_exists(policy_name)
    
    def test_post_restore_02_check_policy(self):
        policy_name = "t1_backup_policy"
        self.validate_policy_exists(policy_name)
    
    def test_pre_backup_03_assign_policy(self):
        sm = self.get_system_management()
        user_name = "t1-tester"
        policy_name = "t1_backup_policy"
        assigned_policies = ["t1_backup_policy"]
        sm.unassign_policy(user_name, policy_name)
        sleep(3)
        sm.assign_policy(user_name, policy_name)
        sleep(3)
        self.validate_policy_assignment(user_name, assigned_policies)
    
    def test_post_restore_03_check_assign_policy(self):
        user_name = "t1-tester"
        assigned_policies = ["t1_backup_policy"]
        self.validate_policy_assignment(user_name, assigned_policies)
            
    def test_pre_backup_04_import_files_and_solutions(self):
        sm = self.get_system_management()
        
        local_file = self.get_data_file_path("data/sample.csv")
        sm.remove_folder("/br-folder")
        sm.add_folder("/br-folder/br-subfolder")
        sm.import_file(local_file, f"/br-folder/sample.csv")
        sm.import_file(local_file, f"/br-folder/br-subfolder/sample.csv")
        
        sm.remove_folder("/files/vflow/graphs/cet")
        local_operator = self.get_data_file_path("data/br-test_operator-1.0.0.zip")
        sm.import_solution(local_operator)
        local_pipeline = self.get_data_file_path("data/br-test_pipelines-1.0.0.zip")
        sm.import_solution(local_pipeline)
        
        sm.remove_folder("/files/rms")
        local_rms = self.get_data_file_path("data/br-test_rms-1.0.0.zip")
        sm.import_solution(local_rms)
        
        # check folder, file
        self.validate_file_exists_in_user_workspace("/br-folder/sample.csv")
        self.validate_file_exists_in_user_workspace("/br-folder/br-subfolder/sample.csv") 
        # check graph
        self.validate_folder_exists_in_user_workspace("/files/vflow/graphs/cet/br_test/write_to_file_gen1")
        self.validate_folder_exists_in_user_workspace("/files/vflow/graphs/cet/br_test/cds_to_hana_gen1")
        # check rms
        self.validate_file_exists_in_user_workspace("/files/rms/cet_br_test-from_abap_to_file.replication")
        self.validate_file_exists_in_user_workspace("/files/rms/cet_br_test-from_abap_to_hana.replication")
        
    def test_post_restore_04_check_files_and_solutions(self):
        # check folder, file
        self.validate_file_exists_in_user_workspace("/br-folder/sample.csv")
        self.validate_file_exists_in_user_workspace("/br-folder/br-subfolder/sample.csv") 
        # check graph
        self.validate_folder_exists_in_user_workspace("/files/vflow/graphs/cet/br_test/write_to_file_gen1")
        self.validate_folder_exists_in_user_workspace("/files/vflow/graphs/cet/br_test/cds_to_hana_gen1")
        # check rms
        self.validate_file_exists_in_user_workspace("/files/rms/cet_br_test-from_abap_to_file.replication")
        self.validate_file_exists_in_user_workspace("/files/rms/cet_br_test-from_abap_to_hana.replication")
            
    def test_pre_backup_05_import_connections(self):
        cm = self.get_connection_management()
        connections = ["BR_ADL_V2", "BR_S4", "BR_HANA"]
        for conn in connections:
            if cm.connection_exists(conn):
                cm.delete_connection(conn)
        local_connection_file = self.get_data_file_path("data/br-test_connections.json")
        cm.import_connections(local_connection_file)
        
        connections.append("DI_DATA_LAKE")
        for conn in connections:
            sleep(3)
            self.validate_connection_status(conn, "OK")
    
    def test_post_restore_05_check_connections(self):
        connections = ["DI_DATA_LAKE","BR_ADL_V2", "BR_S4", "BR_HANA"]
        for conn in connections:
            self.validate_connection_status(conn, "OK")
    
    def test_pre_backup_06_run_graphs(self):
        # enable auto start
        auto_restart_config={
            "maxRestartCount": 1,
            "restartDelaySeconds": 2,
            "resetTimeThreshold": "180s"
        }
        graph_gen1_ar_full_name = "cet.br_test.write_to_file_gen1"
        graph_gen1_ar_run_name = self.get_graph_name(graph_gen1_ar_full_name)
        self.run_graph_until_running(graph_gen1_ar_full_name, 
                                     graph_gen1_ar_run_name,
                                     False,
                                     auto_restart_config=auto_restart_config)
        # initial, enable auto restart
        graph_gen1_full_name = "cet.br_test.cds_to_hana_gen1"
        graph_gen1_run_name = self.get_graph_name(graph_gen1_full_name)
        self.run_graph_until_completed(graph_gen1_full_name, 
                                       graph_gen1_run_name, 
                                       config_substitutions={"RUN_ID": "S001"})
    
    def test_post_restore_06_check_graphs(self):
        graph_gen1_ar_full_name = "cet.br_test.write_to_file_gen1"
        graph_gen1_ar_run_name = self.get_graph_name(graph_gen1_ar_full_name)
        self.validate_graph_status(graph_gen1_ar_run_name, GraphStatus.RUNNING)
        
        graph_gen1_full_name = "cet.br_test.cds_to_hana_gen1"
        graph_gen1_run_name = self.get_graph_name(graph_gen1_full_name)
        self.validate_graph_status(graph_gen1_run_name, GraphStatus.COMPLETED)
    
    def test_pre_backup_07_run_rms(self):
        rf_suspend_name = "cet_br_test-from_abap_to_file"
        self.get_rms().undeploy_replication_flow(rf_suspend_name)
        self.get_rms().deploy_replication_flow(rf_suspend_name)
        self.get_rms().wait_util_deploy_complete(rf_suspend_name)
        self.get_rms().run_replication_flow(rf_suspend_name)        
        sleep(120)
        self.get_rms().suspend_replication_flow(rf_suspend_name)
        
        rf_running_name = "cet_br_test-from_abap_to_hana"
        self.get_rms().undeploy_replication_flow(rf_running_name)
        self.get_rms().deploy_replication_flow(rf_running_name)
        self.get_rms().wait_util_deploy_complete(rf_running_name)
        self.get_rms().run_replication_flow(rf_running_name)
        
    def test_post_restore_07_check_rms(self):
        rf_suspend_name = "cet_br_test-from_abap_to_file"
        self.validate_rf_status(rf_suspend_name, "SUSPENDED")
        self.get_rms().run_replication_flow(rf_suspend_name)
        sleep(60)
        self.validate_rf_status(rf_suspend_name, "RUNNING")
        
        rf_running_name = "cet_br_test-from_abap_to_hana"
        self.validate_rf_status(rf_running_name, "RUNNING")

        self.get_rms().undeploy_replication_flow(rf_suspend_name)
        self.get_rms().undeploy_replication_flow(rf_running_name)
    
    def test_pre_backup_08_me_create_hierarchy_and_tag(self):
        me = self.get_metadata_explorer()
        hierarchy_name = "T1BrTest"
        tag_name = "BR_TEST_T1_TESTER"
        me.delete_hierarchy(hierarchy_name)
        hierarchy_id = me.create_hierarchy(hierarchy_name, "Used for backup and restore testing")
        me.create_tag_by_hierarchy_id(hierarchy_id, tag_name, "Tag for backup and restore testing")
        
    def test_post_restore_08_me_check_hierarchy_and_tag(self):
        hierarchy_name = "T1BrTest"
        tag_name = "BR_TEST_T1_TESTER"
        self.validate_metadata_tags_exist(hierarchy_name, [tag_name])
        
    def test_pre_backup_09_me_publish(self):
        me = self.get_metadata_explorer()
        publications = [{
            "name": "hana_table_publication",
            "conn_type": "HANA_DB",
            "conn_name": "BR_HANA",
            "file_path": "/CET_BR_TEST/BR_TABLE"
        },{
            "name": "abap_publication",
            "conn_type": "ABAP",
            "conn_name": "BR_S4",
            "file_path": "/CDS/CA/DI/IS/ABA/DHE2E_CDS_WA_LS"
        }]
        for publication in publications:
            me.delete_publication(publication["name"])
            me.publish(publication["name"], publication["conn_name"], publication["conn_type"], publication["file_path"])
            sleep(140)
            self.vaidate_metadata_file_status(publication["conn_name"], publication["file_path"], expected_published=True)
        
    def test_post_restore_09_me_check_publish(self):
        publications = [{
            "conn_name": "BR_HANA",
            "file_path": "/CET_BR_TEST/BR_TABLE"
        },{
            "conn_name": "BR_S4",
            "file_path": "/CDS/CA/DI/IS/ABA/DHE2E_CDS_WA_LS"
        }]
        for publication in publications:
            self.vaidate_metadata_file_status(publication["conn_name"], publication["file_path"], expected_published=True)
            
    def test_pre_backup_10_me_profile(self):
        me = self.get_metadata_explorer()
        conn_type = "ADL_V2"
        conn_name = "BR_ADL_V2"
        file_path = "/CET_BR_TEST/For_ME_Test/Customers.csv"
        me.delete_factsheet(conn_name, file_path)
        
        me.profile(conn_name, conn_type, file_path)
        sleep(30)
        self.vaidate_metadata_file_status(conn_name, file_path, expected_profiled=True)
        
    def test_post_restore_10_me_check_profile(self):
        conn_name = "BR_ADL_V2"
        file_path = "/CET_BR_TEST/For_ME_Test/Customers.csv"
        self.vaidate_metadata_file_status(conn_name, file_path, expected_profiled=True)
    
    def test_pre_backup_11_me_tag_on_the_publication(self):
        me = self.get_metadata_explorer()
        publications = [{
            "conn_name": "BR_HANA",
            "file_path": "/CET_BR_TEST/BR_TABLE"
        },{
            "conn_name": "BR_S4",
            "file_path": "/CDS/CA/DI/IS/ABA/DHE2E_CDS_WA_LS"
        }]
        hierarchy_name = "T1BrTest"
        tag_name = "BR_TEST_T1_TESTER"
        for publication in publications:
            me.remove_tag_from_file(publication["conn_name"], publication["file_path"], hierarchy_name, tag_name)
            
            me.add_tag_to_file(publication["conn_name"], publication["file_path"], hierarchy_name, tag_name)
            sleep(10)
            self.validate_metadata_tags_on_file(publication["conn_name"], publication["file_path"], hierarchy_name, [tag_name])
        
    def test_post_restore_11_me_check_tag_on_the_publication(self):
        publications = [{
            "conn_name": "BR_HANA",
            "file_path": "/CET_BR_TEST/BR_TABLE"
        },{
            "conn_name": "BR_S4",
            "file_path": "/CDS/CA/DI/IS/ABA/DHE2E_CDS_WA_LS" #TODO full path of abap file
        }]
        hierarchy_name = "T1BrTest"
        tag_name = "BR_TEST_T1_TESTER"
        for publication in publications:
            sleep(3)
            self.validate_metadata_tags_on_file(publication["conn_name"], publication["file_path"], hierarchy_name, [tag_name])
    
    def test_pre_backup_12_me_create_rule(self):
        me = self.get_metadata_explorer()
        rulebook_name = "t1-br-rulebook"
        rule_category_name = "T1BrTest"
        rule_name = "ColumnNotNull"
        rule_category_description = "Rule for backup and restore testing"
        me.delete_rulebook(rulebook_name)
        me.delete_rule(rule_category_name, rule_name)
        me.delete_rule_category(rule_category_name)
        
        category_id = me.create_rule_category(rule_category_name, rule_category_description)
        rule_column_not_null = {
            "categoryId": category_id,
            "displayName": "ColumnNotNull",
            "name": rule_name,
            "description": "Check column is not null",
            "config": {
                "parameters": [{
                    "name": "$COL",
                    "previousName": "",
                    "description": "",
                    "contentType": "DEFAULT",
                    "type": "INTEGER"
                }],
                "conditions": [{
                    "name": "$COL",
                    "operator": "IS_NOT_NULL",
                    "conditionName": "ColumnNotNull",
                    "valuesList": []
                }],
                "conditionGrouping": [],
                "filters":[],
                "filterGrouping":[]
            }
        }
        me.create_rule(rule_column_not_null)
        self.validate_metadata_rule_exist(rule_category_name, rule_name)
        
    def test_post_restore_12_me_check_rule(self):
        rule_category_name = "T1BrTest"
        rule_name = "ColumnNotNull"
        self.validate_metadata_rule_exist(rule_category_name, rule_name)
    
    def test_pre_backup_13_me_create_and_run_rulebook(self):
        me = self.get_metadata_explorer()
        rulebook_name = "t1-br-rulebook"
        rule_category_name = "T1BrTest"
        rule_name = "ColumnNotNull"
        me.delete_rulebook(rulebook_name)
        # create rulebook
        rulebook_id, rulebook_timestamp = me.create_rulebook(rulebook_name, "Rule book for backup and restore testing")
        # add rule in rulebook
        rule_refer_id, rulebook_timestamp = me.add_rule_in_rulebook(rulebook_id, rulebook_timestamp, rule_category_name, rule_name)
        # add data binding in rule of rulebook
        conn_name = "BR_HANA"
        file_path = "/CET_BR_TEST/BR_TABLE"
        binding_config = [{
            "parameter": "$COL",
            "column": "EMPNO"
        }]
        me.create_rule_binding_in_rulebook(rulebook_id, rulebook_timestamp, rule_refer_id, conn_name, file_path, binding_config)
        # run rulebook
        me.run_rulebook(rulebook_id)
        sleep(30)
        self.validate_metadata_rulebook_result(rulebook_id, 93.33)
        
    def test_post_restore_13_me_check_run_rulebook(self):
        me = self.get_metadata_explorer()
        rulebook_name = "t1-br-rulebook"
        rulebook_id, _ = me.get_rulebook_by_name(rulebook_name)
        me.run_rulebook(rulebook_id)
        sleep(130)
        self.validate_metadata_rulebook_result(rulebook_id, 93.33)
    
    def test_pre_backup_14_me_create_rule_dashboard(self):
        me = self.get_metadata_explorer()
        rule_dashboard_name = "t1-br-rule-dashboard"
        rulebook_name = "t1-br-rulebook"
        me.delete_rule_dashboard(rule_dashboard_name)
        
        # create a dashboard
        dashboard_id = me.create_rule_dashboard(rule_dashboard_name, "Dashboard for backup and restore testing")
        # add scorecard in the dashboard
        rulebook_id, _ = me.get_rulebook_by_name(rulebook_name)
        scorecard = ScoreCard(rulebook_id, rulebook_name)
        scorecard.apply_template_RadialKpiWidget()
        me.add_scorecard_in_rule_dashboard(dashboard_id, scorecard)
        self.validate_metadata_rule_scorecard_percentage(dashboard_id, 0, 0, 93.33)
        
    def test_post_restore_14_me_check_rule_dashboard(self):
        me = self.get_metadata_explorer()
        rule_dashboard_name = "t1-br-rule-dashboard"
        dashboard_id, _ = me.get_rule_dashboard_by_name(rule_dashboard_name)
        self.validate_metadata_rule_scorecard_percentage(dashboard_id, 0, 0, 93.33)
        
    def test_pre_backup_15_me_create_favorite(self):
        me = self.get_metadata_explorer()
        me.delete_favorite_in_catalog("BR_S4", "/CDS")
        
        me.add_favorite_in_catalog("BR_S4", "/CDS")
        sleep(3)
        self.validate_metadata_favorite("BR_S4", "/CDS")
        
    def test_post_restore_15_me_check_favorite(self):
        self.validate_metadata_favorite("BR_S4", "/CDS")
    
    def test_post_restore_16_me_check_tasks(self):
        me = self.get_metadata_explorer()
        publications = ["abap_publication", "hana_table_publication"]
        profiles = ["Customers.csv"]
        rulebooks = ["t1-br-rulebook"]
        self.validate_metadata_monitor_task(publications + profiles + rulebooks)