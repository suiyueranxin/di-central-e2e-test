from time import sleep
from utils.case_pipeline import PipelineCase
from di_qa_e2e.metadata_explorer import ScoreCard, Term
from di_qa_e2e.models.graph import GraphStatus

from utils.component.connection_management import ConnectionManagement as cm


class TestBRDefaultTenant(PipelineCase):

    def test_pre_backup_01_change_modeler_property(self):
        property_value = "{\"group-default-resources\":{\"requests\":{\"cpu\": \"0.05\", \"memory\": \"256Mi\"}}}"
        property_json = [{
            "defaultValue": "",
            "apiVersion": "v2",
            "id": "vflow.internal.tenant",
            "description": "Modeler: Internal tenant settings, change only when advised by SAP product support",
            "type": "string",
            "customValue": property_value
        }]
        sm = self.get_system_management()
        sm.change_application_property(property_json)
        sm.restart_application("vflow", True)
        sleep(10)
        self.validate_application_property("vflow.internal.tenant", property_value)
        
    def test_post_restore_01_check_modeler_property(self):
        property_value = "{\"group-default-resources\":{\"requests\":{\"cpu\": \"0.05\", \"memory\": \"256Mi\"}}}"
        self.validate_application_property("vflow.internal.tenant", property_value)
    
    def test_pre_backup_02_add_policy(self):
        sm = self.get_system_management()
        policy_name = "backup_policy"
        built_in_policies = ["sap.dh.developer", 
                             "app.datahub-app-data.fullAccess",
                             "sap.dh.connectionContentAllManage"]
        sm.delete_policy(policy_name)
        sleep(3)
        sm.add_policy(policy_name, built_in_policies, "Custom policy. Give access to Modeler and Metadata Explorer.")
        sleep(10)
        self.validate_policy_exists(policy_name)
    
    def test_post_restore_02_check_policy(self):
        policy_name = "backup_policy"
        self.validate_policy_exists(policy_name)
    
    def test_pre_backup_03_assign_policy(self):
        sm = self.get_system_management()
        user_name = "tester"
        policy_name = "backup_policy"
        assigned_policies = ["backup_policy"]
        sm.unassign_policy(user_name, policy_name)
        sleep(3)
        sm.assign_policy(user_name, policy_name)
        sleep(10)
        self.validate_policy_assignment(user_name, assigned_policies)
    
    def test_post_restore_03_check_assign_policy(self):
        user_name = "tester"
        assigned_policies = ["backup_policy"]
        self.validate_policy_assignment(user_name, assigned_policies)

    def test_pre_backup_04_import_files_and_solutions(self):
        sm = self.get_system_management()
        
        local_file = self.get_data_file_path("data/sample.csv")
        sm.remove_folder("/br-folder")
        sleep(3)
        sm.add_folder("/br-folder/br-subfolder")
        sm.import_file(local_file, f"/br-folder/sample.csv")
        sm.import_file(local_file, f"/br-folder/br-subfolder/sample.csv")
        
        sm.remove_folder("/files/vflow/graphs/cet")
        local_solution = self.get_data_file_path("data/default_pipelines-1.0.0.zip")
        sm.import_solution(local_solution)
        
        local_zip_file = self.get_data_file_path("data/schedule.tgz")
        sm.import_zip_file(local_zip_file, "/files/vflow/graphs/cet/br_test")
        
        sm.remove_folder("/files/rms")
        local_rms = self.get_data_file_path("data/default_rms-1.0.0.zip")
        sm.import_solution(local_rms)
        
        # check folder, file
        self.validate_file_exists_in_user_workspace("/br-folder/sample.csv")
        self.validate_file_exists_in_user_workspace("/br-folder/br-subfolder/sample.csv") 
        # check graph
        self.validate_folder_exists_in_user_workspace("/files/vflow/graphs/cet/br_test/cds_to_hana_gen2")
        self.validate_folder_exists_in_user_workspace("/files/vflow/graphs/cet/br_test/write_to_file_gen1")
        self.validate_folder_exists_in_user_workspace("/files/vflow/graphs/cet/br_test/schedule")
        # check rms
        self.validate_file_exists_in_user_workspace("/files/rms/cet_br_test-from_abap_to_file.replication")
        
    def test_post_restore_04_check_files_and_solutions(self):
        # check folder, file
        self.validate_file_exists_in_user_workspace("/br-folder/sample.csv")
        self.validate_file_exists_in_user_workspace("/br-folder/br-subfolder/sample.csv") 
        # check graph
        self.validate_folder_exists_in_user_workspace("/files/vflow/graphs/cet/br_test/cds_to_hana_gen2")
        self.validate_folder_exists_in_user_workspace("/files/vflow/graphs/cet/br_test/write_to_file_gen1")
        self.validate_folder_exists_in_user_workspace("/files/vflow/graphs/cet/br_test/schedule")
        # check rms
        self.validate_file_exists_in_user_workspace("/files/rms/cet_br_test-from_abap_to_file.replication")
            
    def test_pre_backup_05_import_connections(self):
        cm = self.get_connection_management()
        connections = ["BR_HANA_PAL_CC", "BR_GCS", "BR_S4"]
        for conn in connections:
            if cm.connection_exists(conn) :
                cm.delete_connection(conn)
                sleep(3)  
        local_connection_file = self.get_data_file_path("data/default_connections.json")
        cm.import_connections(local_connection_file)
        
        connections.append("DI_DATA_LAKE")
        for conn in connections:
            sleep(3)
            self.validate_connection_status(conn, "OK")
    
    def test_post_restore_05_check_connections(self):
        connections = ["DI_DATA_LAKE", "BR_HANA_PAL_CC", "BR_GCS", "BR_S4"]
        for conn in connections:
            self.validate_connection_status(conn, "OK")
    
    def test_pre_backup_06_run_graphs(self):
        # graph contains customer js operator
        graph_gen1_full_name = "cet.br_test.write_to_file_gen1"
        graph_gen1_run_name = self.get_graph_name(graph_gen1_full_name)
        self.run_graph_until_completed(graph_gen1_full_name, graph_gen1_run_name)
        
        # initial + delta, enable auto restart
        graph_gen2_full_name = "cet.br_test.cds_to_hana_gen2"
        graph_gen2_run_name = self.get_graph_name(graph_gen2_full_name)
        snapshot_config = {
            "enabled": True,
            "periodSeconds": 30
        }
        auto_restart_config={
            "maxRestartCount": 1,
            "restartDelaySeconds": 2,
            "resetTimeThreshold": "180s"
        }
        self.run_graph_until_running(graph_gen2_full_name, 
                                     graph_gen2_run_name,
                                     False, 
                                     snapshot_config=snapshot_config, 
                                     auto_restart_config=auto_restart_config)
    
    def test_post_restore_06_check_graphs(self):
        graph_gen1_full_name = "cet.br_test.write_to_file_gen1"
        graph_gen1_run_name = self.get_graph_name(graph_gen1_full_name)
        self.validate_graph_status(graph_gen1_run_name, GraphStatus.COMPLETED)
        
        graph_gen2_full_name = "cet.br_test.cds_to_hana_gen2"
        graph_gen2_run_name = self.get_graph_name(graph_gen2_full_name)
        self.validate_graph_status(graph_gen2_run_name, GraphStatus.RUNNING) #TODO complete in 10 mins
    
    def test_pre_backup_07_run_rms(self):
        rf_name = "cet_br_test-from_abap_to_file"
        self.get_rms().undeploy_replication_flow(rf_name)
        self.get_rms().deploy_replication_flow(rf_name)
        self.get_rms().wait_util_deploy_complete(rf_name)
        self.get_rms().run_replication_flow(rf_name)
        sleep(30)
        self.validate_rf_status(rf_name, "RUNNING")
                
    def test_post_restore_07_check_rms(self):
        rf_name = "cet_br_test-from_abap_to_file"
        self.validate_rf_status(rf_name, "RUNNING")
        self.get_rms().undeploy_replication_flow(rf_name)
    
    def test_pre_backup_08_create_schedule(self):
        monitoring = self.get_monitoring()
       
        schedule_name = "br_schedule_default"
        monitoring.schedules.delete_schedule(schedule_name)
        sleep(3)
        monitoring.schedules.create_schedule(schedule_name, "cet.br_test.schedule", "40 14 1 * *")
        sleep(30)
        self.validate_schedule_exist(schedule_name)
            
    def test_post_restore_08_check_schedule(self):
        schedule_name = "br_schedule_default"
        self.validate_schedule_exist(schedule_name)
        
    def test_pre_backup_09_me_create_hierarchy_and_tag(self):
        me = self.get_metadata_explorer()
        hierarchy_name = "BrTest"
        tag_name = "BR_DEFAULT_TESTER"
        me.delete_hierarchy(hierarchy_name)
        sleep(3)
        hierarchy_id = me.create_hierarchy(hierarchy_name, "Used for backup and restore testing")
        me.create_tag_by_hierarchy_id(hierarchy_id, tag_name, "Tag for backup and restore testing")
        
    def test_post_restore_09_me_check_hierarchy_and_tag(self):
        hierarchy_name = "BrTest"
        tag_name = "BR_DEFAULT_TESTER"
        self.validate_metadata_tags_exist(hierarchy_name, [tag_name])
        
    def test_pre_backup_10_me_publish(self):
        me = self.get_metadata_explorer()
        publication_name = "gcs_publication"
        conn_type = "GCS"
        conn_name = "BR_GCS"
        file_path = "/CET_BR_TEST/For_ME_Test/Customers.csv"
        me.delete_publication(publication_name)
        sleep(3)
        me.publish(publication_name, conn_name, conn_type, file_path)
        sleep(50)
        self.vaidate_metadata_file_status(conn_name, file_path, expected_published=True)
        
    def test_post_restore_10_me_check_publish(self):
        conn_name = "BR_GCS" 
        file_path = "/CET_BR_TEST/For_ME_Test/Customers.csv"
        self.vaidate_metadata_file_status(conn_name, file_path, expected_published=True)
        
    def test_pre_backup_11_me_lineage(self):
        me = self.get_metadata_explorer()
        publication_name = "hana_view_publication"
        conn_type = "HANA_DB"
        conn_name = "BR_HANA_PAL_CC"
        file_path = "/CET_BR_TEST/BR_VIEW"
        me.delete_publication(publication_name)
        sleep(6)
        me.publish(publication_name, conn_name, conn_type, file_path, extract_lineage=True)
        sleep(130)
        self.vaidate_metadata_file_status(conn_name, file_path, expected_published=True, expected_has_lineage=True)
    
    def test_post_restore_11_me_check_lineage(self):
        conn_name = "BR_HANA_PAL_CC"
        file_path = "/CET_BR_TEST/BR_VIEW"
        self.vaidate_metadata_file_status(conn_name, file_path, expected_published=True, expected_has_lineage=True)
            
    def test_pre_backup_12_me_profile(self):
        me = self.get_metadata_explorer()
        conn_type = "GCS"
        conn_name = "BR_GCS"
        file_path = "/CET_BR_TEST/For_ME_Test/Customers.csv"
        me.delete_factsheet(conn_name, file_path)
        sleep(3)
        me.profile(conn_name, conn_type, file_path)
        sleep(30)
        self.vaidate_metadata_file_status(conn_name, file_path, expected_profiled=True)
        
    def test_post_restore_12_me_check_profile(self):
        conn_name = "BR_GCS"
        file_path = "/CET_BR_TEST/For_ME_Test/Customers.csv"
        self.vaidate_metadata_file_status(conn_name, file_path, expected_profiled=True)
    
    def test_pre_backup_13_me_tag_on_the_publication(self):
        me = self.get_metadata_explorer()
        conn_name = "BR_GCS"
        file_path = "/CET_BR_TEST/For_ME_Test/Customers.csv"
        hierarchy_name = "BrTest"
        tag_name = "BR_DEFAULT_TESTER"
        me.remove_tag_from_file(conn_name, file_path, hierarchy_name, tag_name)
        
        me.add_tag_to_file(conn_name, file_path, hierarchy_name, tag_name)
        sleep(10)        
        self.validate_metadata_tags_on_file(conn_name, file_path, hierarchy_name, [tag_name])
        
    def test_post_restore_13_me_check_tag_on_the_publication(self):
        conn_name = "BR_GCS"
        file_path = "/CET_BR_TEST/For_ME_Test/Customers.csv"
        hierarchy_name = "BrTest"
        tag_name = "BR_DEFAULT_TESTER"
        self.validate_metadata_tags_on_file(conn_name, file_path, hierarchy_name, [tag_name])
    
    def test_pre_backup_14_me_prepare_data(self):
        me = self.get_metadata_explorer()
        preparation_name = "gcs_preparation"
        conn_name = "BR_GCS"
        file_path = "/CET_BR_TEST/For_ME_Test/Customers.csv"
        target_conn_name = "BR_HANA_PAL_CC"
        target_file_path = "/CET_BR_TEST/GCS_PREPARATION"
        sample_row = ["1000000002", "WEI", "Tsou", "CN"]
        me.delete_preparation(preparation_name)
        sleep(3)
        # create preparation task
        preparation_id = me.create_preparation(preparation_name, conn_name, file_path, description="GCS preparation for backup and restor testing.")
        sleep(5)
        # add data action: filter on COUNTRY==CN
        column_country_id = me.get_column_id_by_name_in_preparation(preparation_id, "COUNTRY")
        filter_action_on_country = {
            "type": "filter",
            "parameters": {
                "filters": {
                "operator": "AND",
                "operands": [{
                    "id": column_country_id,
                    "comparator": "EQUAL",
                    "values": ["CN"]
                    }]
                }
            }
        }
        me.create_action_in_preparation(preparation_id, filter_action_on_country)
        sleep(5)
        # add data action: upper case on FIRST_NAME
        column_first_name_id = me.get_column_id_by_name_in_preparation(preparation_id, "FIRST_NAME")     
        upper_case_action_on_first_name = {
            "type": "changeCase",
            "parameters": {
                "appliedColumnIds": [column_first_name_id],
                "caseOption": "UPPER"
            }
        }
        me.create_action_in_preparation(preparation_id, upper_case_action_on_first_name)
        sleep(5)
        # run preparation
        me.run_preparation(preparation_id, target_conn_name, target_file_path, type="TABLE")
        self.validate_metadata_row_count(target_conn_name, target_file_path, 31)
        self.validate_metadata_sample_row(target_conn_name, target_file_path, 0, sample_row)
        
    def test_post_restore_14_me_check_prepare_data(self):
        target_conn_name = "BR_HANA_PAL_CC"
        target_file_path = "/CET_BR_TEST/GCS_PREPARATION"
        sample_row = ["1000000002", "WEI", "Tsou", "CN"]
        # self.validate_conn_content_total_count(target_conn_name, "CET_BR_TEST.GCS_PREPARATION", 31)
        self.validate_metadata_row_count(target_conn_name, target_file_path, 31)
        self.validate_metadata_sample_row(target_conn_name, target_file_path, 0, sample_row)
        
    def test_pre_backup_15_me_create_rule(self):
        me = self.get_metadata_explorer()
        rule_category_name = "BrTest"
        rule_name = "ColumnNotNull"
        rule_category_description = "Rule for backup and restore testing"
        rulebook_name = "br-rulebook"
        
        me.delete_rulebook(rulebook_name)
        sleep(3)
        me.delete_rule(rule_category_name, rule_name)
        sleep(3)
        me.delete_rule_category(rule_category_name)
        sleep(3)
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
                    "type": "STRING"
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
        
    def test_post_restore_15_me_check_rule(self):
        rule_category_name = "BrTest"
        rule_name = "ColumnNotNull"
        self.validate_metadata_rule_exist(rule_category_name, rule_name)
    
    def test_pre_backup_16_me_create_and_run_rulebook(self):
        me = self.get_metadata_explorer()
        rulebook_name = "br-rulebook"
        rule_category_name = "BrTest"
        rule_name = "ColumnNotNull"
        me.delete_rulebook(rulebook_name)
        sleep(3)
        # create rulebook
        rulebook_id, rulebook_timestamp = me.create_rulebook(rulebook_name, "Rule book for backup and restore testing")
        # add rule in rulebook
        rule_refer_id, rulebook_timestamp = me.add_rule_in_rulebook(rulebook_id, rulebook_timestamp, rule_category_name, rule_name)
        # add data binding in rule of rulebook
        conn_name = "DI_DATA_LAKE"
        file_path = "/shared/CET_BR_TEST/For_ME_Test/Customers2.csv"
        binding_config = [{
            "parameter": "$COL",
            "column": "FIRST_NAME"
        }]
        me.create_rule_binding_in_rulebook(rulebook_id, rulebook_timestamp, rule_refer_id, conn_name, file_path, binding_config)
        # run rulebook
        me.run_rulebook(rulebook_id)
        sleep(100)
        self.validate_metadata_rulebook_result(rulebook_id, 100)
        
    def test_post_restore_16_me_check_run_rulebook(self):
        me = self.get_metadata_explorer()
        rulebook_name = "br-rulebook"
        rulebook_id, _ = me.get_rulebook_by_name(rulebook_name)
        me.run_rulebook(rulebook_id)
        sleep(100)
        self.validate_metadata_rulebook_result(rulebook_id, 100)
    
    def test_pre_backup_17_me_create_rule_dashboard(self):
        me = self.get_metadata_explorer()
        rule_dashboard_name = "br-rule-dashboard"
        rulebook_name = "br-rulebook"
        me.delete_rule_dashboard(rule_dashboard_name)
        sleep(3)
        # create a dashboard
        dashboard_id = me.create_rule_dashboard(rule_dashboard_name, "Dashboard for backup and restore testing")
        # add scorecard in the dashboard
        rulebook_id, _ = me.get_rulebook_by_name(rulebook_name)
        scorecard = ScoreCard(rulebook_id, rulebook_name)
        scorecard.apply_template_RadialKpiWidget()
        me.add_scorecard_in_rule_dashboard(dashboard_id, scorecard)
        self.validate_metadata_rule_scorecard_percentage(dashboard_id, 0, 0, 100)
        
    def test_post_restore_17_me_check_rule_dashboard(self):
        me = self.get_metadata_explorer()
        rule_dashboard_name = "br-rule-dashboard"
        dashboard_id, _ = me.get_rule_dashboard_by_name(rule_dashboard_name)
        self.validate_metadata_rule_scorecard_percentage(dashboard_id, 0, 0, 100)
        
    def test_pre_backup_18_me_create_terms(self):
        me = self.get_metadata_explorer()
        # prepare term related dataset
        publication_name = "sdl_publication"
        conn_type = "SDL"
        conn_name = "DI_DATA_LAKE"
        file_path = "/shared/CET_BR_TEST/For_ME_Test/Customers2.csv"
        me.delete_publication(publication_name)
        sleep(3)
        me.publish(publication_name, conn_name, conn_type, file_path)
        sleep(30)
        # prepare term related rulebook
        rulebook_name = "br-rulebook"
        rule_category_name = "BrTest"
        rule_name = "ColumnNotNull"
        # clearn glossary
        glossary_name = "br-glossary"
        me.delete_glossary(glossary_name)
        sleep(3)
        # create glossary
        glossary_id = me.create_glossary(glossary_name, "Terms for backup and restore testing")
        # add categories and terms
        categories = ["Address", "Customer/Name"]
        terms = [{
            "name": "COUNTRY",
            "description": "An area of land that has or used to have its own government and laws",
            "categories": ["Address"],
            "keywords": ["country"],
            "synonyms": [],
            "relationships": {}
        },{
            "name": "CITY",
            "description": "A large and important town.",
            "categories": ["Address"],
            "keywords": ["cites", "city", "town","towns"],
            "synonyms": ["TOWN"],
            "relationships": {
                "term": "COUNTRY"
            }
        },{
            "name": "FIRST_NAME",
            "description": "Given the first name from customer",
            "categories": ["Customer"],
            "keywords": ["given name", "name"],
            "synonyms": ["FORE_NAME", "GIVEN_NAME"],
            "relationships": {
                "dataset": [conn_name, file_path, ["FIRST_NAME"]]
            }
        },{
            "name": "LAST_NAME",
            "description": "Family name of a customer",
            "categories": ["Customer"],
            "keywords": ["family name", "last name", "name"],
            "synonyms": ["FAMILY_NAME", "SURNAME"],
            "relationships": {
                "term": "FIRST_NAME",
                "dataset": [conn_name, file_path, ["LAST_NAME"]],
                "rulebook": rulebook_name,
                "rule": [rule_category_name, rule_name]
            }
        }]
        for category in categories:
            category_names = category.split("/")
            category_id = ""
            category_name = ""
            for category_name in category_names:               
                category_id = me.add_category_in_glossary(glossary_id, category_name, parent_category_id=category_id)
        for term_data in terms:
            term = Term(term_data["name"], term_data["description"], term_data["keywords"], term_data["synonyms"])
            me.add_term_in_glossary(glossary_id, term, term_data["categories"][0])
            sleep(30)
            relationships = term_data.get("relationships")
            if relationships != {}:
                related_term = relationships.get("term")
                related_dataset = relationships.get("dataset")
                related_rulebook = relationships.get("rulebook")
                relate_rule = relationships.get("rule")
                if related_term is not None:
                    me.update_related_term_of_term_in_glossary(glossary_id, term, related_term)
                if related_dataset is not None:
                    me.update_related_dataset_or_columns_of_term_in_glossary(glossary_id, term, 
                                                                                conn_name=related_dataset[0], 
                                                                                file_path=related_dataset[1], 
                                                                                column_names=related_dataset[2])
                if related_rulebook is not None:
                    me.update_related_rulebook_of_term_in_glossary(glossary_id, term, related_rulebook)
                if relate_rule is not None:
                    me.update_related_rule_of_term_in_glossary(glossary_id, term, 
                                                                rule_category_name=relate_rule[0],
                                                                rule_name=relate_rule[1])
                
                me.update_relationship_of_term_in_glossary(glossary_id, term)
        
    def test_post_restore_18_me_check_terms(self):
        conn_name = "DI_DATA_LAKE"
        file_path = "/shared/CET_BR_TEST/For_ME_Test/Customers2.csv"
        rulebook_name = "br-rulebook"
        rule_category_name = "BrTest"
        rule_name = "ColumnNotNull"
        glossary_name = "br-glossary"
        terms = [{
            "name": "COUNTRY",
            "description": "An area of land that has or used to have its own government and laws",
            "categories": ["Address"],
            "keywords": ["country"],
            "synonyms": [],
            "relationships":{
                "term": "CITY"
            }
        },{
            "name": "CITY",
            "description": "A large and important town.",
            "categories": ["Address"],
            "keywords": ["cites", "city", "town","towns"],
            "synonyms": ["TOWN"],
            "relationships": {
                "term": "COUNTRY"
            }
        },{
            "name": "FIRST_NAME",
            "description": "Given the first name from customer",
            "categories": ["Customer"],
            "keywords": ["given name", "name"],
            "synonyms": ["FORE_NAME", "GIVEN_NAME"],
            "relationships": {
                "term": "LAST_NAME",
                "dataset": [conn_name, file_path, ["FIRST_NAME"]]
            }
        },{
            "name": "LAST_NAME",
            "description": "Family name of a customer",
            "categories": ["Customer"],
            "keywords": ["family name", "last name", "name"],
            "synonyms": ["FAMILY_NAME", "SURNAME"],
            "relationships": {
                "term": "FIRST_NAME",
                "dataset": [conn_name, file_path, ["LAST_NAME"]],
                "rule": [rule_category_name, rule_name],
                "rulebook": rulebook_name
            }
        }]
        terms.sort(key=(lambda x: x["name"]))
        self.validate_metadata_terms(glossary_name, terms)
        
    def test_pre_backup_19_me_create_favorite(self):
        me = self.get_metadata_explorer()
        me.delete_favorite_in_catalog("BR_HANA_PAL_CC", "/CET_BR_TEST")
        sleep(3)
        me.add_favorite_in_catalog("BR_HANA_PAL_CC", "/CET_BR_TEST")
        sleep(10)
        self.validate_metadata_favorite("BR_HANA_PAL_CC", "/CET_BR_TEST")
        
    def test_post_restore_19_me_check_favorite(self):
        self.validate_metadata_favorite("BR_HANA_PAL_CC", "/CET_BR_TEST")
    
    def test_pre_backup_20_me_create_scheduled_task(self):
        me = self.get_metadata_explorer()
        publication_name = "hana_view_publication"
        me.delete_scheduled_publication_task(publication_name)
        sleep(3)
        me.add_scheduled_publication_task(publication_name, "2022-11-01T00:00:00.000Z", "15 13 1 */1 *")
        sleep(3)
        self.validate_metadata_schedules([publication_name])
        
    def test_post_restore_20_me_check_scheduled_task(self):
        publication_name = "hana_view_publication"
        self.validate_metadata_schedules([publication_name])
    
    def test_post_restore_21_me_check_tasks(self):
        me = self.get_metadata_explorer()
        publications = ["gcs_publication", "hana_view_publication", "sdl_publication"]
        profiles = ["Customers.csv"]
        preparation = ["gcs_preparation"]
        rulebooks = ["br-rulebook"]
        self.validate_metadata_monitor_task(publications + profiles + preparation + rulebooks)