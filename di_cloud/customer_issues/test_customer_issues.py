from time import sleep
from utils.case_pipeline import PipelineCase


class TestCustomerIssues(PipelineCase):

    def test_dibugs_10531(self):
        graph_full_name = "test.customer_issues.DIBUGS-10531"
        self.run_graph_until_running(graph_full_name, 
                                     self.get_graph_name(graph_full_name, "gen2"),
                                     should_immediate_stop=True)
     
    def test_dibugs_11570(self):
        graph_full_name = "test.customer_issues.DIBUGS-11570"
        substitutions = self.get_graph_substitutions(graph_full_name)
        self.assertListEqual(substitutions,["File_Connection_ID","HANA_Connection_ID","Table_Name"])
        
    def test_dibugs_12139(self):
        graph_full_name = "test.customer_issues.DIBUGS-12139"
        self.run_graph_until_running(graph_full_name, 
                                     self.get_graph_name(graph_full_name, "gen1"),
                                     should_immediate_stop=True)
      
    def test_dibugs_13121(self):
        # check cnode and no_auth
        graph_full_name_cnode_no_auth = "test.customer_issues.DIBUGS-13121.cnode_no_auth"
        target_conn = "CIT_FILE"
        target_file_cnode_no_auth = "/CET_TEST/DIBUGS-13121/cnode_no_auth.txt"
        target_content_cnode_no_auth = '"success":true'
        graph_execution = self.run_graph_until_running(graph_full_name_cnode_no_auth, 
                                     self.get_graph_name(graph_full_name_cnode_no_auth, "gen1"))
        sleep(30)
        self.validate_conn_content_contains(target_conn, target_file_cnode_no_auth, target_content_cnode_no_auth)
        graph_execution.stop()
        
        # check openapi and no_auth
        graph_full_name_local_openapi = "test.customer_issues.DIBUGS-13121.local_openapi_no_auth"
        target_file_local_openapi = "/CET_TEST/DIBUGS-13121/local_openapi_no_auth.txt"
        target_content_local_openapi = "This is a dummy server with no auth"
        graph_openapi_execution = self.run_graph_until_running(graph_full_name_local_openapi, 
                                                               self.get_graph_name(graph_full_name_local_openapi, "gen1"))
        sleep(30)
        self.validate_conn_content_contains(target_conn, target_file_local_openapi, target_content_local_openapi)
        graph_openapi_execution.stop()
        
        # check openapi and oauth2
        graph_full_name_local_openapi_oauth2 = "test.customer_issues.DIBUGS-13121.local_openapi_oauth2"
        target_file_local_openapi_oauth2 = "/CET_TEST/DIBUGS-13121/local_openapi_oauth2.txt"
        target_content_local_openapi_oauth2 = "unsupported_token_type"
        graph_oauth2_execution = self.run_graph_until_running(graph_full_name_local_openapi_oauth2, 
                                                              self.get_graph_name(graph_full_name_local_openapi_oauth2, "gen1"))
        sleep(30)
        self.validate_conn_content_contains(target_conn, target_file_local_openapi_oauth2, target_content_local_openapi_oauth2)
        graph_oauth2_execution.stop()
        
    #TODO: check sftp session 
    def test_dibugs_11222(self):
        graph_full_name = "test.customer_issues.DIBUGS-11222"
        graph_execution = self.run_graph_until_running(graph_full_name, 
                                     self.get_graph_name(graph_full_name, "gen1"))
        sleep(30)
        graph_execution.stop()
        
        
    #TODO: check sftp session
    def test_dibugs_13282(self):
        graph_full_name = "test.customer_issues.DIBUGS-13282"
        self.run_graph_until_completed(graph_full_name, 
                                       self.get_graph_name(graph_full_name, "gen1"), 
                                       config_substitutions={
                                            "PATH_INPUT": "source/sample.csv",
                                            "PATH_RESULT": "/upload/13282.txt"})
       
    def test_dibugs_11143(self):
        graph_full_name = "test.customer_issues.DIBUGS-11143"
        self.run_graph_until_running(graph_full_name, 
                                     self.get_graph_name(graph_full_name, "gen1"), 
                                     should_immediate_stop=True)
        
    def test_dibugs_14309(self):
        graph_full_name = "test.customer_issues.DIBUGS-14309"
        self.run_graph_until_running(graph_full_name, 
                                     self.get_graph_name(graph_full_name, "gen1"), 
                                     should_immediate_stop=True)
        
    def test_dibugs_18399(self):
        graph_full_name_gen1 = "test.customer_issues.DIBUGS-18399.adl_v2_sas_gen1"
        self.run_graph_until_running(graph_full_name_gen1, 
                                     self.get_graph_name(graph_full_name_gen1, "gen1"), 
                                     should_immediate_stop=True)
        
        graph_full_name_gen2 = "test.customer_issues.DIBUGS-18399.adl_v2_sas_gen2"
        self.run_graph_until_running(graph_full_name_gen1, 
                                     self.get_graph_name(graph_full_name_gen2, "gen2"), 
                                     should_immediate_stop=True)
    
    def test_dibugs_18486_statuscheck_ADLV2_oauth2_client(self):
        connection = "ADL_V2_oauth2_client"
        self.validate_connection_status(connection, "OK")

    def test_dibugs_18486_statuscheck_ADLV2_oauth2_user(self):
        connection = "ADL_V2_oauth2_user"
        self.validate_connection_status(connection, "OK")

    def test_dibugs_18291_SFTP_connection_with_RSA_key(self):
        graph_full_name = "test.customer_issues.DIBUGS-18291"
        self.run_graph_until_completed(graph_full_name,graph_full_name)

    def test_dibugs_18249(self):
        graph_full_name = "test.customer_issues.DIBUGS-18249"
        self.run_graph_until_completed(graph_full_name, 
                                       self.get_graph_name(graph_full_name, "gen1"), 
                                       config_substitutions={
                                            "PATH_INPUT": "source/sample.csv",
                                            "PATH_RESULT": "cet-test/DIBUGS-18249/success.csv"})
        
    def test_dibugs_18162(self):
        # check OData with grant_type: password_with_confidential_client
        graph_full_name = "test.customer_issues.DIBUGS-18162.odata"
        self.run_graph_until_running(graph_full_name,graph_full_name)

        # check OpenAPI with grant_type: client_credentials
        graph_full_name = "test.customer_issues.DIBUGS-18162.openapi"
        target_conn = "CIT_FILE"
        target_file = "/CET_TEST/DIBUGS-18162/openapi_oauth2.txt"
        target_content = '"username":"test"'
        self.run_graph_until_running(graph_full_name, 
                                       self.get_graph_name(graph_full_name, "gen1"), 
                                       should_immediate_stop=True)
        self.validate_conn_content_contains(target_conn, target_file, target_content)

        # check HTTP with grant_type: password_with_confidential_client
        graph_full_name = "test.customer_issues.DIBUGS-18162.http"
        target_conn = "CIT_FILE"
        target_file = "/CET_TEST/DIBUGS-18162/http_oauth2.txt"
        target_content = '"username":"test"'
        self.run_graph_until_running(graph_full_name, 
                                       self.get_graph_name(graph_full_name, "gen2"), 
                                       should_immediate_stop=True)
        self.validate_conn_content_contains(target_conn, target_file, target_content)