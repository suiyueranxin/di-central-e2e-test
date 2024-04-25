from time import sleep
from utils.case_pipeline import PipelineCase

class TestBRBigDataDebug(PipelineCase):

    def test_pre_backup_import_files_of_big_size(self):
        sm = self.get_system_management()
        # check there are 500 file with size: 100M
        self.validate_file_count_in_user_workspace("/big-data", "massive_data", 500)

    def test_post_restore_import_files_of_big_size(self):
        sm = self.get_system_management()
        # check there are 500 file with size: 100M
        self.validate_file_count_in_user_workspace("/big-data", "massive_data", 500)
         