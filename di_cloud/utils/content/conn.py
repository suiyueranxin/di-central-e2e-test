from datetime import datetime, timedelta
import os
import inspect

from email import header

from di_qa_e2e_validation.abap.CitAbapClient import CitAbapClient
from di_qa_e2e_validation.hana.HanaClient import HanaClient
from di_qa_e2e_validation.datalake.DatalakeClient import DatalakeClient
from di_qa_e2e.connections.connection_data import ConnectionData


class ConnContent:
    
    def __init__(self, conn_config, content_id: str):
        self.__conn_config = conn_config # type: ConnConfig
        self.__content_id = content_id
        
    def get_client(self):
        return self.__conn_config.get_conn_client()
        
    def get_count_of_content(self):
        return self._call()

    def get_count_of_content_from_adlv2(self, substring, header_index, is_row_count_same):
        return self._get_count_of_content_adl_v2(substring, header_index, is_row_count_same)

    def get_content(self):
        return self._call()
        
    @classmethod
    def compare_count_of_content(content_1, content_2) -> int:
        count_1 = content_1.get_count_of_content() 
        count_2 = content_2.get_count_of_content()
        if count_1 > count_2:
            return 1
        elif count_1 < count_2:
            return -1
        return 0
    
    def _call(self, *args, **kwargs):
        caller = inspect.stack()[1][3]
        called = getattr(self, f"_{caller}_{self.__conn_config.get_conn_type().lower()}")
        if called:
            return called(*args, **kwargs)
        return None
    
    def _get_count_of_content_hana(self):
        schema, table = self.__content_id.split(".")
        return self.get_client().get_rowcount(schema, table)
    
    def _get_count_of_content_abap(self):
        return self.get_client().get_rowcount(self.__content_id)
    
    def _get_count_of_content_adl_v2(self, substring='', header_index = 'infer', is_row_count_same = True):
        """ Calculate the total row count of files under a specified folder.

        Parameters
        ----------
        path: str
            The directory of the files located.

        substring: str
            The contained string of the files to be filtered and calculated.
        
        header_index: str
            Indicate if there is header of the file and the index of it

        is_row_count_same: boolean
            The identifier to indicate if the files contain same row count or not. 
            If set TRUE, will use convenient way to calculate the total row count: row count of each file * files count.
            If set FALSE, will calculate the total row count by add the row count of each file.
        """
        file_client = self.get_client()
        path = self.__content_id
        files = file_client.get_fileNames(path, substring) 
        filescount = len(files)
        rowcount = 0
        if filescount != 0:
            if is_row_count_same == True:
                if '.json' in files[0]:
                    file_first_rowcount = len(file_client.get_jsonfile_content(path, files[0]))
                else:
                    file_first_rowcount = file_client.get_csv_file_rowcount(path, files[0], header_index)
                    
                target_index = filescount - 1
                sum_count = 0
                for i in range(target_index, -1, -1):
                    if '.json' in files[0]:
                        file_index_rowcount = len(file_client.get_jsonfile_content(path, files[i]))
                    else:
                        file_index_rowcount = file_client.get_csv_file_rowcount(path, files[i], header_index)

                    if file_index_rowcount == file_first_rowcount:
                        target_index = i
                        break
                    else:
                        sum_count += file_index_rowcount

                rowcount = file_first_rowcount * (target_index + 1) + sum_count
            else:
                if '.json' in files[0]:
                    for file in files:
                        rowcount += len(file_client.get_jsonfile_content(path,file))
                else:
                    for file in files:
                        rowcount += file_client.get_csv_file_rowcount(path,file,header_index)

        return rowcount
    
    #TODO: To be enhanced case by case   
    def _get_content_adl_v2(self):
        index = self.__content_id.rfind("/")
        path, file_name = self.__content_id[0:index], self.__content_id[index+1:]
        file_type = file_name.split(".")[-1] 
        # Not handle a big non-structural file as string, will handle it if there's a case.     
        if file_type == "txt": 
            content = next(self.get_client().download_file_as_bytes(path, file_name), None)
            return "" if content is None else str(content, 'UTF-8')
        #TODO: handle other file_types such as json and csv
        return ""
    
class ConnConfig:
    
    # static variables
    __ENV_FILE_ROOT_PATH = os.path.join(os.path.dirname(__file__), '../../connection_data')
    
    def __init__(self, conn_id: str, conn_type: str, conn_timeout: int):
        self.__conn_type = conn_type
        self.__conn_data = None
        self.__conn_client = None
        self.__conn_timeout = 0 if conn_timeout is None else conn_timeout  # unit: minute
        self.__session_start = None     
        self._init_conn_data(conn_id)
        self._init_client()
    
    def get_conn_type(self):
        return self.__conn_type
    
    def get_conn_data(self):
        return self.__conn_data
    
    def get_conn_client(self):
        if not self._session_is_valid():
            self._init_client()
        return self.__conn_client
    
    def _call(self, *args, **kwargs):
        caller = inspect.stack()[1][3]
        called = getattr(self, f"{caller}_{self.__conn_type.lower()}")
        if called:
            return called(*args, **kwargs)
        return None
    
    def _init_conn_data(self, conn_id):
        self._call(conn_id)
    
    def _init_conn_data_abap(self, conn_id):
        self.__conn_data = ConnectionData.for_abap(conn_id, self.__ENV_FILE_ROOT_PATH)
        
    def _init_conn_data_hana(self, conn_id):
        self.__conn_data = ConnectionData.for_hana(conn_id, self.__ENV_FILE_ROOT_PATH)
        
    def _init_conn_data_adl_v2(self, conn_id):
        self.__conn_data = ConnectionData.for_datalake(conn_id, self.__ENV_FILE_ROOT_PATH)
    
    def _session_is_valid(self) -> bool:
        if not self.__session_start:
            return False
        now = datetime.now()
        expired = self.__conn_timeout
        return (not expired or self.__session_start + timedelta(minutes=expired) > now)
    
    def _init_client(self):
        self.__session_start = datetime.now()
        self._call()
        
    def _init_client_abap(self):
        self.__conn_client = CitAbapClient.connect_to(self.__conn_data)
        
    def _init_client_hana(self):
        self.__conn_client = HanaClient.connect_to(self.__conn_data)
        
    def _init_client_adl_v2(self):
        self.__conn_client = DatalakeClient.connect_to(self.__conn_data)