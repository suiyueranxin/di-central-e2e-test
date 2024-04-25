from di_embedded_api_test.components.rms.task import TaskEntity
from di_embedded_api_test.components.rms.space import Space
from di_embedded_api_test.components.rms.constellation import Constellation
from di_embedded_api_test.utils.retry import Retry
from di_embedded_api_test.components.rms.rms import Rms
    
class Task:
    
    def __init__(self, rms:Rms, name, runtime_instance:TaskEntity=None):
        self._rms = rms
        self._name = name
        self._runtime_instance = runtime_instance
    
    @property
    def name(self):
        return self._name

    @property
    def source_space(self):
        return f"{self._name}_SOURCE_SPACE"
        
    @property
    def target_space(self):
        return f"{self._name}_TARGET_SPACE"

    @property
    def constellation(self):
        return f"{self._name}_CONSTELLATION"
    
    @property
    def rms(self) -> Rms:
        return self._rms

    @property
    def runtime_instance(self):
        return self._runtime_instance
    
    @runtime_instance.setter
    def runtime_instance(self, task_entity:TaskEntity):
        self._runtime_instance = task_entity
    
    @Retry()
    def _wait_until_task_not_exist(self, max_retries: int = 3, timeout: float = 1):
        if self._is_task_exist():
            raise RuntimeError(f"Task {self._name} still exists after delete action, need to wait deleting")
        
    def _is_task_exist(self):
        return self.rms.get_task_id_by_name(self.name, True) is not None
    
    def _is_space_exist(self, space_name):
        return self.rms._get_resource_id_by_name(Space, space_name, True) is not None

    def _is_constellation_exist(self):
        return self.rms._get_resource_id_by_name(Constellation, self.constellation, True) is not None

    def cleanup(self):
        #TODO: it's not safe now
        # instance: TaskEntity = self._runtime_instance
        # if instance:
        #     instance.cleanup() 
        
        if self._is_task_exist():
            self.rms.delete_task_by_name(self.name)
            self._wait_until_task_not_exist()
        if self._is_constellation_exist():
            self.rms.delete_constellation_by_name(self.constellation)
        if self._is_space_exist(self.target_space):
            self.rms.delete_space_by_name(self.target_space)
        if self._is_space_exist(self.source_space):   
            self.rms.delete_space_by_name(self.source_space)

    def get_task_status_by_name(self):
        return self.rms.get_task_status_by_name(self.name, True)
            