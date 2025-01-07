# Author :  Yeshwant deshpande
# Purpose : This file contains customised callbacks for the kubernetes pod operator which will be called based
#           on the state of the pod such as created , completed ,resumed ,failed etc.
 
#imports
from airflow.providers.cncf.kubernetes.callbacks import KubernetesPodOperatorCallback
import logging
#create a logger
_logger=logging.getLogger(__name__)
 
 
class MyCustomKubernetesCallbacks(KubernetesPodOperatorCallback): #inherit the podcallback class
       
    @staticmethod
    def on_pod_starting(*, pod, client, mode, **kwargs):
        _logger.info('Execute a custom code before  starting the pod execution.')
        pass
   
   
    @staticmethod
    def on_pod_completion(*, pod, client, mode, **kwargs):
        _logger.info('Execute a custom code on pod completion')
        pass
   
    @staticmethod
    def on_pod_cleanup(*, pod, client, mode: str, **kwargs):
        _logger.info(' Execute a custom code on pod cleanup')
        pass
   
    @staticmethod
    def on_pod_creation(*, pod, client, mode, **kwargs):
        _logger.info(' Execute a custom code on pod creation')
        pass
   
    @staticmethod
    def on_operator_resuming(*, pod, event, client, mode, **kwargs):
        _logger.info(' Execute a custom code on operator resuming')
        pass
   
 
    @staticmethod
    def on_sync_client_creation(*, client, **kwargs) -> None:
        _logger.info('Invoke this callback after creating the sync client.')
        pass
       
    @staticmethod
    def progress_callback(*, line, client, mode, **kwargs):
        _logger.info('Execute on every line written in log')
        pass
   
   
 
# --------example usage of the custom callback class, add in your main dag file --------
example_usage = KubernetesPodOperator(
    namespace='namespace_test',
    image='your image url',
    cmds=['if_any_command'],
 
    name="example_usage",
    task_id="example_usage",
    labels={"nodelabel": 'testnodelabel'},
    affinity=get_affinity_and_toleration()[0],  # get affinity
    tolerations=get_affinity_and_toleration()[1],  # get toleration
    env_vars={},
    volumes=[],
    volume_mounts=[],
    is_delete_operator_pod='Y',
    get_logs=True,
    dag=dag,
    callbacks=MyCustomKubernetesCallbacks()  # provide the custom callback class object.
)
