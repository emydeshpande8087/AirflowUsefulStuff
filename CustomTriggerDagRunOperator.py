'''
A module provides a custom triggered dag operator designed to trigger the dags from orchestration dags. The way it differs from
original triggerdagrun operator is the population of `conf` field. In original operator the `conf` field is jinja templated and cannot be
used to call python functions inside it. This version uses a pre_execute method to call the python function and populate the conf field
and then pass it to the target dag which we want to run. 
 
 
 
'''
#----------------------------------------------------------------
# Created by : Yeshwant Deshpande
    # Date : 2025-01-16
    # Purpose : To create a custom trigger dag run operator that fetches required parameters 
    # and populates them in the conf field.
    # Note : The operator uses the pre_execute method to call the python function and populate the conf field.
#----------------------------------------------------------------
 
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import logging
 
_logger = logging.getLogger(__name__)
 
 
class CustomTriggerDagRunOperator(TriggerDagRunOperator):
    def __init__(self,
                 trigger_dag_id: str,
                 conf: dict | None = None,
                 wait_for_completion=True,
                 **kwargs):
        #initialize our conf attribute
        self.conf = conf
        #initialise parent object with all the details
        super().__init__(trigger_dag_id=trigger_dag_id, conf=conf,
                         wait_for_completion=wait_for_completion, **kwargs)
 
    def pre_execute(self, context):
        '''Preexecute method for custom logic to fetch values from database'''
        try:

            _logger.info('This is the old conf %s', self.conf)
            system_id = self.conf['system_id']
            _logger.info(
                'Updating original conf dictionary to new things by pulling it from database')
            key_one='xyz' #values from some function
            directory_name='abcdd'  #values from db
            self.conf['key_one'] = key_one
            self.conf['directory_name'] = directory_name
            # assign the updated conf back to original
            context['conf'] = self.conf
            _logger.info('Conf object updated successfully')
        except KeyError as e:
            _logger.error(
                'Exception occurred in pre_execute. More details :', e)
            raise
 
        except Exception as e:
            _logger.exception('Exception occurred while pre_executing :', e)
            raise
 
    def execute(self, context):
        '''Overridden execute method of original operator'''
        try:
            #call to execute the custom logic
            self.pre_execute(context)
            _logger.info('New updated conf for target dag is %s', self.conf)
            #call the original triggerdag operator execute method
            return super().execute(context)
        except Exception as e:
            _logger.exception('Exception occurred while executing CustomTriggerDagRunOperator. More details :', e)
            raise
