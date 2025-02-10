from cronsim import CronSim
from typing import Mapping, Any
from airflow.models.baseoperator import BaseOperator
from datetime import datetime,timedelta
import logging
import time
_logger = logging.getLogger('cron_schedule_watch_operator')
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
 
class CronScheduleWatchOperator(BaseOperator):
    def __init__(self, op_kwargs: Mapping[str, Any] | None = None,**kwargs):
        super().__init__(**kwargs)
        self.system_schedule_dict = op_kwargs['system_schedule_dict']
        if op_kwargs['window_size']:
            self.window_size = timedelta(minutes=op_kwargs['window_size'])
        else:
            self.window_size = timedelta(minutes=2)
        self.target_dag_id = op_kwargs['target_dag_id']
        self.fired_systems_tracker=[]
    def execute(self, context):
        mins_passed_count=0
        _logger.info("Will poke every 30 secs for 24 hrs")
        while mins_passed_count<1440: #run this loop for 24 hrs max
            _logger.debug("Mins passed count in this iteration is :",mins_passed_count)
            for system,schedule in self.system_schedule_dict.items():            
                current_time=datetime.now().replace(second=0, microsecond=0)
                next_scheduled = next(CronSim(schedule, current_time))
                prev_scheduled = next(CronSim(schedule, current_time, reverse=True))
                window_start_time=current_time
                window_end_time=(current_time+self.window_size)
                _logger.debug(f"Next scheduled time: {next_scheduled}")
                _logger.debug(f"Previous scheduled time: {prev_scheduled}")
                _logger.debug(f"Window Start Time: {window_start_time}")
                _logger.debug(f"Window End Time: {window_end_time}")
                _logger.info(f"Checking if next schedule for system {system} at {next_scheduled} falls within start time : {window_start_time} and end time:{window_end_time}")
                is_within = window_start_time <= next_scheduled <= window_end_time
                _logger.debug('Is scheduled within window ? ',is_within)
                if is_within:
                    if system not in self.fired_systems_tracker:
                        _logger.info(f"Triggering SYSTEM_ID {system} as per schedule {next_scheduled} at {current_time}")
                        triggerer=TriggerDagRunOperator(task_id=f'{system}_target_dag',trigger_dag_id=self.target_dag_id,wait_for_completion=False,conf={'SYSTEM_ID':system})
                        triggerer.execute(context)
                        _logger.info(f"Marking {system} system as completed")
                        self.fired_systems_tracker.append(system)
                   
                else:
                    _logger.debug(f'Waiting till next scheduled time ....')
            _logger.debug('sleeping for 30 sec ')
            time.sleep(30)
            mins_passed_count+=1
