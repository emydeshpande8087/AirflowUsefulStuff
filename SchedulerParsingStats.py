from airflow.configuration import conf
from airflow.dag_processing.processor import DagFileProcessor
from airflow.operators.python import PythonOperator
import time
import logging,os
custom_logger = logging.getLogger("DagParseLogger")
from airflow import DAG
class MyCustomDagProcessor(DagFileProcessor):
    def __init__(self, dag_ids, dag_directory, custom_logger_name:logging.Logger):
        self.logger_name=custom_logger_name
        super().__init__(dag_ids, dag_directory, custom_logger_name)
    def process_file(self,file_path,callback_requests=[]):
        start_time = time.time()
        # Call the default processing logic of airflow
        result = super().process_file(file_path, callback_requests)
        # Calculate and log parse time
        end_time = time.time()
        parse_time = end_time - start_time
        #self.logger_name.info(f"Processed file: {file_path}, Parse Time: {parse_time:.2f} seconds")
        return f"Processed file: {file_path}, Parse Time: {parse_time:.2f} seconds"

 
       

def run_analytics():
    dag_path = conf.get('core', 'dags_folder') #retrieve dags folder from configuration
    custom_logger.info('dags path is  %s',dag_path)
    custom_logger.info('Folders in path are %s',os.listdir(dag_path))
    folder_list=os.listdir(dag_path)
    exclude_folder=['__pycache__'] #provide list of folders that shall be excluded from parsing
    final_list=[]
    for folder in folder_list:
        if os.path.exists(os.path.join(dag_path,folder)) and os.path.isdir(os.path.join(dag_path,folder)):            
            for root, dirs, files in os.walk(os.path.join(dag_path,folder)):
                dirs[:] = [d for d in dirs if d not in exclude_folder]
                for file in files:
                    final_list.append(os.path.join(root, file))
        else:
            print(f"Invalid folder path: {folder}")
    
    obj = MyCustomDagProcessor(custom_logger_name=custom_logger, dag_directory=dag_path, dag_ids=None)
    for filename in final_list:        
        if not os.path.isfile(filename):
            continue  # Skip non-file entries (e.g., directories)     
        custom_logger.info(obj.process_file(callback_requests=[], file_path=filename))
 
#main with clause for dag
with DAG(dag_id='scheduler_parsing_stats_collector',schedule=None,catchup=False,is_paused_upon_creation=False):
    task1=PythonOperator(task_id='test',python_callable=run_analystics)
    task1
