'''
This module provides utilty functions to search for a dag run or dag instance based on certain conditions.
It uses airflow's public interface to query metadata db.
'''
 
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.models import DagRun
import logging
_logger=logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)
 
@provide_session
def FindDagRuns(dag_id:str,state_to_find:str,session=None)->list:
    """ Find all dag runs with the dag_id in the given state and return
 
    Args:
        dag_id (str): dag id to search for
        state_to_find (str): state to search for ex running , failed etc
        session (_type_, optional): . Defaults to None.
 
    Returns:
        list: list of collected dag runs
    """    
    try:
        _logger.info(f'Finding DAG runs for dag_id : {dag_id} with state as : {state_to_find}')
        all_dag_runs=session.query(DagRun).filter(DagRun.dag_id==dag_id,DagRun.state==state_to_find).all()
        _logger.info('Number of instances found = %s',len(all_dag_runs))
        return all_dag_runs if all_dag_runs else []
    except :
        _logger.exception('Failed to retrieve dag runs from the metadata db',exc_info=True)
        raise
 
 
@provide_session
def GetXcomOfDagRun(dag_run_id:str,session=None):
    """Get Xcom data for a given dag run id
 
    Args:
        dag_run_id (str): dag run id of a specific instance
        session (_type_, optional): _description_. Defaults to None.
 
    Returns:
        _type_: Xcom data
    """    
    try:
        _logger.info('Getting Xcom data for Dag_run_id: %s', dag_run_id)
        xcom_data=session.query(XCom).filter(XCom.run_id==dag_run_id )               
        return xcom_data if xcom_data  else []
    except :
        _logger.exception('Failed to retrieve Xcom from the metadata db',exc_info=True)
        raise  
