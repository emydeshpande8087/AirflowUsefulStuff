from airflow.utils.db import provide_session
import logging 
_logger=logging.getLogger(__name__)

@provide_session #to pass in the db connection/session
def delete_xcoms_for_dag_id(dag_id, session=None):
    from airflow.models import XCom
    _logger.info("Deleting XCOM for DAG ID : %s", dag_id)
    session.query(XCom).filter(XCom.dag_id == dag_id).delete() #pull all the xcoms and delete it
    session.commit()
 
delete_xcoms_for_dag_id(dag_id='your_dag_id')
