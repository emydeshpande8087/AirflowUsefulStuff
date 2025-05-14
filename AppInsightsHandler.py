import logging
from opencensus.ext.azure.log_exporter import AzureLogHandler, AzureEventHandler

 
 
fetchedInstrumentKey= {put your instrumentation key here }
def getLogger(level=logging.DEBUG,enableAppInsights=False):
    #Reduce noisy loggers
    logging.basicConfig(level=logging.DEBUG,format="%(levelname)s:%(name)s:%(filename)s:%(message)s")
    logging.getLogger('azure.identity').setLevel(logging.ERROR)
    logging.getLogger('azure.core').setLevel(logging.ERROR)
    logging.getLogger('snowflake.connector').setLevel(logging.ERROR)
    logger = logging.getLogger("mlx_trace")    
    logger.setLevel(level=level)
    if enableAppInsights:
        if not any(isinstance(h, AzureLogHandler) for h in logger.handlers):
            connection_string = fetchedInstrumentKey
            azure_handler = AzureLogHandler(connection_string=connection_string)
            azure_handler.setLevel(logging.DEBUG)  # This is critical
            logger.addHandler(azure_handler)
 
    return logger
 
 
 
def log_event(msg,**kwargs):
    _logger=logging.getLogger("mlx_event")
    if not _logger.handlers:  # Only add handler if not already present
        connection_string = fetchedInstrumentKey
        event_handler = AzureEventHandler(connection_string=connection_string)
        _logger.addHandler(event_handler)
    _logger.info(msg=msg,extra={'custom_dimensions': kwargs})
