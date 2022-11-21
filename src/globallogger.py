import logging, queue
from pathlib import Path
import logging.handlers
from pytz import timezone
from datetime import datetime
import os

loggers = {}

def setup_custom_logger(name):    
    global loggers

    if loggers.get(name):
        return loggers.get(name)
    else:
        formatter = logging.Formatter('%(asctime)s [%(module)s.%(funcName)s:%(lineno)d] %(levelname)s %(message)s')
        formatter.converter = lambda *args: datetime.now(tz=timezone('CET')).timetuple()

        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        
        log_queue     = queue.Queue()
        queue_handler = logging.handlers.QueueHandler(log_queue)       
        #set the non-blocking handler first
        logger.addHandler(queue_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(formatter)
        
        # Create logs directory if it is missing.
        dir = Path(__file__).parent.parent.absolute().joinpath('logs')
        os.makedirs(dir, exist_ok=True)  
        file = dir.joinpath('app_rolling.log')
        timerotating_handler = logging.handlers.TimedRotatingFileHandler(file, when='D', backupCount=30)
        timerotating_handler.setLevel(logging.INFO)
        timerotating_handler.setFormatter(formatter)
        
        listener = logging.handlers.QueueListener(log_queue, stream_handler, timerotating_handler, respect_handler_level=True)
            
        listener.start()
        loggers[name] = logger

        return logger