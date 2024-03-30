import datetime
from queue import Empty, Queue
import logging
from contextlib import contextmanager
import json
@contextmanager
def exception_handler(msg:str):
    logging.basicConfig(level=logging.INFO, filename="ingestor.log",filemode="w")
    try:
        yield
    except OSError : 
         logging.error("Something happened in OS -> {msg}")
        
    except Empty:
        logging.error("queue is empty -> {msg}")
    
    except Exception:
         logging.error("Something happened -> {msg}")
    
