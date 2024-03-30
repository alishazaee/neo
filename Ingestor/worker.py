import csv
import os 
import logging
from recordproducer import *
from utils import *
from threading import Thread
from recordproducer import *
from watcher import *
from itertools import  islice

class worker(Thread):
    def __init__(self,arrivedfilesqueue:list , producer:RecordProducer , worker_id:int) -> None:
        Thread.__init__(self)
        self.arrivedfilesqueue = arrivedfilesqueue
        self.worker_id = worker_id
        self.is_active = True
        self.producer = producer
    
    def run(self):
        while(self.is_active):
            file_path = self.get_file_path()
            self.process_file(file_path=file_path)
            self.delete_file(file_path=file_path)

    
    
    def get_file_path(self):
        file_path = self.arrivedfilesqueue.get()
        time.sleep(3)
        return file_path
    
    
    def process_file(self,file_path:str):
        with open(file_path , 'r' , newline='') as csv_file:
            csv_reader = csv.reader(csv_file)
            for line in csv_reader:
                if line[0] == 'IPV4_SRC_ADDR':
                    continue
                record = Parser.parse_csv_line(line=line,record_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                self.producer.send_record(record)
    
    def delete_file(self, file_path):
        os.remove(file_path)
        self.arrivedfilesqueue.task_done()

    def deactivate(self):
        self.is_active= False
        

