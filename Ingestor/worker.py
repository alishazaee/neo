import csv
import os 
import logging
from recordproducer import *
from utils import *
from threading import Thread
from recordproducer import *
from watcher import *
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
        time.sleep(6)
        self.arrivedfilesqueue.task_done()
        return file_path
    
    
    def process_file(self,file_path:str):
        with open(file_path , 'r' , newline='') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)
            for line in csv_reader:
                record = Parser.parse_csv_line(line=line,record_time=datetime.datetime.now().strftime('%Y-%m-%d'))
                self.producer.send_record(record)
    
    def delete_file(self, file_path):
        os.remove(file_path)    
    
    def deactivate(self):
        self.is_active= False
        

if __name__ == '__main__':
    queue = Queue()
    w = Watcher(directory_path='/home/alisha/Desktop/',arrived_files_queue=queue)
    w.start()
    
    s = worker(arrivedfilesqueue=queue , producer=RecordProducer(bootstrapservers="127.0.0.1:9092", topic="test") , worker_id=100)
    s.start()