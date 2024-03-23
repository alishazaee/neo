from pathlib import Path
from queue import Queue, Empty
from utils import *
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import logging
from concurrent.futures import ThreadPoolExecutor, Future
from threading import Thread


    
class Watcher(Thread): 
    
    def __init__(self , directory_path:str , arrived_files_queue:Queue ):
        Thread.__init__(self)
        self.DirectoryPath=Path(directory_path)
        if not self.DirectoryPath.exists() or not self.DirectoryPath.is_dir():
            return ValueError("this path is not available on the system")
        
        self.isActive = True
        self.arrived_files_queue = arrived_files_queue
        self.MAX_NUM_SCHEDULER_THREAD=20
        self.AWAIT_SCHEDULE_TIME_SECONDS=5
        self.executor = ThreadPoolExecutor(max_workers=self.MAX_NUM_SCHEDULER_THREAD)

    def run(self):
        handler = PatternMatchingEventHandler(patterns=['*.csv'],ignore_directories=True)
        handler.on_modified = self._file_created_or_deleted
        self.observer = Observer()
        self.observer.schedule(event_handler=handler,path=self.DirectoryPath,recursive=False)
        self.observer.start()
        try:
            while True:
                if self.isActive == False:
                    break
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()
        
            
    def add_to_queue(self,  file_path , delay=0):  
        time.sleep(delay)
        self.arrived_files_queue.put(file_path)
        return self.arrived_files_queue
        
            
        
    def _file_created_or_deleted(self,event):
        if not self.isActive:
            return
        
        file_path= event.src_path
        if event.event_type == "created":
            print(f'New file found : {file_path}')
        elif event.event_type == "modified":
            print(f'File modfied : {file_path}')
        self.add_to_queue(file_path=file_path)
        

    
    def deactivate(self):
        self.isActive =False
        self.observer.stop()
        self.observer.join()
        print("Deactivated Watcher")

