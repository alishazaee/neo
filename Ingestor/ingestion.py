from watcher import *
from worker import *
from recordproducer import *
from dotenv import load_dotenv
import os
class ingestor: 
    def __init__(self, directory_path:str, thread_count:int , producer:RecordProducer) -> None:
        self.directory_path= directory_path
        self.arrived_files_queue = Queue()
        self.thread_count = thread_count
        self.producer = producer
    
    def start(self):
        watcher=Watcher(directory_path=self.directory_path , arrived_files_queue=self.arrived_files_queue)
        watcher.start()
        workers = worker(arrivedfilesqueue=self.arrived_files_queue , producer=self.producer , worker_id=1)
        workers.start()

if __name__ == "__main__":
    load_dotenv()
    producer=RecordProducer(topic=os.getenv('TOPIC_NAME') , bootstrapservers=os.getenv('BOOTSTRAP_SERVERS'))
    Ingest=ingestor(directory_path=os.getenv('DIR_PATH') , thread_count=int(os.getenv('THREAD_COUNT')) , producer=producer)
    Ingest.start()