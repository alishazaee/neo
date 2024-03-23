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
    

class NetRecord:
    def __init__(self , date : datetime.datetime , ipv4_src_addr:str , l4_src_port:int , ipv4_dst_addr:str , l4_dst_port: int , inbyte:int  , outbyte:int ) -> None:
        self.date = date
        self.ipv4_src_addr = ipv4_src_addr
        self.l4_src_port = l4_src_port
        self.ipv4_dst_addr = ipv4_dst_addr
        self.l4_dst_port = l4_dst_port
        self.inbyte= inbyte
        self.outbyte = outbyte
        
    def to_json(self):
        return json.dumps(self.__dict__)


class Parser:
    @staticmethod
    def parse_csv_line(line , record_time) -> NetRecord:
        ipv4_src_addr= line[0]
        l4_src_port=line[1]
        ipv4_dst_addr=line[2]
        l4_dst_port = line[3]
        inbyte = line[6]
        outbyte=line[8]
        
        return NetRecord(
            date=record_time,
            ipv4_src_addr=ipv4_src_addr,
            l4_src_port=l4_src_port,
            ipv4_dst_addr=ipv4_dst_addr,
            l4_dst_port=l4_dst_port,
            inbyte=inbyte,
            outbyte=outbyte
        )
        
        
    
