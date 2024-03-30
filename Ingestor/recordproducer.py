from confluent_kafka import Producer
import json
import datetime

class NetRecord:
    def __init__(self, date: datetime.datetime, ipv4_src_addr: str, l4_src_port: int, ipv4_dst_addr: str,
                 l4_dst_port: int, inbyte: int, outbyte: int) -> None:
        self.date = date
        self.ipv4_src_addr = ipv4_src_addr
        self.l4_src_port = l4_src_port
        self.ipv4_dst_addr = ipv4_dst_addr
        self.l4_dst_port = l4_dst_port
        self.inbyte = inbyte
        self.outbyte = outbyte

    def to_json(self):
        return json.dumps(self.__dict__)


class Parser:
    @staticmethod
    def parse_csv_line(line, record_time) -> NetRecord:
        ipv4_src_addr = line[0]
        l4_src_port = line[1]
        ipv4_dst_addr = line[2]
        l4_dst_port = line[3]
        inbyte = line[6]
        outbyte = line[8]

        return NetRecord(
            date=record_time,
            ipv4_src_addr=ipv4_src_addr,
            l4_src_port=l4_src_port,
            ipv4_dst_addr=ipv4_dst_addr,
            l4_dst_port=l4_dst_port,
            inbyte=inbyte,
            outbyte=outbyte
        )


class RecordProducer:
    
    def __init__(self , topic: str , bootstrapservers:str ) -> None:
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': bootstrapservers,
            'batch.size': 1000,  
            'linger.ms': 5,  
            'compression.type': 'gzip', 
            'acks': '1',  
            'max.in.flight.requests.per.connection': 5,  
            'retries': 5, 
            'retry.backoff.ms': 300 
        })

    def send_record(self , record):
        self.producer.produce(topic = self.topic , key = record.ipv4_src_addr , value = record.to_json() , on_delivery=self.acked )
        self.producer.poll(1)
    
    def acked(self,err, msg):
        if err is not None:
             print(f"Failed to deliver message: {err.str()}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

