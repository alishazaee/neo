from confluent_kafka import Producer

class RecordProducer:
    
    def __init__(self , topic: str , bootstrapservers:str ) -> None:
        self.topic = topic
        self.bootstrapservers = bootstrapservers
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

