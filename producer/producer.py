from pykafka import KafkaClient, SslConfig
from datetime import datetime
from pickle import dumps
import time
import logging
from psutil import virtual_memory


class Producer:
    def __init__(self, kafka_host, kafka_topic, machine_identifier):
        self.KAFKA_TOPIC = kafka_topic
        self.MACHINE_IDENTIFIER = machine_identifier

        config = SslConfig(cafile='./ca.pem',
                           certfile='./service.cert',
                           keyfile='./service.key')

        self.kafka_client = KafkaClient(hosts=kafka_host, ssl_config=config)

    @staticmethod
    def get_utc_timestamp():
        return datetime.utcnow().timestamp()

    @staticmethod
    def get_os_metrics():
        vm_info = virtual_memory()
        return {
            'availableMem': vm_info.available,
            'usedMemPer': vm_info.percent
            }

    def produce(self, limit=None):
        logging.log(logging.INFO, 'connected to kafka')

        topic = self.kafka_client.topics[self.KAFKA_TOPIC]

        count = 0
        with topic.get_producer() as producer:
            while limit is None or count < limit:
                msg = {'machine': self.MACHINE_IDENTIFIER,
                       'time': Producer.get_utc_timestamp(),
                       'metrics': Producer.get_os_metrics()}
                producer.produce(dumps(msg))
                logging.log(logging.INFO, f'message #{count} sent')
                count += 1
                time.sleep(10)
