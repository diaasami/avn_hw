from pykafka import KafkaClient, SslConfig
from socket import gethostname
from datetime import datetime
from pickle import dumps
from psutil import virtual_memory
import time
import logging
from os import getenv

KAFKA_HOST = getenv('KAFKA_HOST')
KAFKA_TOPIC = getenv('KAFKA_TOPIC', "os_metrics")
MACHINE_IDENTIFIER = gethostname()


def get_utc_timestamp():
    return datetime.utcnow().timestamp()


def get_os_metrics():
    vm_info = virtual_memory()
    return {
        'availableMem': vm_info.available,
        'usedMemPer': vm_info.percent
        }


def produce(kafka_client, limit=None):
    logging.log(logging.INFO, 'connected to kafka')

    topic = kafka_client.topics[KAFKA_TOPIC]

    count = 0
    with topic.get_producer() as producer:
        while limit is None or count < limit:
            msg = {'machine': MACHINE_IDENTIFIER, 'time': get_utc_timestamp(), 'metrics': get_os_metrics()}
            producer.produce(dumps(msg))
            logging.log(logging.INFO, f'message #{count} sent')
            count += 1
            time.sleep(10)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    config = SslConfig(cafile='./ca.pem',
                       certfile='./service.cert',
                       keyfile='./service.key')

    client = KafkaClient(hosts=KAFKA_HOST, ssl_config=config)

    produce(client)


