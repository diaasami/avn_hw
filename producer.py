from pykafka import KafkaClient, SslConfig
from socket import gethostname
from datetime import datetime
from pickle import dumps
from time import sleep
import logging
from os import getenv

KAFKA_HOST = getenv('KAFKA_HOST')
KAFKA_TOPIC = getenv('KAFKA_TOPIC', "os_metrics")
MACHINE_IDENTIFIER = gethostname()

logging.basicConfig(level=logging.INFO)


def get_time():
    return datetime.utcnow().timestamp()


def get_metrics():
    with open('/proc/meminfo') as memInfo:
        lines = memInfo.readlines()
        return {
            'totalMem': int(lines[0].split()[1]),
            'freeMem': int(lines[1].split()[1])
            }


if __name__ == '__main__':
    config = SslConfig(cafile='./ca.pem',
                       certfile='./service.cert',
                       keyfile='./service.key')

    client = KafkaClient(hosts=KAFKA_HOST, ssl_config=config)
    logging.log(logging.INFO, 'connected to kafka')

    topic = client.topics[KAFKA_TOPIC]

    count = 0
    with topic.get_producer() as producer:
        while True:
            msg = {'machine': MACHINE_IDENTIFIER, 'time': get_time(), 'metrics': get_metrics()}
            producer.produce(dumps(msg))
            logging.log(logging.INFO, f'message #{count} sent')
            count += 1
            sleep(10)

