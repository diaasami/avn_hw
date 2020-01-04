from os import getenv
from socket import gethostname
import logging
from producer import Producer


logging.basicConfig(level=logging.INFO)

myproducer = Producer(kafka_host=getenv("KAFKA_HOST"),
                      kafka_topic=getenv("KAFKA_TOPIC"),
                      machine_identifier=gethostname())
myproducer.produce()