import logging
from consumer import Consumer
from os import getenv


logging.basicConfig(level=logging.INFO)

myconsumer = Consumer(kafka_host=getenv('KAFKA_HOST'),
                      kafka_topic=getenv('KAFKA_TOPIC'),
                      pg_host=getenv("PG_HOST"),
                      pg_port=getenv("PG_PORT"),
                      pg_username=getenv("PG_USERNAME"),
                      pg_password=getenv("PG_PASSWORD"))
myconsumer.consume_and_insert_in_db()
