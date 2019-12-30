from pykafka import KafkaClient, SslConfig
import psycopg2
from pickle import loads
import logging
from os import getenv
import json


class Consumer:
    def __init__(self, kafka_host, kafka_topic, pg_host, pg_port, pg_username, pg_password):
        config = SslConfig(cafile='./ca.pem',
                           certfile='./service.cert',
                           keyfile='./service.key')

        self.kafka_client = KafkaClient(hosts=self.kafka_host, ssl_config=config)
        logging.log(logging.INFO, 'connected to kafka')

        self.pg_con = psycopg2.connect(host=pg_host, port=pg_port, user=pg_username, password=pg_password,
                                       dbname="os_metrics", sslmode='require')
        logging.log(logging.INFO, 'connected to pgsql')

        self.KAFKA_TOPIC = kafka_topic

    def consume_and_insert_in_db(self, limit=None):
        topic = self.kafka_client.topics[self.KAFKA_TOPIC]

        consumer = topic.get_simple_consumer()

        count = 0
        with self.pg_con.cursor() as cur:
            for message in consumer:
                if message is not None:
                    obj_msg = loads(message.value)

                    try:
                        # TODO: batch insertion
                        cur.execute("INSERT INTO metrics "
                                    "VALUES(%s, %s, %s);",
                                    (psycopg2.TimestampFromTicks(obj_msg['time']), obj_msg['machine'],
                                     json.dumps(obj_msg['metrics'])))

                        logging.log(logging.INFO, f"{cur.rowcount} rows inserted")

                        # TODO: consider a better commit strategy
                        self.pg_con.commit()
                    except psycopg2.IntegrityError:
                        logging.log(logging.INFO, "skipping already existing row")
                        self.pg_con.rollback()

                    count += 1
                    if limit and count >= limit:
                        break


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    myconsumer = Consumer(kafka_host=getenv('KAFKA_HOST'),
                          kafka_topic=getenv('KAFKA_TOPIC'),
                          pg_host=getenv("PG_HOST"),
                          pg_port=getenv("PG_PORT"),
                          pg_username=getenv("PG_USERNAME"),
                          pg_password=getenv("PG_PASSWORD"))
    myconsumer.consume_and_insert_in_db()
