from pykafka import KafkaClient, SslConfig
import psycopg2
from pickle import loads
import logging
import json


class Consumer:
    def __init__(self, kafka_host, kafka_topic, pg_host, pg_port, pg_username, pg_password, logger=None):
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

        config = SslConfig(cafile='./ca.pem',
                           certfile='./service.cert',
                           keyfile='./service.key')

        self.kafka_client = KafkaClient(hosts=kafka_host, ssl_config=config)
        self.logger.log(logging.INFO, 'connected to kafka')

        self.pg_con = psycopg2.connect(host=pg_host, port=pg_port, user=pg_username, password=pg_password,
                                       dbname="os_metrics", sslmode='require')
        self.logger.log(logging.INFO, 'connected to pgsql')

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

                        self.logger.log(logging.INFO, f"{cur.rowcount} rows inserted")

                        # TODO: consider a better commit strategy
                        self.pg_con.commit()
                    except psycopg2.IntegrityError:
                        self.logger.log(logging.INFO, "skipping already existing row")
                        self.pg_con.rollback()

                    count += 1
                    if limit and count >= limit:
                        break
