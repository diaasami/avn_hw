from pykafka import KafkaClient, SslConfig
import psycopg2
from pickle import loads
import logging
from os import getenv
import json

KAFKA_HOST = getenv('KAFKA_HOST')
KAFKA_TOPIC = getenv('KAFKA_TOPIC', "os_metrics")

PG_HOST = getenv("PG_HOST")
PG_PORT = getenv("PG_PORT")
PG_USERNAME = getenv("PG_USERNAME")
PG_PASSWORD = getenv("PG_PASSWORD")


def consume_and_insert_in_db(kafka_client, pg_con, limit=None):
    topic = kafka_client.topics[KAFKA_TOPIC]

    consumer = topic.get_simple_consumer()

    count = 0
    with pg_con.cursor() as cur:
        for message in consumer:
            if message is not None:
                obj_msg = loads(message.value)
                # print(message.offset, objMsg)

                try:
                    cur.execute("INSERT INTO metrics "
                                "VALUES(%s, %s, %s);",
                                (psycopg2.TimestampFromTicks(obj_msg['time']), obj_msg['machine'],
                                 json.dumps(obj_msg['metrics'])))

                    logging.log(logging.INFO, f"{cur.rowcount} rows inserted")
                    # TODO: consider a better commit strategy
                    pg_con.commit()
                except psycopg2.IntegrityError as e:
                    logging.log(logging.INFO, "skipping already existing row")
                    pg_con.rollback()

                count += 1
                if limit and count >= limit:
                    break


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    config = SslConfig(cafile='./ca.pem',
                       certfile='./service.cert',
                       keyfile='./service.key')

    client = KafkaClient(hosts=KAFKA_HOST, ssl_config=config)
    logging.log(logging.INFO, 'connected to kafka')

    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USERNAME, password=PG_PASSWORD, dbname="os_metrics",
                            sslmode='require')
    logging.log(logging.INFO, 'connected to pgsql')

    consume_and_insert_in_db(client, conn)
