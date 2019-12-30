from unittest import TestCase, mock
from unittest.mock import Mock
import pickle
from consumer import Consumer


class TestingConsumer(Consumer):
    def __init__(self, kafka_client, kafka_topic, pg_conn):
        self.KAFKA_TOPIC = kafka_topic
        self.kafka_client = kafka_client
        self.pg_con = pg_conn


class TestConsumer(TestCase):
    def test_consumer(self):
        # setup kafka mocks
        kafka_topic = "TEST_TOPIC"
        kafka_mock = Mock()
        topic = Mock()

        kafka_mock.topics = {kafka_topic: topic}
        topic.get_simple_consumer.return_value = [Mock(value=pickle.dumps(
            {'machine': 'm', 'time': 1, 'metrics': {}}))]

        # setup pgsql mocks
        pgsql_mock = Mock()
        cursor_mock = Mock()

        pgsql_mock.cursor.return_value = Mock(__enter__=cursor_mock,
                                              __exit__=Mock())

        consumer = TestingConsumer(kafka_client=kafka_mock,
                                   kafka_topic=kafka_topic,
                                   pg_conn=pgsql_mock)
        consumer.consume_and_insert_in_db(limit=1)

        # Custom argument matcher
        # Attribution: https://stackoverflow.com/a/16976500
        class ValidateInsertQuery(str):
            def __eq__(self, q):
                return q.lower().startswith("insert ")

        # TODO: Not possible to validate a native object parameter, for now
        # ANY was used
        cursor_mock.return_value.execute \
            .assert_called_once_with(ValidateInsertQuery(),
                                     (mock.ANY, 'm', '{}'))
