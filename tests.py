import unittest
from unittest import mock
from unittest.mock import Mock
import producer as producer_module
import consumer as consumer_module
from pickle import loads, dumps


class TestProducer(unittest.TestCase):
    @mock.patch('producer.get_metrics', return_value={})
    @mock.patch('time.sleep', return_value=None)
    def test_producer(self, sleep, get_metrics):
        kafka_mock = Mock()
        kafka_mock.topics = {producer_module.KAFKA_TOPIC: Mock()}
        kafka_producer = Mock()
        kafka_producer.produce.return_value = None
        topic = kafka_mock.topics[producer_module.KAFKA_TOPIC]
        topic.get_producer.return_value = Mock(__enter__=kafka_producer,
                                               __exit__=Mock())

        producer_module.produce(kafka_mock, 1)

        # Custom argument matcher
        # Attribution: https://stackoverflow.com/a/16976500
        class ValidateMessageStructure(str):
            def __eq__(self, msg):
                obj_keys = loads(msg).keys()
                return all(k in obj_keys for k in ["machine", "time", "metrics"])

        get_metrics.assert_called_once()
        topic.get_producer.assert_called_once()
        kafka_producer.return_value.produce.assert_called_once_with(ValidateMessageStructure())

    def test_get_metrics(self):
        output = producer_module.get_metrics()

        assert(output is not None)
        assert(len(output.keys()) > 0)

    def test_get_time(self):
        t1 = producer_module.get_time()
        t2 = producer_module.get_time()

        assert(t2 > t1)


class TestConsumer(unittest.TestCase):
    def test_consumer(self):
        # setup kafka mocks
        kafka_mock = Mock()
        kafka_consumer_mock = Mock()
        topic = Mock()

        kafka_mock.topics = {consumer_module.KAFKA_TOPIC: topic}
        topic.get_simple_consumer.return_value = [Mock(value=dumps({'machine': 'm', 'time': 1, 'metrics': {}}))]

        # setup pgsql mocks
        pgsql_mock = Mock()
        cursor_mock = Mock()

        pgsql_mock.cursor.return_value = Mock(__enter__=cursor_mock,
                                              __exit__=Mock())

        consumer_module.consume_and_insert_in_db(kafka_mock, pgsql_mock, 1)

        # Custom argument matcher
        # Attribution: https://stackoverflow.com/a/16976500
        class ValidateInsertQuery(str):
            def __eq__(self, q):
                return q.lower().startswith("insert ")

        # TODO: Not possible to validate a native object parameter, for now ANY was used
        cursor_mock.return_value.execute.assert_called_once_with(ValidateInsertQuery(), (mock.ANY, 'm', '{}'))


if __name__ == '__main__':
    unittest.main()
