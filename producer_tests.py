import unittest
from unittest import mock
from unittest.mock import Mock
import producer as producer_module
from pickle import loads


class TestProducer(unittest.TestCase):

    @mock.patch('producer.get_metrics')
    def test_producer(self, get_metrics):
        get_metrics.return_value = {}
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


if __name__ == '__main__':
    unittest.main()
