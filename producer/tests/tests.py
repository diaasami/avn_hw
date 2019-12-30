from unittest import TestCase, mock
from unittest.mock import Mock
import pickle
import producer


class TestingProducer(producer.Producer):
    def __init__(self, kafka_client, kafka_topic, machine_identifier):
        self.kafka_client = kafka_client
        self.KAFKA_TOPIC = kafka_topic
        self.MACHINE_IDENTIFIER = machine_identifier


class TestProducer(TestCase):
    @mock.patch('producer.Producer.get_os_metrics', return_value=None)
    @mock.patch('time.sleep', return_value=None)
    def test_producer(self, sleep, get_os_metrics):
        # setup kafka mocks
        kafka_topic = "TEST_TOPIC"

        kafka_mock = Mock()
        kafka_mock.topics = {kafka_topic: Mock()}
        kafka_producer = Mock()
        topic = kafka_mock.topics[kafka_topic]
        topic.get_producer.return_value = Mock(__enter__=kafka_producer,
                                               __exit__=Mock())

        myproducer = TestingProducer(kafka_mock, kafka_topic, "test_machine")

        myproducer.produce(limit=1)

        # Custom argument matcher
        # Attribution: https://stackoverflow.com/a/16976500
        class ValidateMessageStructure(str):
            def __eq__(self, msg):
                obj_keys = pickle.loads(msg).keys()
                return all(k in obj_keys for k in ["machine", "time", "metrics"])

        get_os_metrics.assert_called_once()
        topic.get_producer.assert_called_once()
        kafka_producer.return_value.produce.assert_called_once_with(ValidateMessageStructure())

    def test_get_metrics(self):
        output = TestingProducer.get_os_metrics()

        assert(output is not None)
        assert(len(output.keys()) > 0)

    def test_get_time(self):
        t1 = TestingProducer.get_utc_timestamp()
        t2 = TestingProducer.get_utc_timestamp()

        assert(t2 > t1)
