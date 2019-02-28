from unittest import TestCase

from pubsub.ems_message import EmsMessage


class TestEmsMessage(TestCase):

    @staticmethod
    def test_dataIsParsedAsJson():
        # pylint: disable=invalid-string-quote
        message = EmsMessage("ackId", b'{"a":"v"}', dict())

        assert {"a": "v"} == message.data_json

    @staticmethod
    def test_constructorWontThrowOnInvalidData():
        EmsMessage("ackId", b"Invalid json", dict())
