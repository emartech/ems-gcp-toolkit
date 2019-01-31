from unittest import TestCase

from pubsub.ems_message import EmsMessage


class TestEmsMessage(TestCase):

    def test_dataIsParsedAsJson(self):
        message = EmsMessage("ackId", b'{"a":"v"}', dict())

        assert {"a": "v"} == message.data_json

    def test_constructorWontThrowOnInvalidData(self):
        EmsMessage("ackId", b"Invalid json", dict())

