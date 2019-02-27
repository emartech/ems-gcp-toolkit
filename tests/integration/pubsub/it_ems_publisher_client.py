import time
from unittest import TestCase

from google.api_core.exceptions import NotFound
from google.cloud.pubsub_v1 import PublisherClient

from pubsub.ems_publisher_client import EmsPublisherClient
from tests.integration import GCP_PROJECT_ID


class ItEmsPublisherClient(TestCase):

    def setUp(self):
        self.ems_client = EmsPublisherClient()

    def test_topic_create(self):
        publisher = PublisherClient()
        expected_topic_name = "test_topic" + str(int(time.time()))
        expected_topic_path = publisher.api.topic_path(GCP_PROJECT_ID, expected_topic_name)

        self.ems_client.topic_create(GCP_PROJECT_ID, expected_topic_name)

        try:
            topic = publisher.api.get_topic(expected_topic_path)
        except NotFound:
            self.fail(f"Topic not created with name {expected_topic_name}")

        self.assertEqual(topic.name, expected_topic_path)
