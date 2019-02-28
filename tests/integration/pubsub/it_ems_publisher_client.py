import time
from unittest import TestCase

from google.api_core.exceptions import NotFound, AlreadyExists
from google.cloud.pubsub_v1 import PublisherClient

from pubsub.ems_publisher_client import EmsPublisherClient
from tests.integration import GCP_PROJECT_ID


class ItEmsPublisherClient(TestCase):

    def setUp(self):
        self.ems_client = EmsPublisherClient()

    def test_topic_create_if_not_exists_new_topic_creation(self):
        publisher = PublisherClient()
        expected_topic_name = self.generate_test_name("topic")
        expected_topic_path = publisher.api.topic_path(GCP_PROJECT_ID, expected_topic_name)

        self.ems_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)

        try:
            topic = publisher.api.get_topic(expected_topic_path)
        except NotFound:
            self.fail(f"Topic not created with name {expected_topic_name}")

        self.assertEqual(topic.name, expected_topic_path)

    def test_topic_create_if_not_exists_topic_already_created(self):
        publisher = PublisherClient()
        expected_topic_name = self.generate_test_name("topic")
        publisher.api.topic_path(GCP_PROJECT_ID, expected_topic_name)

        self.ems_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)

        try:
            self.ems_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)
        except AlreadyExists:
            self.fail(f"Topic already exists but tried to recreate with name {expected_topic_name}")

    def test_subscription_create(self):
        publisher = PublisherClient()
        expected_topic_name = self.generate_test_name("topic")
        expected_subscription_name = self.generate_test_name("subscription")
        expected_subscription_list = ["projects/" + GCP_PROJECT_ID + "/subscriptions/" + expected_subscription_name]
        subscription_list = []

        self.ems_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)
        self.ems_client.subscription_create(GCP_PROJECT_ID, expected_topic_name, expected_subscription_name)

        try:
            topic_path = publisher.api.topic_path(GCP_PROJECT_ID, expected_topic_name)
            subscriptions = publisher.api.list_topic_subscriptions(topic_path)

            for subscription in subscriptions:
                subscription_list.append(subscription)
        except NotFound:
            self.fail(
                f"Subscription not created with topic name {expected_topic_name}," +
                f"subscription name {expected_subscription_name}"
            )

        self.assertNotEqual(len(subscription_list), 0, "Subscription list is empty")
        self.assertEqual(subscription_list, expected_subscription_list, "Subscriptions not created")

    @staticmethod
    def generate_test_name(context: str):
        return "test_" + context + "_" + str(int(time.time()))
