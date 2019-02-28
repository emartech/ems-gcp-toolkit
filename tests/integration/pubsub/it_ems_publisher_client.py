import time
from unittest import TestCase

from google.api_core.exceptions import NotFound, AlreadyExists
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient

from pubsub.ems_publisher_client import EmsPublisherClient
from tests.integration import GCP_PROJECT_ID


class ItEmsPublisherClient(TestCase):

    def setUp(self):
        self.ems_client = EmsPublisherClient()
        self.publisher = PublisherClient()

    def test_topic_create_if_not_exists_new_topic_creation(self):
        expected_topic_name = self.generate_test_name("topic")
        expected_topic_path = self.publisher.api.topic_path(GCP_PROJECT_ID, expected_topic_name)

        self.ems_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)

        try:
            topic = self.publisher.api.get_topic(expected_topic_path)
        except NotFound:
            self.fail(f"Topic not created with name {expected_topic_name}")

        self.assertEqual(topic.name, expected_topic_path)

        self.delete_topic(expected_topic_name)

    def test_topic_create_if_not_exists_topic_already_created(self):
        expected_topic_name = self.generate_test_name("topic")
        self.publisher.api.topic_path(GCP_PROJECT_ID, expected_topic_name)

        self.ems_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)

        try:
            self.ems_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)
        except AlreadyExists:
            self.fail(f"Topic already exists but tried to recreate with name {expected_topic_name}")

        self.delete_topic(expected_topic_name)

    def test_subscription_create(self):
        expected_topic_name = self.generate_test_name("topic")
        expected_subscription_name = self.generate_test_name("subscription")
        expected_subscription_list = ["projects/" + GCP_PROJECT_ID + "/subscriptions/" + expected_subscription_name]
        subscription_list = []

        self.ems_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)
        self.ems_client.subscription_create(GCP_PROJECT_ID, expected_topic_name, expected_subscription_name)

        try:
            topic_path = self.publisher.api.topic_path(GCP_PROJECT_ID, expected_topic_name)
            subscriptions = self.publisher.api.list_topic_subscriptions(topic_path)

            for subscription in subscriptions:
                subscription_list.append(subscription)
        except NotFound:
            self.fail(
                f"Subscription not created with topic name {expected_topic_name}," +
                f"subscription name {expected_subscription_name}"
            )

        self.assertNotEqual(len(subscription_list), 0, "Subscription list is empty")
        self.assertEqual(subscription_list, expected_subscription_list, "Subscriptions not created")

        self.delete_topic(expected_topic_name)
        self.delete_subscription(expected_subscription_name)

    def delete_topic(self, expected_topic_name):
        self.publisher.api.delete_topic(self.publisher.api.topic_path(GCP_PROJECT_ID, expected_topic_name))

    @staticmethod
    def delete_subscription(subscription_name: str):
        subscriber = SubscriberClient()
        subscription_path = subscriber.api.subscription_path(GCP_PROJECT_ID, subscription_name)
        subscriber.api.delete_subscription(subscription_path)

    @staticmethod
    def generate_test_name(context: str):
        return "test_" + context + "_" + str(int(time.time()))
