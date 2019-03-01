import time
from unittest import TestCase

from google.api_core.exceptions import NotFound, AlreadyExists
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient

from pubsub.ems_publisher_client import EmsPublisherClient
from tests.integration import GCP_PROJECT_ID


class ItEmsPublisherClient(TestCase):

    def setUp(self):
        self.__ems_publisher_client = EmsPublisherClient()
        self.__publisher_client = PublisherClient()

    def test_topic_create_if_not_exists_new_topic_creation(self):
        expected_topic_name = self.__generate_test_name("topic")
        expected_topic_path = self.__publisher_client.api.topic_path(GCP_PROJECT_ID, expected_topic_name)

        self.__ems_publisher_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)

        try:
            topic = self.__publisher_client.api.get_topic(expected_topic_path)
        except NotFound:
            self.fail(f"Topic not created with name {expected_topic_name}")

        self.assertEqual(topic.name, expected_topic_path)

        self.__delete_topic(expected_topic_name)

    def test_topic_create_if_not_exists_topic_already_created(self):
        expected_topic_name = self.__generate_test_name("topic")

        self.__ems_publisher_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)

        try:
            self.__ems_publisher_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)
        except AlreadyExists:
            self.fail(f"Topic already exists but tried to recreate with name {expected_topic_name}")

        self.__delete_topic(expected_topic_name)

    def test_subscription_create(self):
        expected_topic_name = self.__generate_test_name("topic")
        expected_subscription_name = self.__generate_test_name("subscription")
        expected_subscription_list = ["projects/" + GCP_PROJECT_ID + "/subscriptions/" + expected_subscription_name]
        subscription_list = []

        self.__ems_publisher_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)
        self.__ems_publisher_client.subscription_create(GCP_PROJECT_ID, expected_topic_name, expected_subscription_name)

        try:
            topic_path = self.__publisher_client.api.topic_path(GCP_PROJECT_ID, expected_topic_name)
            subscriptions = self.__publisher_client.api.list_topic_subscriptions(topic_path)

            for subscription in subscriptions:
                subscription_list.append(subscription)
        except NotFound:
            self.fail(
                f"Subscription not created with topic name {expected_topic_name}," +
                f"subscription name {expected_subscription_name}"
            )

        self.assertNotEqual(len(subscription_list), 0, "Subscription list is empty")
        self.assertEqual(subscription_list, expected_subscription_list, "Subscriptions not created")

        self.__delete_subscription(expected_subscription_name)
        self.__delete_topic(expected_topic_name)

    def test_delete_topic_if_exists(self):
        topic_name = self.__generate_test_name("topic")

        self.__ems_publisher_client.topic_create_if_not_exists(GCP_PROJECT_ID, topic_name)
        self.__ems_publisher_client.delete_topic_if_exists(GCP_PROJECT_ID, topic_name)
        try:
            self.__ems_publisher_client.delete_topic_if_exists(GCP_PROJECT_ID, topic_name)
        except NotFound:
            self.fail("delete_topic_if_exists raised NotFound error")

        topic_path = self.__publisher_client.api.topic_path(GCP_PROJECT_ID, topic_name)
        with self.assertRaises(NotFound):
            self.__publisher_client.api.get_topic(topic_path)

    def __delete_topic(self, topic_name):
        topic_path = self.__publisher_client.api.topic_path(GCP_PROJECT_ID, topic_name)
        self.__publisher_client.api.delete_topic(topic_path)

    @staticmethod
    def __delete_subscription(subscription_name: str):
        subscriber_client = SubscriberClient()
        subscription_path = subscriber_client.api.subscription_path(GCP_PROJECT_ID, subscription_name)
        subscriber_client.api.delete_subscription(subscription_path)

    @staticmethod
    def __generate_test_name(context: str):
        return "test_" + context + "_" + str(int(time.time()))
