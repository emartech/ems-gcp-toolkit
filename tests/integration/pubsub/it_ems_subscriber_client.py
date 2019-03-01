import time
from unittest import TestCase

from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient

from pubsub.ems_publisher_client import EmsPublisherClient
from pubsub.ems_subscriber_client import EmsSubscriberClient
from tests.integration import GCP_PROJECT_ID


class ItEmsSubscriberClient(TestCase):

    def setUp(self):
        self.__ems_publisher_client = EmsPublisherClient()
        self.__ems_subscriber_client = EmsSubscriberClient()
        self.__publisher_client = PublisherClient()

    def test_create_subscription_if_not_exists(self):
        expected_topic_name = self.__generate_test_name("topic")
        expected_subscription_name = self.__generate_test_name("subscription")

        self.__ems_publisher_client.topic_create_if_not_exists(GCP_PROJECT_ID, expected_topic_name)
        self.__ems_subscriber_client.create_subscription_if_not_exists(
            GCP_PROJECT_ID,
            expected_topic_name,
            expected_subscription_name
        )
        self.__ems_subscriber_client.create_subscription_if_not_exists(
            GCP_PROJECT_ID,
            expected_topic_name,
            expected_subscription_name
        )

        topic_path = self.__publisher_client.api.topic_path(GCP_PROJECT_ID, expected_topic_name)
        subscriptions = list(self.__publisher_client.api.list_topic_subscriptions(topic_path))

        expected_subscriptions = ["projects/" + GCP_PROJECT_ID + "/subscriptions/" + expected_subscription_name]
        self.assertEqual(expected_subscriptions, subscriptions)

        self.__delete_subscription(expected_subscription_name)
        self.__delete_topic(expected_topic_name)

    def __delete_topic(self, expected_topic_name):
        self.__publisher_client.api.delete_topic(
            self.__publisher_client.api.topic_path(GCP_PROJECT_ID, expected_topic_name)
        )

    @staticmethod
    def __delete_subscription(subscription_name: str):
        subscriber = SubscriberClient()
        subscription_path = subscriber.api.subscription_path(GCP_PROJECT_ID, subscription_name)
        subscriber.api.delete_subscription(subscription_path)

    @staticmethod
    def __generate_test_name(context: str):
        return "test_" + context + "_" + str(int(time.time()))
