import time
from unittest import TestCase

from google.api_core.exceptions import NotFound, AlreadyExists
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient

from pubsub.ems_publisher_client import EmsPublisherClient
from pubsub.ems_subscriber_client import EmsSubscriberClient
from tests.integration import GCP_PROJECT_ID


class ItEmsSubscriberClient(TestCase):

    def setUp(self):
        self.__ems_publisher_client = EmsPublisherClient()
        self.__ems_subscriber_client = EmsSubscriberClient()
        self.__publisher_client = PublisherClient()
        self.__subscriber_client = SubscriberClient()

    def test_create_subscription_if_not_exists(self):
        topic_name = self.__generate_test_name("topic")
        subscription_name = self.__generate_test_name("subscription")

        self.__ems_publisher_client.topic_create_if_not_exists(GCP_PROJECT_ID, topic_name)
        self.__ems_subscriber_client.create_subscription_if_not_exists(GCP_PROJECT_ID, topic_name, subscription_name)
        try:
            self.__ems_subscriber_client.create_subscription_if_not_exists(GCP_PROJECT_ID, topic_name, subscription_name)
        except AlreadyExists:
            self.fail("create_subscription_if_not_exists raised AlreadyExists error")

        topic_path = self.__publisher_client.api.topic_path(GCP_PROJECT_ID, topic_name)
        subscriptions = list(self.__publisher_client.api.list_topic_subscriptions(topic_path))

        expected_subscriptions = ["projects/" + GCP_PROJECT_ID + "/subscriptions/" + subscription_name]
        self.assertEqual(expected_subscriptions, subscriptions)

        self.__delete_subscription(subscription_name)
        self.__delete_topic(topic_name)

    def test_delete_subscription_if_exists(self):
        topic_name = self.__generate_test_name("topic")
        subscription_name = self.__generate_test_name("subscription")

        self.__ems_publisher_client.topic_create_if_not_exists(GCP_PROJECT_ID, topic_name)
        self.__ems_subscriber_client.create_subscription_if_not_exists(GCP_PROJECT_ID, topic_name, subscription_name)
        self.__ems_subscriber_client.delete_subscription_if_exists(GCP_PROJECT_ID, subscription_name)
        try:
            self.__ems_subscriber_client.delete_subscription_if_exists(GCP_PROJECT_ID, subscription_name)
        except NotFound:
            self.fail("delete_subscription_if_exists raised NotFound error")

        subscription_path = self.__subscriber_client.api.subscription_path(GCP_PROJECT_ID, subscription_name)
        with self.assertRaises(NotFound):
            self.__subscriber_client.api.get_subscription(subscription_path)

        self.__delete_topic(topic_name)

    def __delete_topic(self, topic_name):
        topic_path = self.__publisher_client.api.topic_path(GCP_PROJECT_ID, topic_name)
        self.__publisher_client.api.delete_topic(topic_path)

    def __delete_subscription(self, subscription_name: str):
        subscription_path = self.__subscriber_client.api.subscription_path(GCP_PROJECT_ID, subscription_name)
        self.__subscriber_client.api.delete_subscription(subscription_path)

    @staticmethod
    def __generate_test_name(context: str):
        return "test_" + context + "_" + str(int(time.time()))
