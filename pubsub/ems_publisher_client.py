import logging
from concurrent.futures import Future

from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient

LOGGER = logging.getLogger(__name__)


class EmsPublisherClient:

    def __init__(self):
        self.__client = PublisherClient()

    def publish(self, topic: str, data: bytes, **attrs) -> Future:
        return self.__client.publish(topic=topic, data=data, **attrs)

    def topic_create_if_not_exists(self, project_id: str, topic_name: str):
        topic_path = self.__client.api.topic_path(project_id, topic_name)
        try:
            self.__client.api.create_topic(topic_path)
            LOGGER.info("Topic %s created in project %s", topic_name, project_id)
        except AlreadyExists:
            LOGGER.info("Topic %s already exists in project %s", topic_name, project_id)

    def delete_topic_if_exists(self, project_id: str, topic_name: str):
        topic_path = self.__client.api.topic_path(project_id, topic_name)
        try:
            self.__client.api.delete_topic(topic_path)
            LOGGER.info("Topic %s deleted in project %s", topic_name, project_id)
        except NotFound:
            LOGGER.info("Topic %s not found in project %s", topic_name, project_id)

    @staticmethod
    def subscription_create(project_id: str, topic_name: str, subscription_name: str):
        subscriber = SubscriberClient()

        topic_path = subscriber.api.topic_path(project_id, topic_name)
        subscription_path = subscriber.api.subscription_path(project_id, subscription_name)

        subscriber.api.create_subscription(subscription_path, topic_path)
