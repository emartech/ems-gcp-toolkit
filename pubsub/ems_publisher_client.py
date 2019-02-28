from concurrent.futures import Future

from google.api_core.exceptions import NotFound
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient


class EmsPublisherClient:

    def __init__(self):
        self.__client = PublisherClient()

    def publish(self, topic: str, data: bytes, **attrs) -> Future:
        return self.__client.publish(topic=topic, data=data, **attrs)

    def topic_create_if_not_exists(self, project_id: str, topic_name: str):
        try:
            self.__client.get_topic(self.__client.api.topic_path(project_id, topic_name))
        except NotFound:
            self.__client.api.create_topic(self.__client.api.topic_path(project_id, topic_name))

    @staticmethod
    def subscription_create(project_id: str, topic_name: str, subscription_name: str):
        subscriber = SubscriberClient()

        topic_path = subscriber.api.topic_path(project_id, topic_name)
        subscription_path = subscriber.api.subscription_path(project_id, subscription_name)

        subscriber.api.create_subscription(subscription_path, topic_path)
