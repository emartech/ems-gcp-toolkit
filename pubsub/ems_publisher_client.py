from concurrent.futures import Future

from google.cloud.pubsub_v1 import PublisherClient


class EmsPublisherClient:
    __client = PublisherClient()

    def publish(self, topic: str, data: bytes, **attrs) -> Future:
        return self.__client.publish(topic=topic, data=data, **attrs)

    def topic_create(self, project_id: str, topic_name: str):
        self.__client.api.create_topic(self.__client.api.topic_path(project_id, topic_name))