from concurrent.futures import Future

from google.cloud.pubsub_v1 import PublisherClient


class EmsPublisherClient:
    __client = PublisherClient()

    def publish(self, topic: str, data: bytes, **attrs) -> Future:
        return self.__client.publish(topic=topic, data=data, **attrs)