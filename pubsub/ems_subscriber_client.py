import logging
from typing import Callable, Iterator, List

from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message

from pubsub.ems_streaming_future import EmsStreamingFuture
from pubsub.ems_message import EmsMessage

LOGGER = logging.getLogger(__name__)


class EmsSubscriberClient:

    def __init__(self):
        self.__client = SubscriberClient()

    def subscribe(self, subscription: str, callback: Callable[[EmsMessage], None]) -> EmsStreamingFuture:
        def callback_wrapper(message: Message) -> None:
            callback(EmsMessage(message.ack_id, message.data, message.attributes))
            message.ack()

        future = self.__client.subscribe(
            subscription=subscription,
            callback=callback_wrapper
        )

        return EmsStreamingFuture(future)

    def pull(self,
             subscription: str,
             max_messages: int,
             return_immediately: bool = None) -> Iterator[EmsMessage]:
        messages = self.__client.api.pull(
            subscription=subscription,
            max_messages=max_messages,
            return_immediately=return_immediately
        ).received_messages

        # pylint: disable=map-builtin-not-iterating
        return map(lambda msg: EmsMessage(msg.ack_id, msg.message.data, msg.message.attributes), messages)

    def acknowledge(self,
                    subscription: str,
                    ack_ids: List[str]) -> None:
        self.__client.api.acknowledge(subscription=subscription, ack_ids=ack_ids)

    def create_subscription_if_not_exists(self, project_id: str, topic_name: str, subscription_name: str):
        topic_path = self.__client.api.topic_path(project_id, topic_name)
        subscription_path = self.__client.api.subscription_path(project_id, subscription_name)
        try:
            self.__client.api.create_subscription(subscription_path, topic_path)
            LOGGER.info("Subscription %s created for topic %s in project %s",
                        subscription_name, topic_name, project_id)
        except AlreadyExists:
            LOGGER.info("Subscription %s already exists for topic %s in project %s",
                        subscription_name, topic_name, project_id)

    def delete_subscription_if_exists(self, project_id: str, subscription_name: str):
        subscription_path = self.__client.api.subscription_path(project_id, subscription_name)
        try:
            self.__client.api.delete_subscription(subscription_path)
            LOGGER.info("Subscription %s deleted in project %s", subscription_name, project_id)
        except NotFound:
            LOGGER.info("Subscription %s not found in project %s", subscription_name, project_id)
