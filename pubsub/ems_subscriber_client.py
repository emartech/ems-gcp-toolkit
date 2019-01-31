from typing import Callable, Iterator, List

from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message

from pubsub.ems_streaming_future import EmsStreamingFuture
from pubsub.ems_message import EmsMessage


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
             return_immediately: bool=None) -> Iterator[EmsMessage]:
        messages = self.__client.pull(
            subscription=subscription,
            max_messages=max_messages,
            return_immediately=return_immediately
        ).received_messages

        return map(lambda msg: EmsMessage(msg.ack_id, msg.message.data, msg.message.attributes), messages)

    def acknowledge(self,
                    subscription: str,
                    ack_ids: List[str]) -> None:
        self.__client.acknowledge(subscription=subscription, ack_ids=ack_ids)
