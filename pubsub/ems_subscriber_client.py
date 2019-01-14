from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message

from pubsub.ems_streaming_future import EmsStreamingFuture
from pubsub.ems_message import EmsMessage


class EmsSubscriberClient:
    client = SubscriberClient()

    def subscribe(self, subscription: str, callback):
        def callback_wrapper(message: Message):
            callback(EmsMessage(message.data))
            message.ack()

        future = self.client.subscribe(
            subscription=subscription,
            callback=callback_wrapper
        )

        return EmsStreamingFuture(future)
