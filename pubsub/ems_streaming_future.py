from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture


class EmsStreamingFuture:

    def __init__(self, streaming_pull_future: StreamingPullFuture):
        self.__future = streaming_pull_future

    def result(self) -> str:
        return self.__future.result()

    def cancel(self):
        return self.__future.cancel()
