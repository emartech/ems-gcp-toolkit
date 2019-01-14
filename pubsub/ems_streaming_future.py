from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture


class EmsStreamingFuture:

    def __init__(self, streaming_pull_future: StreamingPullFuture):
        self.future = streaming_pull_future

    def result(self):
        return self.future.result()

    def cancel(self):
        return self.future.cancel()