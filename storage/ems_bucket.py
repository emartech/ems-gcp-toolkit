from storage.ems_blob import EmsBlob


class EmsBucket:
    def __init__(self, client, bucket_name):
        self.__client = client
        self.bucket_name = bucket_name

    def blob(self, blob_name: str) -> EmsBlob:
        # blob = self.__bucket.blob(blob_name)
        return EmsBlob(self.__client, blob_name)
