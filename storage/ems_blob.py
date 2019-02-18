class EmsBlob:
    def __init__(self, client, blob_name):
        self.__client = client
        self.blob_name = blob_name

    def download_as_string(self, start: int=None, end: int=None):
        # return self.__blob.download_as_string(client=self.__client, start=0, end=1000000).decode("utf-8")
        pass

