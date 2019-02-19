from google.cloud import storage


class EmsStorageClient:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.__client = storage.Client(project_id)

    def download_lines(self, bucket_name: str, blob_name: str, num_lines: int, chunk_size: int=1024*1024) -> list:
        bucket = self.__client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        buffer = blob.download_as_string(start=0, end=chunk_size)
        lines = buffer.decode("utf-8").split("\n")
        if len(lines) < num_lines:
            raise NotImplementedError("First chunk does not contain enough lines, increase chunk size")
        return lines[0:num_lines]
