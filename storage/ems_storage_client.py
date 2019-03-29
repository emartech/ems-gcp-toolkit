from google.cloud import storage
from google.cloud.storage.notification import JSON_API_V1_PAYLOAD_FORMAT


class EmsStorageClient:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.__client = storage.Client(project_id)

    def download_lines(self, bucket_name: str, blob_name: str, num_lines: int, chunk_size: int = 1024 * 1024) -> list:
        bucket = self.__client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        buffer = blob.download_as_string(start=0, end=chunk_size)
        lines = buffer.decode("utf-8", errors="ignore").split("\n")
        if len(lines) < num_lines:
            raise NotImplementedError("First chunk does not contain enough lines, increase chunk size")
        return lines[0:num_lines]

    def upload_from_string(self, bucket_name: str, blob_name: str, content: str):
        bucket = self.__client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content)

    def download_content_as_string(self, bucket_name: str, blob_name: str):
        bucket = self.__client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_string().decode("utf-8")

    def create_bucket_if_not_exists(self, bucket_name: str, project=None, location=None):
        bucket = self.__client.bucket(bucket_name)
        if not bucket.exists():
            bucket.create(project=project, location=location)

    def delete_bucket_if_exists(self, bucket_name: str, force: bool = False):
        bucket = self.__client.bucket(bucket_name)
        if bucket.exists():
            bucket.delete(force=force)

    def create_notification_if_not_exists(self, topic_name: str, bucket_name: str):
        bucket = self.__client.bucket(bucket_name)

        if not self.is_notification_exists(bucket, topic_name):
            bucket.notification(topic_name, payload_format=JSON_API_V1_PAYLOAD_FORMAT).create()

    def delete_notification_if_exists(self, topic_name: str, bucket_name: str):
        bucket = self.__client.bucket(bucket_name)

        for notification in bucket.list_notifications():
            if notification.topic_name == topic_name:
                notification.delete()

    @staticmethod
    def is_notification_exists(bucket, topic_name):
        for notification in bucket.list_notifications():
            if notification.topic_name == topic_name:
                return True
        return False

    def delete_blob(self, bucket_name: str, blob_name: str):
        bucket = self.__client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
