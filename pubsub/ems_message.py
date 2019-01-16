import json


class EmsMessage:

    def __init__(self, data: bytes):
        self.__data = json.loads(data, encoding="utf-8")

    @property
    def data(self):
        return self.__data
