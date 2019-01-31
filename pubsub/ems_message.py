import json
from typing import Dict


class EmsMessage:

    def __init__(self, ack_id: str, data: bytes, attributes: Dict[str, str]):
        self.__data = None
        self.__raw_data = data
        self.__attributes = attributes
        self.__ack_id = ack_id

    @property
    def data_json(self):
        if self.__data is None:
            self.__data = json.loads(self.__raw_data, encoding="utf-8")

        return self.__data

    @property
    def data_raw(self):
        return self.__raw_data

    @property
    def attributes(self) -> Dict[str, str]:
        return self.__attributes

    @property
    def ack_id(self) -> str:
        return self.__ack_id
