import json
from typing import Dict


class EmsMessage:

    def __init__(self, ack_id: str, data: bytes, attributes: Dict[str, str]):
        self.__data = json.loads(data, encoding="utf-8")
        self.__attributes = attributes
        self.__ack_id = ack_id

    @property
    def data(self)  -> bytes:
        return self.__data

    @property
    def attributes(self) -> Dict[str, str]:
        return self.__attributes
