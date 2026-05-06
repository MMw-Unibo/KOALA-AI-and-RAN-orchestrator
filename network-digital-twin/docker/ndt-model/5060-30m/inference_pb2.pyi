import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class InferenceRequest(_message.Message):
    __slots__ = ("datetime",)
    DATETIME_FIELD_NUMBER: _ClassVar[int]
    datetime: _timestamp_pb2.Timestamp
    def __init__(self, datetime: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class InferenceResult(_message.Message):
    __slots__ = ("predictions",)
    PREDICTIONS_FIELD_NUMBER: _ClassVar[int]
    predictions: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, predictions: _Optional[_Iterable[float]] = ...) -> None: ...
