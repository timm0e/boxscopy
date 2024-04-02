import dataclasses
import struct
import typing


@dataclasses.dataclass
class PickleLength:
    pickle_len: int

    class Meta:
        bytes = 4
        struct = struct.Struct("!I")

    @classmethod
    def from_bytes(cls, b: bytes):
        if not len(b) == cls.Meta.bytes:
            raise ValueError(f"Expected {cls.Meta.bytes} bytes, got {len(b)}")

        pickle_len = cls.Meta.struct.unpack(b)[0]
        return cls(pickle_len)

    def to_bytes(self) -> bytes:
        return self.Meta.struct.pack(self.pickle_len)


@dataclasses.dataclass
class Request:
    call_id: int
    func: str
    args: tuple[typing.Any, ...]
    kwargs: dict[typing.Any, typing.Any]


@dataclasses.dataclass
class Response:
    call_id: int
    value: typing.Any
