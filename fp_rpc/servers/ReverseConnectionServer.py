import asyncio
import pickle
import typing

from fp_rpc import rpc
from fp_rpc.proto import PickleLength, Request, Response
from fp_rpc.rpc import Role, get_current_role, DEBUG


class NotConnected(Exception):
    def __str__(self):
        return "Did not connect to controller yet"


class Connection(typing.NamedTuple):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class ReverseConnectionServer:
    def __init__(self, host: str, port: str | int):
        self.host = host
        self.port = port
        self.connection: Connection | None = None
        self._dispatch_tasks: set[asyncio.Task] = set()

    async def connect(self):
        role = get_current_role()
        if role is None:
            raise RuntimeError("no role set")
        role_bytes = get_current_role().name.encode("utf-8")

        reader, writer = await asyncio.open_connection(self.host, self.port)
        self.connection = Connection(reader, writer)

        writer.write(PickleLength(len(role_bytes)).to_bytes())
        writer.write(role_bytes)
        await writer.drain()

    async def _dispatch(self, request_bytes: bytes):
        request: Request = pickle.loads(request_bytes)

        if DEBUG:
            print(f"Received {request.call_id}")

        func, _ = rpc.FUNCMAP[request.func]

        return_val = await func(*request.args, **request.kwargs)

        response = Response(request.call_id, return_val)
        response_bytes = pickle.dumps(response)

        reader, writer = self.connection

        if DEBUG:
            print(f"Responding to {request.call_id}")

        # If introducing an await between the following two lines, make sure to add a Lock
        writer.write(PickleLength(len(response_bytes)).to_bytes())
        writer.write(response_bytes)
        await writer.drain()

    async def serve(self):
        if self.connection is None:
            raise NotConnected()

        reader, writer = self.connection
        while True:
            try:
                # If you add any other readers to the connection, make sure to add a Lock here
                request_len_bytes = await reader.readexactly(PickleLength.Meta.bytes)
                request_len = PickleLength.from_bytes(request_len_bytes).pickle_len

                request_bytes = await reader.readexactly(request_len)
            except asyncio.exceptions.IncompleteReadError as e:
                assert (
                    not e.partial
                ), "Expected to have been cancelled because of an EOF"
                break

            dispatch_task = asyncio.create_task(self._dispatch(request_bytes))
            self._dispatch_tasks.add(dispatch_task)
            dispatch_task.add_done_callback(self._dispatch_tasks.discard)

        writer.close()
        await writer.wait_closed()
