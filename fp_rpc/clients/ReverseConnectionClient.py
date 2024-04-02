import asyncio
import dataclasses
import typing
import pickle

from fp_rpc.proto import PickleLength, Response, Request
from fp_rpc.rpc import Role, FUNCMAP, DEBUG


@dataclasses.dataclass
class Connection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


RoleConnections = dict[Role, Connection]


class ControllerRPCClient:
    def __init__(self, role_connections: RoleConnections):
        self.current_calls: dict[int, asyncio.Future[typing.Any]] = {}
        self.role_connections = role_connections
        self.id_counter = 0
        self.receiver_tasks: list[asyncio.Task] = []

    async def process_responses(self, role: Role):
        # We are the only ones reading this socket, so no locks are necessary

        reader = self.role_connections[role].reader

        while True:
            response_len_bytes = await reader.readexactly(PickleLength.Meta.bytes)
            response_len = PickleLength.from_bytes(response_len_bytes).pickle_len

            response_bytes = await reader.readexactly(response_len)

            response: Response = pickle.loads(response_bytes)

            future = self.current_calls.pop(response.call_id)
            try:
                future.set_result(response.value)
            except asyncio.InvalidStateError:
                # TODO implement task cancellation over RPC
                print(f"Discarding response for cancelled call {response.call_id}")

    def get_next_id(self) -> int:
        cur_id = self.id_counter
        self.id_counter += 1
        return cur_id

    async def rpc_call(
        self, funcname: str, *args, **kwargs
    ) -> typing.Awaitable[typing.Any]:
        # TODO: Wrap encoding in async generator
        call_id = self.get_next_id()
        request = Request(call_id, funcname, args, kwargs)
        request_bytes = pickle.dumps(request)

        result_future = asyncio.get_event_loop().create_future()
        self.current_calls[call_id] = result_future

        _, role = FUNCMAP[funcname]

        if DEBUG:
            print(f"Sent {request.call_id}")

        writer = self.role_connections[role].writer
        writer.write(PickleLength(len(request_bytes)).to_bytes())
        writer.write(request_bytes)
        await writer.drain()

        result = await result_future


        if DEBUG:
            print(f"Received {request.call_id}")

        return result

    def cancel_receivers(self):
        for task in self.receiver_tasks:
            task.cancel("cancelling receivers")

    def __enter__(self):
        self.receiver_tasks = [
            asyncio.create_task(self.process_responses(role))
            for role in self.role_connections
        ]

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cancel_receivers()
