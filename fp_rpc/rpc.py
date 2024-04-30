import asyncio
import pickle
import typing

from fp_rpc.proto import PickleLength, Request, Response
import fp_rpc.roles as roles

FuncName = str
FunctionHandle = typing.Tuple[typing.Callable, "roles.Role"]

FUNCMAP: typing.Dict[FuncName, typing.Tuple[typing.Callable, "roles.Role"]] = {}

RPC_CLIENT: "RPCClient | None" = None

DEBUG = False


def register_func(funcname: FuncName, handle: FunctionHandle):
    assert (
            funcname not in FUNCMAP
    ), f"Function with name {funcname} has already been registered"
    FUNCMAP[funcname] = handle


def get_handle(qualname: FuncName) -> FunctionHandle:
    assert qualname in FUNCMAP, "Function was not registered by any role"
    return FUNCMAP[qualname]


class RPCClient(typing.Protocol):
    async def rpc_call(
        self, funcname: str, *args, **kwargs
    ) -> typing.Awaitable[typing.Any]:
        raise NotImplementedError()


class StubRPCClient(RPCClient):
    async def rpc_call(
        self, funcname: str, *args, **kwargs
    ) -> typing.Awaitable[typing.Any]:
        raise NotImplementedError("This worker is not meant to initiate RPC calls.")

class Connection(typing.NamedTuple):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class WorkerClient:
    """
    Connects to a ControllerServer, and enables the worker to issue RPC calls.
    """
    def __init__(self, host: str, port: str | int):
        self.host = host
        self.port = port
        self.connection: Connection | None = None
        self._dispatch_tasks: set[asyncio.Task] = set()

    async def connect(self):
        role = roles.get_current_role()
        if role is None:
            raise RuntimeError("no role set")
        role_bytes = roles.get_current_role().name.encode("utf-8")

        reader, writer = await asyncio.open_connection(self.host, self.port)
        self.connection = Connection(reader, writer)

        writer.write(PickleLength(len(role_bytes)).to_bytes())
        writer.write(role_bytes)
        await writer.drain()

    async def _dispatch(self, request_bytes: bytes):
        request: Request = pickle.loads(request_bytes)

        if DEBUG:
            print(f"Received {request.call_id}")

        func, _ = FUNCMAP[request.func]

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
            raise WorkerClient.NotConnected()

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

    class NotConnected(Exception):
        def __str__(self):
            return "Did not connect to controller yet"


RoleConnections = dict["roles.Role", Connection]


class ControllerServer(RPCClient):
    """
    Server that listens for connections from workers and dispatches RPC Calls.
    """

    def __init__(self, role_connections: RoleConnections):
        self.current_calls: dict[int, asyncio.Future[typing.Any]] = {}
        self.role_connections = role_connections
        self.id_counter = 0
        self.receiver_tasks: list[asyncio.Task] = []

    async def process_responses(self, role: "roles.Role"):
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
