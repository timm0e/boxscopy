import asyncio
import pickle
import typing
from asyncio import StreamReader, StreamWriter

from fp_rpc.proto import PickleLength, Request, Response

QualName = str
FunctionHandle = typing.Tuple[typing.Callable, "Role"]

FUNCMAP: typing.Dict[QualName, typing.Tuple[typing.Callable, "Role"]] = {}
ROLES: set["Role"] = set()

DEBUG = False

_current_role: "Role | None" = None
_rpc_client: "RPCClient | None" = None
_controller_main: typing.Callable[[typing.Any], typing.Awaitable[None]] | None = None


def get_role_by_name(name: str) -> "Role | None":
    for role in ROLES:
        if role.name == name:
            return role
    return None


def init_client(client: "RPCClient"):
    global _current_role
    global _rpc_client
    _current_role = None
    _rpc_client = client
    pass


def init_server(role: "Role"):
    global _current_role
    global _rpc_client
    _current_role = role


def register_func(qualname: QualName, handle: FunctionHandle):
    assert (
        qualname not in FUNCMAP
    ), f"Function with qualified name {qualname} has already been registered by Role"
    FUNCMAP[qualname] = handle


def get_handle(qualname: QualName) -> FunctionHandle:
    assert qualname in FUNCMAP, "Function was not registered by any role"
    return FUNCMAP[qualname]


def get_current_role() -> "Role | None":
    return _current_role


def get_client() -> "RPCClient | None":
    return _rpc_client


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


class Role:
    def __init__(self, name: str):
        self.name = name
        ROLES.add(self)

    def __call__(self, func):
        funcname = func.__qualname__
        register_func(funcname, (func, self))

        def rpc_shim(*args, **kwargs):
            role = get_current_role()
            if role is None:
                return get_client().rpc_call(funcname, *args, **kwargs)

            assert (
                role == self
            ), f"Function {funcname} is only meant to be called on {self}, not {role}!"
            return func(*args, **kwargs)

        return rpc_shim

    def __str__(self):
        return f"<Role: {self.name}>"


async def run_server(reader: StreamReader, writer: StreamWriter):
    while True:
        # Lock is technically not needed because we currently only read from the socket at this point
        try:
            request_len_bytes = await reader.readexactly(4)
            request_len = PickleLength.from_bytes(request_len_bytes).pickle_len

            request_bytes = await reader.readexactly(request_len)
        except asyncio.exceptions.IncompleteReadError as e:
            assert not e.partial, "Expected to have been cancelled because of an EOF"
            break

        # TODO dispatch in task you dumbass

        request: Request = pickle.loads(request_bytes)
        func, _ = FUNCMAP[request.func]

        with open(f"rpc_{get_current_role()}.log", "a") as f:
            f.write(f"Request {request.call_id}\n")

        return_val = await func(*request.args, **request.kwargs)

        with open(f"rpc_{get_current_role()}.log", "a") as f:
            f.write(f"Response {request.call_id}\n")

        response = Response(request.call_id, return_val)
        response_bytes = pickle.dumps(response)

        writer.write(PickleLength(len(response_bytes)).to_bytes())
        writer.write(response_bytes)
        await writer.drain()

    writer.close()
    await writer.wait_closed()


def controller_main(func):
    global _controller_main
    _controller_main = func
    return func


def get_controller_main() -> typing.Callable[[typing.Any], typing.Awaitable[None]]:
    if _controller_main is None:
        raise RuntimeError("No @controller_main function registered")
    return _controller_main
