import asyncio
import logging
from typing import Callable, Any, Awaitable

import click

from fp_rpc.proto import PickleLength
import fp_rpc.rpc as rpc
import fp_rpc.roles as roles


@click.option("-h", "--host", default=None)
@click.option("-p", "--port", default=None)
def controller(host, port):
    asyncio.run(bootstrap_controller(host, port))

logging.basicConfig(level="INFO")

async def bootstrap_controller(host: str | None, port: str | None):
    logger = logging.getLogger("ControllerMain")

    if CONTROLLER_MAIN is None:
        raise RuntimeError("No main function for the controller defined.")

    ready = {funchandle[1]: asyncio.Event() for _, funchandle in rpc.FUNCMAP.items()}

    finalized_roles: rpc.RoleConnections = {}

    async def on_connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        namelen_bytes = await reader.readexactly(PickleLength.Meta.bytes)
        namelen = PickleLength.from_bytes(namelen_bytes).pickle_len

        name_bytes = await reader.readexactly(namelen)
        role_name = name_bytes.decode("utf-8")
        client_role = roles.get_role_by_name(role_name)
        if client_role is None:
            logger.error(
                f"Client at {writer.get_extra_info('peername')} tried to connect with unknown role '{role_name}'. Dropping connection"
            )
            writer.close()
            return

        finalized_roles[client_role] = rpc.Connection(reader, writer)
        logger.info(
            f"Role {client_role.name} registered from {writer.get_extra_info('peername')}"
        )
        ready[client_role].set()

    server = await asyncio.start_server(on_connect, host, port)

    for role in ready.keys():
        logger.info(f"Waiting for worker for role {role.name}")

    for event in ready.values():
        await event.wait()

    rpc_client = rpc.ControllerServer(finalized_roles)
    rpc.RPC_CLIENT = rpc_client
    roles.SELF_ROLE = None

    with rpc_client:
        logger.info("Starting controller main function")
        await CONTROLLER_MAIN()

    # FIXME: wait_for_close (used by the context manager) would be better,
    #        but since Python 3.12 this would wait until all clients are truly disconnected.
    #        Therefore, we just use close to boot all clients for now.
    server.close()
    logger.info("Program done! Controller server closed!")


@click.argument("role_str", metavar="role")
@click.argument("host")
@click.argument("port")
def worker(role_str: str, host, port):
    role = roles.get_role_by_name(role_str)
    if role is None:
        raise RuntimeError(f"Role {role} does not exist.")

    roles.SELF_ROLE = role
    asyncio.run(bootstrap_worker(host, port))


async def bootstrap_worker(host, port):
    logger = logging.getLogger("WorkerMain")
    logger.info(f"Connecting to {host}:{port} as {roles.SELF_ROLE}")

    server = rpc.WorkerClient(host, port)
    await server.connect()

    logger.info(f"Success!")

    logging.info(f"Introduction done, serving as {roles.SELF_ROLE}...")

    await server.serve()

    logging.info("Controller has closed the connection, shutting down...")


CONTROLLER_MAIN: Callable[[], Awaitable[Any]] | None = None


def controller_main(func):
    global CONTROLLER_MAIN
    if CONTROLLER_MAIN is not None:
        raise RuntimeError(
            f"Only one function can be marked as @controller_main. "
            f"(Currently registered: {CONTROLLER_MAIN.__qualname__}"
        )
    CONTROLLER_MAIN = func
    return func


_DEFAULT_GROUP = click.Group()


def run(setup_group: click.Group = _DEFAULT_GROUP):
    setup_group.command("controller")(controller)
    setup_group.command("worker")(worker)

    setup_group()
