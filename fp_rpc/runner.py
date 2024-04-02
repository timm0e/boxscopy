import asyncio
import importlib
import logging

import click

from fp_rpc.clients.ReverseConnectionClient import (
    RoleConnections,
    Connection,
    ControllerRPCClient,
)
from fp_rpc.rpc import get_role_by_name, init_client, get_controller_main
from fp_rpc.rpc import init_server, run_server
from fp_rpc.proto import PickleLength
from fp_rpc.rpc import FUNCMAP, get_current_role
from fp_rpc.servers.ReverseConnectionServer import ReverseConnectionServer


@click.group(invoke_without_command=True)
@click.option(
    "--loglevel",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    default="INFO",
)
@click.argument("module")
@click.pass_context
def cli(ctx, loglevel, module):
    logging.basicConfig(level=getattr(logging, loglevel))
    importlib.import_module(module)
    importlib.invalidate_caches()
    if ctx.invoked_subcommand is None:
        controller()


@cli.command("controller")
@click.option("-h", "--host", default=None)
@click.option("-p", "--port", default=None)
def controller(host, port):
    asyncio.run(controller_wrapper(host, port))


async def controller_wrapper(host: str | None, port: str | None):
    logger = logging.getLogger("ControllerMain")

    ready = {funchandle[1]: asyncio.Event() for _, funchandle in FUNCMAP.items()}

    finalized_roles: RoleConnections = {}

    async def on_connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        namelen_bytes = await reader.readexactly(PickleLength.Meta.bytes)
        namelen = PickleLength.from_bytes(namelen_bytes).pickle_len

        name_bytes = await reader.readexactly(namelen)
        role_name = name_bytes.decode("utf-8")
        client_role = get_role_by_name(role_name)
        if client_role is None:
            logger.error(
                f"Client at {writer.get_extra_info('peername')} tried to connect with unknown role '{role_name}'. Dropping connection"
            )
            writer.close()
            return

        finalized_roles[client_role] = Connection(reader, writer)
        logger.info(
            f"Role {client_role.name} registered from {writer.get_extra_info('peername')}"
        )
        ready[client_role].set()

    server = await asyncio.start_server(on_connect, host, port)

    for role in ready.keys():
        logger.info(f"Waiting for worker for role {role.name}")

    for event in ready.values():
        await event.wait()

    rpc_client = ControllerRPCClient(finalized_roles)
    init_client(rpc_client)

    with rpc_client:
        async with server:
            await get_controller_main()()

    logger.info("Program done! Controller server closed!")


@cli.command("worker")
@click.argument("role_str", metavar="role")
@click.argument("host")
@click.argument("port")
def worker(role_str: str, host, port):
    role = get_role_by_name(role_str)
    if role is None:
        raise RuntimeError(f"Role {role} does not exist")

    init_server(role)
    asyncio.run(worker_main(host, port))

async def worker_main(host, port):
    logger = logging.getLogger("WorkerMain")
    logger.info(f"Connecting to {host}:{port} as {get_current_role()}")

    server = ReverseConnectionServer(host, port)
    await server.connect()

    logger.info(f"Success!")

    logging.info(f"Introduction done, serving as {get_current_role()}...")

    await server.serve()

    logging.info("Controller has closed the connection, shutting down...")


if __name__ == "__main__":
    cli()
