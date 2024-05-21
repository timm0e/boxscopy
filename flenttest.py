import os.path
from contextlib import asynccontextmanager
from ipaddress import IPv4Interface
from os import makedirs

import click

from fp_rpc.roles import Role, StatefulRole
from fp_rpc.runner import controller_main, run
from utils import netperf
from utils.flent import run_flent, FlentResult
from utils.linux import ip
from utils.netperf import NetperfServer


class Outside(StatefulRole):
    def __init__(self, name):
        super().__init__(name)
        self.generator_instance = None

    async def context_wrapper(self):
        async with NetperfServer():
            print("Starting Netperf Server")
            yield
            print("Closing Netperf Server")

    async def start_server(self):
        if netperf.is_local_server_running():
            print("Netperf server already running")
            return
        self.generator_instance = self.context_wrapper()
        await anext(self.generator_instance)

    async def stop_server(self):
        if not self.generator_instance:
            print("Did not start Netperf server - skipping shutdown")
            return

        try:
            await anext(self.generator_instance)
        except StopAsyncIteration:
            # That's expected
            pass
        else:
            print("Something happened to the Netperf Server :(")

    async def get_ip(self):
        return await ip.addr(OUTSIDE_INTERFACE)


outside = Outside("outside")
inside = Role("inside")


@inside
async def run_flent_against_outside(outside_ip: str) -> FlentResult:
    return await run_flent("rrul", "-H", outside_ip, "-l", "15")


@asynccontextmanager
async def outside_netperf_server():
    try:
        await outside.start_server()
        yield await outside.get_ip()
    finally:
        await outside.stop_server()


@controller_main
async def main():
    async with outside_netperf_server() as outside_interfaces:
        outside_interface: IPv4Interface = outside_interfaces[0]
        outside_ip = str(outside_interface.ip)

        print("Running flent")
        result: FlentResult = await run_flent_against_outside(outside_ip)

    makedirs("flent_results", exist_ok=True)
    for filename, filebytes in result.output_files.items():
        print(f"Writing file {filename}")
        outpath = os.path.join("flent_results", filename)
        with open(outpath, "wb") as f:
            f.write(filebytes)


@click.group
@click.option("--outside_interface")
def config(outside_interface):
    global OUTSIDE_INTERFACE
    OUTSIDE_INTERFACE = outside_interface


if __name__ == "__main__":
    run(config)
