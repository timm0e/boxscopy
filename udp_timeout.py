import asyncio
import dataclasses
import logging
from asyncio import DatagramTransport
from contextlib import asynccontextmanager

from fp_rpc.rpc import Role, controller_main
from timeout_measure.abstract_scheduler import (
    AbstractMultiagentSearchScheduler,
)
from timeout_measure.binary_search import BinarySearchScheduler
from timeout_measure.discovery import DiscoveryPhaseScheduler

inside = Role("inside")
outside = Role("outside")

SERVER_IP = "10.0.0.1"
SERVER_PORT = 4729


class MeasureServer(asyncio.DatagramProtocol):
    def __init__(self):
        self.transport: DatagramTransport | None = None
        self.connections: asyncio.Queue = asyncio.Queue()

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self.connections.put_nowait(addr)
        self.transport.sendto("Hello".encode(), addr)

    def reverse_ping(self, addr):
        self.transport.sendto("Ping".encode(), addr)


class MeasureClient(asyncio.DatagramProtocol):
    def __init__(self):
        self.transport: DatagramTransport | None = None
        self.ping_event = asyncio.Event()

    def connection_made(self, transport):
        self.transport = transport

    def say_hello(self):
        self.transport.sendto("Hello".encode())

    def datagram_received(self, data, addr):
        if data.decode() == "Ping":
            self.ping_event.set()


@dataclasses.dataclass
class MeasurementConnection:
    server_addr: str
    server_port: int

    client_port: int

    # To be determined after connection
    nat_addr: str | None
    nat_port: int | None


outside_server: MeasureServer | None = None


@outside
async def start_outside_server():
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: MeasureServer(),
        local_addr=("0.0.0.0", SERVER_PORT),
    )
    global outside_server
    outside_server = protocol


@outside
async def send_reverse(connection: MeasurementConnection):
    print(f"Sending to {connection.client_port}")
    outside_server.reverse_ping((connection.nat_addr, connection.nat_port))


@outside
async def get_latest_client() -> tuple[str, int]:
    async with asyncio.timeout(5):
        return await outside_server.connections.get()


client_connections: dict[int, MeasureClient] = {}


@inside
async def establish_client(connection: MeasurementConnection):
    global client_connections

    transport, protocol = await asyncio.get_running_loop().create_datagram_endpoint(
        lambda: MeasureClient(),
        remote_addr=(connection.server_addr, connection.server_port),
        local_addr=("0.0.0.0", connection.client_port),
    )

    client_connections[connection.client_port] = protocol
    protocol.say_hello()


@inside
async def check_for_ping(connection: MeasurementConnection) -> bool:
    # 5 second timeout
    client = client_connections[connection.client_port]
    try:
        async with asyncio.timeout(5):
            print(f"Checking for ping on port {connection.client_port}")
            await client.ping_event.wait()
            print(f"Received ping on {connection.client_port}")
            return True
    except asyncio.TimeoutError:
        print(f"Missed ping on {connection.client_port}")
        return False


@inside
async def close_client_connection(connection: MeasurementConnection):
    client_connections.pop(connection.client_port).transport.close()


establish_lock = asyncio.Lock()
client_port_counter = 21000


async def send_though_nat(connection: MeasurementConnection) -> bool:
    async with asyncio.TaskGroup() as tg:
        _ = tg.create_task(send_reverse(connection))
        ping = tg.create_task(check_for_ping(connection))
    return await ping


@asynccontextmanager
async def measurement_connection():
    async with establish_lock:
        global client_port_counter
        client_port = client_port_counter
        client_port_counter += 1

        connection = MeasurementConnection(
            SERVER_IP, SERVER_PORT, client_port, None, None
        )

        async with asyncio.TaskGroup() as tg:
            tg.create_task(establish_client(connection))
            client_task = tg.create_task(get_latest_client())
        host, port = await client_task
        connection.nat_addr = host
        connection.nat_port = port
    try:
        yield connection
    finally:
        await close_client_connection(connection)


async def measure_timeout(delay: int) -> bool:
    logging.info(f"Measuring delay {delay}")
    try:
        async with measurement_connection() as conn:
            logging.info(f"Connection established - Delaying {delay}")
            await asyncio.sleep(delay)

            logging.info(f"Sending through NAT {delay}")

            result = await send_though_nat(conn)

            logging.info(f"Result for delay {delay}: {result}")
            return result
    except asyncio.CancelledError as e:
        logging.info(f"Cancelling {delay}")
        raise e


async def do_measure(scheduler: AbstractMultiagentSearchScheduler):
    while not scheduler.has_finished():
        try:
            await scheduler.evaluate(measure_timeout)
        except asyncio.CancelledError:
            # Just grab the next task
            pass


@controller_main
async def controller_main():
    logging.basicConfig(level=logging.DEBUG)
    await start_outside_server()

    discovery_scheduler = DiscoveryPhaseScheduler(lambda i: int(1.15**i))

    async with asyncio.TaskGroup() as tg:
        for _ in range(100):
            tg.create_task(do_measure(discovery_scheduler))

        lower_bound, upper_bound = await discovery_scheduler.wait_for_result()

        print("Lower bound", lower_bound)
        print("Upper bound", upper_bound)

        scheduler = BinarySearchScheduler(lower_bound, upper_bound)

        for _ in range(100):
            tg.create_task(do_measure(scheduler))

        print("Result: ", await scheduler.wait_for_result())
