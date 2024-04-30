import asyncio
import typing
from contextlib import asynccontextmanager
from enum import IntFlag

import click
import scapy.packet
import scapy.layers.inet
import scapy.sendrecv

from fp_rpc.roles import StatefulRole
from fp_rpc.runner import run, controller_main


class Config:
    OUTSIDE_SERVER_IP: str
    OUTSIDE_SERVER_PORT: int
    OUTSIDE_INTERFACE: str
    NAT_IP_FROM_OUTSIDE: str
    INSIDE_INTERFACE: str
    INSIDE_CLIENT_PORT: int
    INSIDE_CLIENT_IP: str


class ECNIPFlags(IntFlag):
    """
    Since scapy still only supports the TOS header, we need to manually spec the ECN Flags
    """

    NON_ECT = 0
    ECT_1 = 1
    ECT_0 = 2
    CE = ECT_0 | ECT_1

    @classmethod
    def from_tos(cls, tos: int) -> "ECNIPFlags":
        TOS_ECN_MASK = 0b00000011
        return cls(tos & TOS_ECN_MASK)


class FlagsDifference(typing.NamedTuple):
    expected: "ECNIPFlags"
    actual: "ECNIPFlags | None"


class ScapyRole(StatefulRole):
    def __init__(self, name: str):
        super().__init__(name)

        self.queue: asyncio.Queue[scapy.packet.Packet] | None = None
        self.sniffer: scapy.sendrecv.AsyncSniffer | None = None

    @property
    def _interface(self) -> str:
        return getattr(Config, f"{self.role.name.upper()}_INTERFACE", None)

    def _enqueue_packet_cb(self, loop: asyncio.AbstractEventLoop):
        def prn(pkt: scapy.packet.Packet):
            print("Packet received:")
            pkt.show()
            loop.call_soon_threadsafe(self.queue.put_nowait, pkt)

        return prn

    async def start_sniffing(self, packet_filter: str):
        if self.sniffer is not None:
            raise RuntimeError("Sniffer already running")

        # Get event loop here, since we are in an async context
        loop = asyncio.get_running_loop()

        self.queue = asyncio.Queue()

        self.sniffer = scapy.sendrecv.AsyncSniffer(
            filter=packet_filter,
            iface=self._interface,
            prn=self._enqueue_packet_cb(loop),
        )
        self.sniffer.start()

    async def stop_sniffing(self):
        if self.sniffer is None:
            raise RuntimeError("Sniffer not running")
        self.sniffer.stop()
        self.sniffer = None

    async def send_packet(self, pkt: scapy.packet.Packet):
        # for layer in [scapy.layers.inet.IP, scapy.layers.inet.UDP, scapy.layers.inet.TCP]:
        #     if layer in pkt:
        #         del pkt[layer].chksum

        # WARNING: This call is blocking! But unless it takes a lot of time, this should be fine for now
        # If this becomes a problem, we can move this to a different thread
        scapy.sendrecv.send(pkt, iface=self._interface)

    async def recv_packet(self) -> scapy.packet.Packet:
        assert self.queue.qsize() <= 1, "More than one packet queued up!"
        return await self.queue.get()


inside = ScapyRole("inside")
outside = ScapyRole("outside")


async def send_and_compare_ect(
    pkt: scapy.packet.Packet,
) -> FlagsDifference | None:
    expected = ECNIPFlags.from_tos(pkt[scapy.layers.inet.IP].tos)
    try:
        async with asyncio.timeout(2):
            async with asyncio.TaskGroup() as tg:
                tg.create_task(inside.send_packet(pkt))
                received = tg.create_task(outside.recv_packet())

            received = await received

            actual = ECNIPFlags.from_tos(received[scapy.layers.inet.IP].tos)

            if expected != actual:
                print("actual:")
                received.show()
                return FlagsDifference(expected, actual)
            return None
    except asyncio.TimeoutError:
        print("Timeout while waiting for packet")
        return FlagsDifference(expected, None)


@inside
async def build_udp_packet(flag):
    pkt = (
        scapy.layers.inet.IP(dst=Config.OUTSIDE_SERVER_IP)
        / scapy.layers.inet.UDP(
            dport=Config.OUTSIDE_SERVER_PORT, sport=Config.INSIDE_CLIENT_PORT
        )
        / "Hello!"
    )

    pkt[scapy.layers.inet.IP].tos = flag
    return pkt


async def validate_udp_ect():
    for flag in [ECNIPFlags.ECT_0, ECNIPFlags.ECT_1, ECNIPFlags.CE]:
        async with sniffing(outside, f"port {Config.OUTSIDE_SERVER_PORT}"):
            print(f"Testing UDP with flag {flag}")
            pkt = await build_udp_packet(flag)

            pkt[scapy.layers.inet.IP].tos = flag

            diff = await send_and_compare_ect(pkt)
            if diff is not None:
                print(f"UDP: Expected {diff.expected} but got {diff.actual}")
            else:
                print(f"UDP: {flag} passed")


@inside
async def build_syn() -> scapy.packet.Packet:
    return scapy.layers.inet.IP(dst=Config.OUTSIDE_SERVER_IP) / scapy.layers.inet.TCP(
        seq=0,
        ack=0,
        dport=Config.OUTSIDE_SERVER_PORT,
        sport=Config.INSIDE_CLIENT_PORT,
        flags="SEC",
    )


@outside
async def build_syn_ack(nat_port: int) -> scapy.packet.Packet:
    return scapy.layers.inet.IP(dst=Config.NAT_IP_FROM_OUTSIDE) / scapy.layers.inet.TCP(
        seq=0, ack=1, dport=nat_port, sport=Config.OUTSIDE_SERVER_PORT, flags="SAE"
    )


@inside
async def build_ack() -> scapy.packet.Packet:
    return scapy.layers.inet.IP(dst=Config.OUTSIDE_SERVER_IP) / scapy.layers.inet.TCP(
        seq=1,
        ack=1,
        dport=Config.OUTSIDE_SERVER_PORT,
        sport=Config.INSIDE_CLIENT_PORT,
        flags="A",
    )


@inside
async def build_data_packet(ect_flag: int) -> scapy.packet.Packet:
    return (
        scapy.layers.inet.IP(dst=Config.OUTSIDE_SERVER_IP, tos=ect_flag)
        / scapy.layers.inet.TCP(
            seq=1,
            ack=1,
            dport=Config.OUTSIDE_SERVER_PORT,
            sport=Config.INSIDE_CLIENT_PORT,
            flags="PA",
        )
        / "Hello World"
    )  # Size: 11 bytes


@outside
async def build_fin_ack_packet(nat_port: int) -> scapy.packet.Packet:
    return scapy.layers.inet.IP(dst=Config.NAT_IP_FROM_OUTSIDE) / scapy.layers.inet.TCP(
        seq=1, ack=12, dport=nat_port, sport=Config.OUTSIDE_SERVER_PORT, flags="FA"
    )


@inside
async def build_also_fin_packet() -> scapy.packet.Packet:
    return scapy.layers.inet.IP(dst=Config.OUTSIDE_SERVER_IP) / scapy.layers.inet.TCP(
        seq=12,
        ack=2,
        dport=Config.OUTSIDE_SERVER_PORT,
        sport=Config.INSIDE_CLIENT_PORT,
        flags="FA",
    )


async def threeway_ecn_handshake() -> int:
    # 1. SYN with EWE+CWR
    syn = await build_syn()
    async with asyncio.timeout(2):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(inside.send_packet(syn))
            syn_recv = tg.create_task(outside.recv_packet())
    syn_recv = await syn_recv
    assert (
        syn[scapy.layers.inet.TCP].flags == syn_recv[scapy.layers.inet.TCP].flags
    ), f"Flags did not match: Expected {syn[scapy.layers.inet.TCP].flags}, got {syn_recv[scapy.layers.inet.TCP].flags}"

    nat_port = syn_recv[scapy.layers.inet.TCP].sport

    # 2. SYN+ACK with EWE only
    syn_ack = await build_syn_ack(nat_port)
    async with asyncio.timeout(2):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(outside.send_packet(syn_ack))
            syn_ack_recv = tg.create_task(inside.recv_packet())
    syn_ack_recv = await syn_ack_recv
    syn_ack_recv.show()

    assert (
        syn_ack[scapy.layers.inet.TCP].flags
        == syn_ack_recv[scapy.layers.inet.TCP].flags
    ), f"Flags did not match: Expected {syn_ack[scapy.layers.inet.TCP].flags}, got {syn_ack_recv[scapy.layers.inet.TCP].flags}"

    # 3. ACK with ECE
    ack = await build_ack()
    await inside.send_packet(ack)
    received = await outside.recv_packet()
    assert (
        received[scapy.layers.inet.TCP].flags == ack[scapy.layers.inet.TCP].flags
    ), f"Flags did not match, expected {ack[scapy.layers.inet.TCP].flags}, got {received[scapy.layers.inet.TCP].flags}"
    return nat_port


async def validate_tcp_no_ce():
    for ect in [ECNIPFlags.ECT_0, ECNIPFlags.ECT_1]:
        async with sniffing(
            inside,
            f"dst {Config.INSIDE_CLIENT_IP} and port {Config.INSIDE_CLIENT_PORT}",
        ), sniffing(
            outside,
            f"dst {Config.OUTSIDE_SERVER_IP} and port {Config.OUTSIDE_SERVER_PORT}",
        ):
            print(f"Testing TCP with ECT({ect})")
            print("1. Shaking hands")
            nat_port = await threeway_ecn_handshake()

            print("2. Sending data")
            data = await build_data_packet(ect)
            diff = await send_and_compare_ect(data)
            if diff is not None:
                print(f"TCP: Expected ECT {diff.expected} but got {diff.actual}")
            else:
                print(f"TCP: ECT={ect} passed")

            print("3. Closing connection")
            fin_ack = await build_fin_ack_packet(nat_port)
            await outside.send_packet(fin_ack)
            also_fin = await build_also_fin_packet()
            await inside.send_packet(also_fin)


@asynccontextmanager
async def sniffing(
    role: ScapyRole, packet_filter: str
) -> typing.AsyncGenerator[None, None]:
    await role.start_sniffing(packet_filter)
    try:
        yield
    finally:
        await role.stop_sniffing()


@controller_main
async def controller_main():
    print("1. Testing UDP")
    await validate_udp_ect()

    print("2. Testing TCP (No CE)")
    await validate_tcp_no_ce()


@click.group
@click.option("--outside-ip", required=True)
@click.option("--outside-port", default=12345, type=int)
@click.option("--outside-interface", required=True)
@click.option("--nat-ip", required=True)
@click.option("--inside-interface", required=True)
@click.option("--inside-port", default=23488, type=int)
@click.option("--inside-ip", required=True)
def setup(
    outside_ip: str,
    outside_port: int,
    outside_interface: str,
    nat_ip: str,
    inside_interface: str,
    inside_port: int,
    inside_ip: str,
):
    Config.OUTSIDE_SERVER_IP = outside_ip
    Config.OUTSIDE_SERVER_PORT = outside_port
    Config.OUTSIDE_INTERFACE = outside_interface
    Config.NAT_IP_FROM_OUTSIDE = nat_ip
    Config.INSIDE_INTERFACE = inside_interface
    Config.INSIDE_CLIENT_PORT = inside_port
    Config.INSIDE_CLIENT_IP = inside_ip


if __name__ == "__main__":
    run(setup)
