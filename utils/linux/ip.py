import asyncio
import re
import subprocess
from ipaddress import IPv4Interface

from utils.subprocess.exceptions import SubprocessException

_IPV4_REGEX = re.compile(r"inet (?P<IP>\d{1,3}\.\d{1,3}.\d{1,3}.\d{1,3}/\d{1,2})")


async def addr(interface: str) -> list[IPv4Interface]:
    """
    Returns the IPv4 interfaces of the given interface.

    This is done by calling `ip address show dev <interface>` and parsing the result.
    :param interface: The interface to query
    :return: List of :class:`IPv4Interface` instances assigned to the interface
    """
    program = "ip"
    args = ["address", "show", "dev", interface]

    proc = await asyncio.create_subprocess_exec(
        program, *args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    stdout, stderr = await proc.communicate()

    if proc.returncode:
        raise SubprocessException(f"Error querying interface {interface}: {stderr.decode()}")

    return [
        IPv4Interface(addr_str) for addr_str in (_IPV4_REGEX.findall(stdout.decode()))
    ]
