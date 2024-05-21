import asyncio
import errno
import socket
from asyncio import CancelledError


def is_local_server_running() -> bool:
    """
    Return if a local netperf server is running on 0.0.0.0:12865.

    This might be the case if netperf has been installed as a package with an accompanying service (as e.g. on Ubuntu).
    This function tries to bind a TCP socket to 0.0.0.0:12865, to determine if a netperf server is already running.
    :return: True if a netperf server is running, False otherwise
    :raises OSError: If another error than "Address already in use" occurs
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("0.0.0.0", 12865))
            return False
    except OSError as e:
        if e.errno == errno.EADDRINUSE:
            return True
        raise e


class NetserverClosed(Exception):
    pass


class NetperfServer:
    def __init__(
        self,
        netserver_path: str | None = None,
        port: int | None = None,
        listen_addr: str | None = None,
    ):
        self.args = ["-D"]  # Do not daemonize
        self.path = netserver_path or "netserver"

        if port:
            self.args.extend(["-p", str(port)])

        if listen_addr:
            self.args.extend(["-L", listen_addr])

    async def _watchdog(self):
        _, stderr = await self.proc.communicate()
        stderr_str = stderr.decode()
        if stderr_str:
            stderr_str = ":\n" + stderr_str
        self.task.cancel()
        return NetserverClosed(
            f"Netserver closed unexpectedly (return {self.proc.returncode})"
            + stderr_str
        )

    async def __aenter__(self):
        self.task = asyncio.current_task()
        self.proc = await asyncio.create_subprocess_exec(
            self.path, *self.args, stdout=None, stderr=asyncio.subprocess.PIPE
        )
        self.watchdog_task = asyncio.create_task(self._watchdog())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.watchdog_task.cancel()
        if exc_type is asyncio.CancelledError:
            try:
                raise await self.watchdog_task from exc_val
            except CancelledError:
                # Watchdog got cancelled -> It did not cancel the task -> Do nothing
                pass

        self.proc.terminate()
