import asyncio
import dataclasses
import os
import tempfile

OutputFiles = dict[str, bytes]


@dataclasses.dataclass
class FlentResult:
    output_files: OutputFiles
    stdout: str
    stderr: str
    status_code: int

    @property
    def success(self) -> bool:
        return self.status_code == 0


async def run_flent(*args, flent_path: str = "flent") -> FlentResult:
    """
    Runs flent with the given arguments in a temporary directory and collects the resulting files.

    :raises FlentError: If flent returns with status code != 0
    :param args: Arguments to pass to flent
    :param flent_path: Path to the flent executable if it's not in the PATH
    :return: FlentResult
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        proc = await asyncio.create_subprocess_exec(
            flent_path,
            *args,
            stdout=None,
            stderr=asyncio.subprocess.PIPE,
            cwd=tmpdir
        )

        stdout, stderr = await proc.communicate()

        output_files = {}
        for filename in os.listdir(tmpdir):
            print("Found output file:", filename)
            with open(os.path.join(tmpdir, filename), "rb") as file:
                output_files[filename] = file.read()

        return FlentResult(
            output_files,
            # stdout.decode(),
            "",
            stderr.decode(), proc.returncode
        )
