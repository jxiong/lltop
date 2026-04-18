import re
import time
import asyncio
import subprocess
import sys


def c_red(text):
    return f"\033[1;31m{text}\033[0m"


def c_green(text):
    return f"\033[92m{text}\033[0m"


def c_yellow(text):
    return f"\033[93m{text}\033[0m"


def c_cyan(text):
    return f"\033[36m{text}\033[0m"


def c_bcyan(text):
    return f"\033[1;36m{text}\033[0m"


def c_white(text):
    return f"\033[37m{text}\033[0m"


def c_bwhite(text):
    return f"\033[1;37m{text}\033[0m"


def c_grey(text):
    return f"\033[90m{text}\033[0m"


def c_bgrey(text):
    return f"\033[1;30m{text}\033[0m"


def clear_screen():
    print("\033[H\033[J", end="")


SSH_CMD = "ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=5"


def expand_node(node_str):
    match = re.search(r"\[(.*?)\]", node_str)
    if not match:
        return [node_str]

    prefix = node_str[: match.start()]
    suffix = node_str[match.end() :]
    inner = match.group(1)

    expanded_nodes = []
    for part in inner.split(","):
        part = part.strip()
        if "-" in part:
            try:
                start_str, end_str = part.split("-", 1)
                pad_len = len(start_str) if start_str.startswith("0") else 0
                start, end = int(start_str), int(end_str)
                for i in range(start, end + 1):
                    mid = f"{i:0{pad_len}d}" if pad_len else str(i)
                    expanded_nodes.append(f"{prefix}{mid}{suffix}")
            except ValueError:
                expanded_nodes.append(f"{prefix}{part}{suffix}")
        else:
            expanded_nodes.append(f"{prefix}{part}{suffix}")

    result = []
    for node in expanded_nodes:
        result.extend(expand_node(node))
    return result


def expand_hostlist(hosts):
    result = []
    for host in hosts:
        result.extend(expand_node(host))

    seen = set()
    unique_result = []
    for r in result:
        if r not in seen:
            unique_result.append(r)
            seen.add(r)
    return unique_result


class SSHWorker:
    def __init__(self, server, ssh_cmd=SSH_CMD):
        self.server = server
        self.ssh_cmd = ssh_cmd
        self.process = None

    async def connect(self):
        if self.server == "localhost":
            cmd = "bash"
        else:
            cmd = f"{self.ssh_cmd} {self.server} bash"

        self.process = await asyncio.create_subprocess_shell(
            cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )

    async def run_cmd(self, cmd, timeout_sec):
        start_time = time.time()
        if self.process is None or self.process.returncode is not None:
            try:
                await asyncio.wait_for(self.connect(), timeout=10)
            except Exception as e:
                return "", time.time() - start_time, f"Connect error: {e}"

        magic_eof = f"__EOF_{time.time()}__"
        magic_bytes = magic_eof.encode()
        full_cmd = f"{cmd}; echo {magic_eof}$?\n"

        try:
            self.process.stdin.write(full_cmd.encode())
            await self.process.stdin.drain()

            buffer = bytearray()
            while True:
                chunk = await asyncio.wait_for(
                    self.process.stdout.read(65536), timeout=timeout_sec
                )
                if not chunk:
                    self.process = None
                    return "", time.time() - start_time, "Connection lost"

                buffer.extend(chunk)
                # O(1) search at the tail end of the buffer
                search_start = max(0, len(buffer) - len(chunk) - len(magic_bytes))
                if buffer.find(magic_bytes, search_start) != -1:
                    break

            # Find the magic string and extract the exit code
            idx = buffer.find(magic_bytes)
            # Find the newline after the magic string
            nl_idx = buffer.find(b"\n", idx)
            if nl_idx == -1:
                # Read one more line if newline isn't in buffer
                code_line = await asyncio.wait_for(
                    self.process.stdout.readline(), timeout=5.0
                )
                exit_code_str = code_line.decode("utf-8", errors="ignore").strip()
            else:
                exit_code_str = (
                    buffer[idx + len(magic_bytes) : nl_idx]
                    .decode("utf-8", errors="ignore")
                    .strip()
                )

            output = buffer[:idx].decode("utf-8", errors="ignore").strip()

            try:
                exit_code = int(exit_code_str)
                if exit_code != 0 and exit_code not in (124, 2) and not output:
                    return "", time.time() - start_time, f"Exited with {exit_code}"
            except ValueError:
                pass

            return output, time.time() - start_time, None

        except asyncio.TimeoutError:
            try:
                self.process.kill()
            except Exception:
                pass
            self.process = None
            return "", time.time() - start_time, "Timed out"
        except Exception as e:
            self.process = None
            return "", time.time() - start_time, str(e)

    def close(self):
        if self.process and self.process.returncode is None:
            try:
                self.process.stdin.write(b"exit\n")
                self.process.stdin.close()
            except:
                pass
            try:
                self.process.kill()
            except:
                pass
