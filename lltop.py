#!/usr/bin/env python3

import argparse
import asyncio
import subprocess
import sys
import os
import re
import time
import select
import termios
import tty
import signal
import multiprocessing as mp
from collections import defaultdict


def default_to_regular(d):
    if isinstance(d, defaultdict):
        d = {k: default_to_regular(v) for k, v in d.items()}
    return d


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


def get_servers_from_mountpoint(mountpoint):
    try:
        cmd = f"lfs getname {mountpoint} 2>/dev/null"
        output = (
            subprocess.check_output(cmd, shell=True)
            .decode("utf-8", errors="ignore")
            .strip()
        )
        if not output:
            return []

        fsname = output.split("-")[0].split(" ")[0].strip()
        servers = set()

        for comp in ["osc", "mdc"]:
            cmd = f"lctl get_param -n {comp}.{fsname}-*.conn_uuid 2>/dev/null"
            try:
                uuids = (
                    subprocess.check_output(cmd, shell=True)
                    .decode("utf-8", errors="ignore")
                    .splitlines()
                )
                for uuid in uuids:
                    uuid = uuid.strip()
                    if not uuid:
                        continue
                    if "@" in uuid:
                        ip = uuid.split("@")[0]
                        servers.add(ip)
            except subprocess.CalledProcessError:
                pass

        server_list = list(servers)
        if not server_list:
            return []

        async def do_dedup():
            async def get_hostname(server):
                cmd = f"{SSH_CMD} {server} 'uname -n'"
                try:
                    process = await asyncio.create_subprocess_shell(
                        cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    stdout, stderr = await asyncio.wait_for(
                        process.communicate(), timeout=10
                    )
                    if process.returncode == 0:
                        hn = stdout.decode("utf-8", errors="ignore").strip()
                        if hn:
                            return server, hn
                except Exception:
                    pass
                return server, server

            tasks = [get_hostname(srv) for srv in server_list]
            results = await asyncio.gather(*tasks)

            hostname_to_ip = {}
            for ip, hostname in results:
                if hostname not in hostname_to_ip:
                    hostname_to_ip[hostname] = ip

            return sorted(list(hostname_to_ip.values()))

        loop = asyncio.get_event_loop()
        deduped = loop.run_until_complete(do_dedup())
        return deduped
    except Exception as e:
        print(f"Error discovering servers: {e}", file=sys.stderr)
        return []


class SSHWorker:
    def __init__(self, server, ssh_cmd):
        self.server = server
        self.ssh_cmd = ssh_cmd
        self.process = None

    async def connect(self):
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


async def fetch_metrics(worker, timeout_sec, params_str):
    cmd = f"sudo lctl get_param {params_str} 2>/dev/null"
    stdout, elapsed, error = await worker.run_cmd(cmd, timeout_sec + 2)
    return worker.server, stdout, elapsed, error


def process_metrics(server, metrics_text, history, current_stats, is_first):
    current_target = "unknown"
    current_client = None

    for line in metrics_text.splitlines():
        line = line.strip()
        if not line:
            continue

        if line.endswith(".stats=") or line.endswith(".ldlm_stats="):
            if ".exports." in line:
                try:
                    target_part, client_part = line.split(".exports.", 1)
                    current_target = target_part.split(".", 1)[1]
                    if client_part.endswith(".stats="):
                        current_client = client_part.rsplit(".stats=", 1)[0].split("@")[
                            0
                        ]
                    else:
                        current_client = client_part.rsplit(".ldlm_stats=", 1)[0].split(
                            "@"
                        )[0]
                except Exception:
                    current_client = None
            continue

        if not current_client:
            continue

        if "samples" not in line:
            continue

        parts = line.split()
        if len(parts) < 3 or parts[2] != "samples":
            continue

        op = parts[0]
        try:
            samples = int(parts[1])
        except ValueError:
            continue

        if op in ("read_bytes", "write_bytes"):
            if len(parts) >= 7:
                try:
                    val = int(parts[6])
                except ValueError:
                    continue
            else:
                continue
            metric_type = "bytes"
        else:
            val = samples
            metric_type = "stats"

        key = (server, current_target, current_client, metric_type, op)
        prev_val = history.get(key, 0)

        diff = val - prev_val
        if diff < 0:
            diff = 0
        history[key] = val

        if is_first:
            continue

        if diff > 0:
            if metric_type == "bytes":
                if op == "write_bytes":
                    current_stats[current_client]["wr_bytes"] += diff
                elif op == "read_bytes":
                    current_stats[current_client]["rd_bytes"] += diff
            elif metric_type == "stats":
                if op in (
                    "ldlm_enqueue",
                    "ldlm_cancel",
                    "ldlm_bl_callback",
                    "ldlm_cp_callback",
                    "ldlm_gl_callback",
                ):
                    current_stats[current_client]["locks"][op] += diff
                else:
                    current_stats[current_client]["ops"][op] += diff


async def worker_loop(servers, params_str, pipe, ssh_cmd):
    history = {}
    is_first = True
    workers = {srv: SSHWorker(srv, ssh_cmd) for srv in servers}

    async def connect_worker(w):
        await w.connect()

    await asyncio.gather(
        *[connect_worker(w) for w in workers.values()], return_exceptions=True
    )

    loop = asyncio.get_event_loop()
    pipe.send(("ready",))

    async def get_pipe_msg():
        fut = loop.create_future()
        fd = pipe.fileno()

        def reader():
            if pipe.poll():
                if not fut.done():
                    try:
                        fut.set_result(pipe.recv())
                    except EOFError:
                        fut.set_exception(EOFError())
                    except Exception as e:
                        fut.set_exception(e)

        loop.add_reader(fd, reader)
        try:
            return await fut
        finally:
            loop.remove_reader(fd)

    while True:
        try:
            cmd = await get_pipe_msg()
        except EOFError:
            break

        if cmd[0] == "quit":
            break
        elif cmd[0] == "fetch":
            timeout_sec = cmd[1]
            current_stats = defaultdict(
                lambda: {
                    "wr_bytes": 0,
                    "rd_bytes": 0,
                    "ops": defaultdict(int),
                    "locks": defaultdict(int),
                }
            )

            tasks = [
                fetch_metrics(workers[srv], timeout_sec, params_str) for srv in servers
            ]
            start_time = time.time()
            results = await asyncio.gather(*tasks)
            max_fetch_time = time.time() - start_time

            errors = []
            for server, metrics_text, elapsed, error in results:
                if error:
                    errors.append((server, error))
                else:
                    process_metrics(
                        server, metrics_text, history, current_stats, is_first
                    )

            is_first = False
            pipe.send(
                ("result", default_to_regular(current_stats), errors, max_fetch_time)
            )

    for w in workers.values():
        w.close()


def worker_process(servers, params_str, pipe, ssh_cmd):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(worker_loop(servers, params_str, pipe, ssh_cmd))
    except Exception:
        pass
    finally:
        try:
            if hasattr(signal, "set_wakeup_fd"):
                try:
                    signal.set_wakeup_fd(-1)
                except ValueError:
                    pass
            loop.close()
        except:
            pass


OP_MAP = {
    "read": "r",
    "write": "w",
    "close": "cl",
    "open": "op",
    "create": "cr",
    "statfs": "st",
    "get_info": "gi",
    "prealloc": "pa",
    "punch": "pu",
    "sync": "sy",
    "getattr": "ga",
    "setattr": "sa",
    "unlink": "un",
    "mknod": "mk",
    "destroy": "dy",
    "ldlm_enqueue": "enq",
    "ldlm_cancel": "cxl",
    "ldlm_bl_callback": "blc",
    "ldlm_cp_callback": "cpc",
    "ldlm_gl_callback": "glc",
}


async def main_loop(
    workers, interval, threshold, limit, print_header, sort_by, total_servers
):
    current_sort = sort_by
    is_first = True
    loop = asyncio.get_event_loop()

    async def get_msg(w):
        fut = loop.create_future()
        p = w["pipe"]
        fd = p.fileno()

        def reader():
            if p.poll():
                if not fut.done():
                    try:
                        fut.set_result(p.recv())
                    except EOFError:
                        fut.set_exception(EOFError())
                    except Exception as e:
                        fut.set_exception(e)

        loop.add_reader(fd, reader)
        try:
            return await fut
        finally:
            loop.remove_reader(fd)

    print(
        f"Initializing persistent SSH connections to {total_servers} servers across {len(workers)} worker processes..."
    )
    await asyncio.gather(*[get_msg(w) for w in workers])
    clear_screen()

    while True:
        curl_timeout = max(5, interval - 2)
        for w in workers:
            w["pipe"].send(("fetch", curl_timeout))

        results = await asyncio.gather(*[get_msg(w) for w in workers])

        all_stats = defaultdict(
            lambda: {
                "wr_bytes": 0,
                "rd_bytes": 0,
                "ops": defaultdict(int),
                "locks": defaultdict(int),
            }
        )
        all_errors = []
        global_max_fetch_time = 0.0

        for msg, stats, errors, max_fetch_time in results:
            global_max_fetch_time = max(global_max_fetch_time, max_fetch_time)
            all_errors.extend(errors)
            for client, c_stats in stats.items():
                all_stats[client]["wr_bytes"] += c_stats["wr_bytes"]
                all_stats[client]["rd_bytes"] += c_stats["rd_bytes"]
                for op, count in c_stats["ops"].items():
                    all_stats[client]["ops"][op] += count
                for lock, count in c_stats["locks"].items():
                    all_stats[client]["locks"][lock] += count

        if not is_first:
            clear_screen()

            if print_header:
                warning = ""
                if global_max_fetch_time > interval:
                    warning = " " + c_red("[WARNING: Fetch time exceeds interval!]")

                print(
                    f"{c_bgrey(f'Servers: {total_servers} | Max Fetch Time: {global_max_fetch_time:.2f}s | Interval: {interval}s | Sort: {current_sort}')}{warning}"
                )

                if all_errors:
                    err_msg = ", ".join(f"{srv}: {err}" for srv, err in all_errors[:3])
                    if len(all_errors) > 3:
                        err_msg += f" ... (+{len(all_errors)-3} more)"
                    print(c_red(f"Errors ({len(all_errors)}): {err_msg}"))

                print(
                    c_bcyan(
                        f"{'CLIENT_IP':<18} {'WRITE(MBps)':<12} {'READ(MBps)':<12} {'LOCK(ops)':<22} REQ(ops)"
                    )
                )

            sorted_stats = []
            for ip, stats in all_stats.items():
                interval_for_calc = interval if interval > 0 else 1
                wr_mbps = (stats["wr_bytes"] / 1048576.0) / interval_for_calc
                rd_mbps = (stats["rd_bytes"] / 1048576.0) / interval_for_calc

                filtered_ops = {k: v for k, v in stats["ops"].items() if v >= threshold}
                total_reqs = sum(filtered_ops.values())

                filtered_locks = {
                    k: v for k, v in stats["locks"].items() if v >= threshold
                }
                total_locks = sum(filtered_locks.values())

                if (
                    stats["wr_bytes"] == 0
                    and stats["rd_bytes"] == 0
                    and not filtered_ops
                    and not filtered_locks
                ):
                    continue

                top_ops = sorted(
                    filtered_ops.items(), key=lambda x: x[1], reverse=True
                )[:3]
                if top_ops:
                    top_ops_details = ",".join(
                        [
                            f"{c_cyan(OP_MAP.get(op, op))}:{c_white(int(round(count/interval_for_calc)))}"
                            for op, count in top_ops
                        ]
                    )
                    top_ops_str = f"{c_bwhite(int(round(total_reqs/interval_for_calc)))} ({top_ops_details})"
                else:
                    top_ops_str = c_grey("0")

                if total_locks > 0:
                    max_op_orig, max_val = max(
                        filtered_locks.items(), key=lambda x: x[1]
                    )
                    max_op = OP_MAP.get(max_op_orig, max_op_orig.replace("ldlm_", ""))
                    max_val_ps = int(round(max_val / interval_for_calc))
                    top_locks_str = f"{c_bwhite(int(round(total_locks/interval_for_calc)))} ({c_cyan(max_op)}:{c_white(max_val_ps)})"
                    top_locks_padded = f"{top_locks_str:<51}"
                else:
                    top_locks_str = c_grey("0")
                    top_locks_padded = f"{top_locks_str:<31}"

                sorted_stats.append(
                    (
                        ip,
                        wr_mbps,
                        rd_mbps,
                        top_locks_padded,
                        total_locks,
                        top_ops_str,
                        total_reqs,
                    )
                )

            if current_sort == "write":
                sorted_stats.sort(key=lambda x: (x[1], x[6]), reverse=True)
            elif current_sort == "read":
                sorted_stats.sort(key=lambda x: (x[2], x[6]), reverse=True)
            elif current_sort == "rw":
                sorted_stats.sort(key=lambda x: (x[1] + x[2], x[6]), reverse=True)
            elif current_sort == "lock":
                sorted_stats.sort(key=lambda x: (x[4], x[6]), reverse=True)
            else:  # Default: 'req'
                sorted_stats.sort(key=lambda x: (x[6], x[4], x[1], x[2]), reverse=True)

            print_count = 0
            for stat in sorted_stats:
                if limit and print_count >= limit:
                    break
                (
                    ip,
                    wr_mbps,
                    rd_mbps,
                    top_locks_padded,
                    total_locks,
                    top_ops_str,
                    total_reqs,
                ) = stat
                print(
                    f"{c_green(f'{ip:<18}')} {c_yellow(f'{wr_mbps:<12.1f}')} {c_yellow(f'{rd_mbps:<12.1f}')} {top_locks_padded} {top_ops_str}"
                )
                print_count += 1

            sys.stdout.flush()

            waited = 0.0
            prompt_mode = False

            def check_stdin():
                nonlocal current_sort, prompt_mode, waited
                try:
                    c = sys.stdin.read(1)
                    if not c:
                        return
                    if prompt_mode:
                        prompt_mode = False
                        if c == "1":
                            current_sort = "write"
                        elif c == "2":
                            current_sort = "read"
                        elif c == "3":
                            current_sort = "rw"
                        elif c == "4":
                            current_sort = "lock"
                        elif c == "5":
                            current_sort = "req"

                        if c in "12345":
                            print(c, flush=True)
                            waited = interval  # Force refresh
                        elif c in ("\n", "\r"):
                            print("", flush=True)
                        else:
                            print(f"{c}\n{c_red(f'Invalid option {c!r}.')}", flush=True)
                            waited = interval
                    else:
                        if c == "s":
                            prompt_mode = True
                            print(
                                "\nSort by: [1]write [2]read [3]rw [4]lock [5]req ? ",
                                end="",
                                flush=True,
                            )
                        elif c == "q":
                            raise KeyboardInterrupt
                except Exception as e:
                    if isinstance(e, KeyboardInterrupt):
                        raise

            fd = sys.stdin.fileno()
            try:
                is_tty = os.isatty(fd)
            except Exception:
                is_tty = False

            if is_tty:
                try:
                    old_settings = termios.tcgetattr(fd)
                    tty.setcbreak(fd)
                    os.set_blocking(fd, False)
                    loop = asyncio.get_event_loop()
                    loop.add_reader(fd, check_stdin)
                except Exception:
                    is_tty = False

            try:
                while waited < interval or prompt_mode:
                    await asyncio.sleep(0.1)
                    if not prompt_mode:
                        waited += 0.1
            finally:
                if is_tty:
                    try:
                        loop.remove_reader(fd)
                        os.set_blocking(fd, True)
                        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
                    except Exception:
                        pass
        else:
            is_first = False
            await asyncio.sleep(interval)


def main():
    global SSH_CMD

    epilog = """
Output Abbreviations:
  r:   read      w:   write     cl:  close      op:  open      cr:  create
  st:  statfs    gi:  get_info  pa:  prealloc   pu:  punch     sy:  sync
  ga:  getattr   sa:  setattr   un:  unlink     mk:  mknod     dy:  destroy
  enq: enqueue   cxl: cancel    blc: bl_callback cpc: cp_callback
  glc: gl_callback
"""
    parser = argparse.ArgumentParser(
        description="Report load by client IP for a Lustre mountpoint or SERVER(s).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    parser.add_argument(
        "targets",
        help="Local Lustre mountpoint (starts with '/') OR list of servers",
        nargs="*",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        default=10,
        help="report load over NUMBER seconds (default: 10)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=min(os.cpu_count() or 4, 16),
        help="number of worker processes (default: up to 16 based on CPUs)",
    )
    parser.add_argument(
        "-t",
        "--threshold",
        type=int,
        default=10,
        help="hide operations with count less than NUMBER (default: 10)",
    )
    parser.add_argument(
        "-n",
        "--limit",
        type=int,
        default=10,
        help="limit output to NUMBER clients (default: 10)",
    )
    parser.add_argument(
        "-s",
        "--sort-by",
        default="req",
        choices=["req", "read", "write", "rw", "lock"],
        help="sort by: req (default), read, write, rw, or lock",
    )
    parser.add_argument(
        "--ost", action="store_true", help="query OST components (obdfilter)"
    )
    parser.add_argument("--mdt", action="store_true", help="query MDT components (mdt)")
    parser.add_argument(
        "--no-header", action="store_true", help="do not display header"
    )
    parser.add_argument(
        "--remote-shell",
        default=SSH_CMD,
        help=f"specify remote shell (default: {SSH_CMD})",
    )

    args = parser.parse_args()

    SSH_CMD = f"{args.remote_shell}"

    params = []
    query_all = not args.ost and not args.mdt
    if query_all or args.mdt:
        params.extend(["mdt.*.exports.*.stats", "mdt.*.exports.*.ldlm_stats"])
    if query_all or args.ost:
        params.extend(
            ["obdfilter.*.exports.*.stats", "obdfilter.*.exports.*.ldlm_stats"]
        )
    params_str = " ".join(params)

    if not args.targets:
        parser.print_help()
        sys.exit(1)

    servers = []
    if args.targets[0].startswith("/"):
        if len(args.targets) > 1:
            print("Error: Only one mountpoint can be specified.", file=sys.stderr)
            sys.exit(1)
        servers = get_servers_from_mountpoint(args.targets[0])
    else:
        servers = expand_hostlist(args.targets)

    if not servers:
        print(f"No servers found for targets: {args.targets}", file=sys.stderr)
        sys.exit(1)

    num_workers = min(args.workers, len(servers))
    chunk_size = (len(servers) + num_workers - 1) // num_workers
    workers = []

    for i in range(num_workers):
        chunk = servers[i * chunk_size : (i + 1) * chunk_size]
        if not chunk:
            continue
        parent_conn, child_conn = mp.Pipe()
        p = mp.Process(
            target=worker_process,
            args=(chunk, params_str, child_conn, SSH_CMD),
        )
        p.start()
        workers.append({"process": p, "pipe": parent_conn, "servers": chunk})

    try:
        loop = asyncio.get_event_loop()
        print(
            f"Collecting baseline metrics... (first refresh in {args.interval}s)",
            file=sys.stderr,
        )
        main_task = asyncio.ensure_future(
            main_loop(
                workers,
                args.interval,
                args.threshold,
                args.limit,
                not args.no_header,
                args.sort_by,
                len(servers),
            )
        )
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        pass
    finally:
        # Ignore signals during cleanup
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGCHLD):
            try:
                signal.signal(sig, signal.SIG_IGN)
            except:
                pass

        # Cancel all tasks
        try:
            try:
                pending = asyncio.all_tasks(loop=loop)
            except AttributeError:
                pending = asyncio.Task.all_tasks(loop=loop)

            if pending:
                for task in pending:
                    task.cancel()
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
        except Exception:
            pass

        for w in workers:
            try:
                w["pipe"].send(("quit",))
            except:
                pass
        for w in workers:
            w["process"].join(timeout=1)
            if w["process"].is_alive():
                w["process"].terminate()

        try:
            # Crucial: Reset wakeup fd before closing loop to avoid OSError: [Errno 9] Bad file descriptor
            if hasattr(signal, "set_wakeup_fd"):
                try:
                    signal.set_wakeup_fd(-1)
                except ValueError:
                    pass
            loop.close()
        except:
            pass
        sys.exit(0)


if __name__ == "__main__":
    main()
