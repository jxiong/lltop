#!/usr/bin/env python3

import argparse
import asyncio
import subprocess
import sys
import os
import re
import time
from collections import defaultdict

# Metrics to fetch from MDS and OSS nodes
LUSTRE_LCTL_PARAMS = [
    "mdt.*.exports.*.stats",
    "obdfilter.*.exports.*.stats",
    "mdt.*.exports.*.ldlm_stats",
    "obdfilter.*.exports.*.ldlm_stats"
]

SSH_CMD = "/usr/bin/ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=5"
FANOUT = 32

def expand_node(node_str):
    match = re.search(r'\[(.*?)\]', node_str)
    if not match:
        return [node_str]

    prefix = node_str[:match.start()]
    suffix = node_str[match.end():]
    inner = match.group(1)

    expanded_nodes = []
    for part in inner.split(','):
        part = part.strip()
        if '-' in part:
            try:
                start_str, end_str = part.split('-', 1)
                pad_len = len(start_str) if start_str.startswith('0') else 0
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
        output = subprocess.check_output(cmd, shell=True).decode('utf-8', errors='ignore').strip()
        if not output:
            return []

        fsname = output.split('-')[0].split(' ')[0].strip()
        servers = set()

        for comp in ['osc', 'mdc']:
            cmd = f"lctl get_param -n {comp}.{fsname}-*.conn_uuid 2>/dev/null"
            try:
                uuids = subprocess.check_output(cmd, shell=True).decode('utf-8', errors='ignore').splitlines()
                for uuid in uuids:
                    uuid = uuid.strip()
                    if not uuid: continue
                    if '@' in uuid:
                        ip = uuid.split('@')[0]
                        servers.add(ip)
            except subprocess.CalledProcessError:
                pass

        server_list = list(servers)
        if not server_list:
            return []

        print(f"Deduplicating {len(server_list)} potential server NIDs...", file=sys.stderr)
        
        async def do_dedup():
            sem = asyncio.Semaphore(FANOUT)
            async def get_hostname(server):
                async with sem:
                    cmd = f"{SSH_CMD} {server} 'uname -n'"
                    try:
                        process = await asyncio.create_subprocess_shell(
                            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
                        if process.returncode == 0:
                            hn = stdout.decode('utf-8', errors='ignore').strip()
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

        # If an event loop is already running, this needs to be scheduled differently,
        # but since this runs before main_loop, we can safely create/use a new one or the current one.
        loop = asyncio.get_event_loop()
        deduped = loop.run_until_complete(do_dedup())
        
        print(f"Found {len(deduped)} unique server nodes.", file=sys.stderr)
        return deduped
    except Exception as e:
        print(f"Error discovering servers: {e}", file=sys.stderr)
        return []

async def fetch_metrics(server, timeout_sec, sem):
    async with sem:
        params_str = " ".join(LUSTRE_LCTL_PARAMS)
        cmd = f"{SSH_CMD} {server} 'sudo lctl get_param {params_str} 2>/dev/null'"
        start_time = time.time()
        try:
            process = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            # Give a little extra buffer over the timeout
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_sec + 2)
            elapsed = time.time() - start_time
            if process.returncode != 0 and process.returncode != 124:
                if not stdout:
                    err_msg = stderr.decode('utf-8', errors='ignore').strip() or f"Exited with {process.returncode}"
                    return server, "", elapsed, err_msg
            return server, stdout.decode('utf-8', errors='ignore'), elapsed, None
        except asyncio.TimeoutError:
            try:
                process.kill()
            except:
                pass
            return server, "", time.time() - start_time, "Timed out"
        except Exception as e:
            return server, "", time.time() - start_time, str(e)


def process_metrics(server, metrics_text, history, current_stats, is_first):
    current_target = "unknown"
    current_client = None

    for line in metrics_text.splitlines():
        line = line.strip()
        if not line: continue

        if line.endswith(".stats=") or line.endswith(".ldlm_stats="):
            if ".exports." in line:
                try:
                    target_part, client_part = line.split('.exports.', 1)
                    current_target = target_part.split('.', 1)[1]
                    if client_part.endswith(".stats="):
                        current_client = client_part.rsplit('.stats=', 1)[0].split('@')[0]
                    else:
                        current_client = client_part.rsplit('.ldlm_stats=', 1)[0].split('@')[0]
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
        if diff < 0: diff = 0
        history[key] = val

        if is_first:
            continue

        if diff > 0:
            if metric_type == "bytes":
                if op == "write_bytes":
                    current_stats[current_client]['wr_bytes'] += diff
                elif op == "read_bytes":
                    current_stats[current_client]['rd_bytes'] += diff
            elif metric_type == "stats":
                if op in ("ldlm_enqueue", "ldlm_cancel", "ldlm_bl_callback", "ldlm_cp_callback"):
                    current_stats[current_client]['locks'][op] += diff
                else:
                    current_stats[current_client]['ops'][op] += diff

OP_MAP = {
    'read': 'r',
    'write': 'w',
    'close': 'cl',
    'open': 'op',
    'create': 'cr',
    'statfs': 'st',
    'get_info': 'gi',
    'prealloc': 'pa',
    'punch': 'pu',
    'sync': 'sy',
    'getattr': 'ga',
    'setattr': 'sa',
    'unlink': 'un',
    'mknod': 'mk',
    'destroy': 'dy',
    'ldlm_enqueue': 'enq',
    'ldlm_cancel': 'cxl',
    'ldlm_bl_callback': 'blc'
}

REV_OP_MAP = {v: k for k, v in OP_MAP.items()}


async def main_loop(servers, interval, threshold, limit, print_header, sort_by):
    history = {}
    is_first = True
    sem = asyncio.Semaphore(FANOUT)

    while True:
        current_stats = defaultdict(lambda: {'wr_bytes': 0, 'rd_bytes': 0, 'ops': defaultdict(int), 'locks': defaultdict(int)})

        # Determine a reasonable curl timeout based on the interval
        curl_timeout = max(5, interval - 2)
        tasks = [fetch_metrics(srv, curl_timeout, sem) for srv in servers]
        results = await asyncio.gather(*tasks)

        max_fetch_time = 0
        errors = []
        for server, metrics_text, elapsed, error in results:
            max_fetch_time = max(max_fetch_time, elapsed)
            if error:
                errors.append((server, error))
            else:
                process_metrics(server, metrics_text, history, current_stats, is_first)

        if errors:
            for srv, err in errors:
                print(f"Error fetching metrics from {srv}: {err}", file=sys.stderr)
            sys.exit(1)

        if not is_first:
            # Clear screen
            print("\033[H\033[J", end='')

            if print_header:
                warning = ""
                if max_fetch_time > interval:
                    warning = f" \033[1;31m[WARNING: Fetch time exceeds interval!]\033[0m"

                print(f"\033[1;30mServers: {len(servers)} | Max Fetch Time: {max_fetch_time:.2f}s | Interval: {interval}s\033[0m{warning}")
                # Bold Cyan header
                print(f"\033[1;36m{'CLIENT_IP':<18} {'WRITE(MBps)':<12} {'READ(MBps)':<12} {'LOCK(ops)':<22} REQ(ops)\033[0m")

            sorted_stats = []
            for ip, stats in current_stats.items():
                wr_mbps = (stats['wr_bytes'] / 1048576.0) / interval
                rd_mbps = (stats['rd_bytes'] / 1048576.0) / interval

                filtered_ops = {k: v for k, v in stats['ops'].items() if v >= threshold}
                total_reqs = sum(filtered_ops.values())

                filtered_locks = {k: v for k, v in stats['locks'].items() if v >= threshold}
                total_locks = sum(filtered_locks.values())

                if stats['wr_bytes'] == 0 and stats['rd_bytes'] == 0 and not filtered_ops and not filtered_locks:
                    continue

                top_ops = sorted(filtered_ops.items(), key=lambda x: x[1], reverse=True)[:3]
                if top_ops:
                    # Cyan operation names, White operation counts
                    top_ops_details = ",".join([f"\033[36m{OP_MAP.get(op, op)}\033[0m:\033[37m{int(round(count/interval))}\033[0m" for op, count in top_ops])
                    # Bold White for total operations
                    top_ops_str = f"\033[1;37m{int(round(total_reqs/interval))}\033[0m ({top_ops_details})"
                else:
                    top_ops_str = "\033[90m-\033[0m" # Dark grey for empty

                if total_locks > 0:
                    max_op_orig, max_val = max(filtered_locks.items(), key=lambda x: x[1])
                    max_op = OP_MAP.get(max_op_orig, max_op_orig.replace('ldlm_', ''))
                    max_val_ps = int(round(max_val / interval))
                    top_locks_str = f"\033[1;37m{int(round(total_locks/interval))}\033[0m (\033[36m{max_op}\033[0m:\033[37m{max_val_ps}\033[0m)"
                    # Visual width is roughly len(str(total)) + 2 + len(max_op) + 1 + len(str(max_val_ps)) + 1
                    # ANSI overhead is 29 characters. To get 22 visual chars, we pad to 51.
                    top_locks_padded = f"{top_locks_str:<51}"
                else:
                    top_locks_str = "\033[90m0\033[0m"
                    # ANSI overhead is 9. To get 22 visual chars, pad to 31.
                    top_locks_padded = f"{top_locks_str:<31}"

                sorted_stats.append((ip, wr_mbps, rd_mbps, top_locks_padded, total_locks, top_ops_str, total_reqs, stats))

            # Apply sorting based on the requested sort_by parameter
            if sort_by == 'bw':
                sorted_stats.sort(key=lambda x: (x[1] + x[2], x[6]), reverse=True)
            elif sort_by in REV_OP_MAP:
                long_op = REV_OP_MAP[sort_by]
                sorted_stats.sort(key=lambda x: (x[7]['ops'].get(long_op, 0) + x[7]['locks'].get(long_op, 0), x[6]), reverse=True)
            else: # Default: sort by total_reqs desc, total_locks desc, wr_mbps desc, rd_mbps desc
                sorted_stats.sort(key=lambda x: (x[6], x[4], x[1], x[2]), reverse=True)

            print_count = 0
            for stat in sorted_stats:
                if limit and print_count >= limit:
                    break
                ip, wr_mbps, rd_mbps, top_locks_padded, total_locks, top_ops_str, total_reqs, _ = stat
                # Green IP, Yellow bandwidth numbers
                print(f"\033[92m{ip:<18}\033[0m \033[93m{wr_mbps:<12.1f}\033[0m \033[93m{rd_mbps:<12.1f}\033[0m {top_locks_padded} {top_ops_str}")
                print_count += 1

            sys.stdout.flush()
            await asyncio.sleep(interval)
        else:
            is_first = False

def main():
    global SSH_CMD, FANOUT

    # Dynamically generate the mappings help text
    map_str = ""
    count = 0
    for short_name, long_name in sorted(REV_OP_MAP.items(), key=lambda x: x[0]):
        map_str += f"{short_name:<3}: {long_name:<18}"
        count += 1
        if count % 4 == 0:
            map_str += "\n"
    
    epilog = f"""
Operation & Lock Mappings (--sort-by support):
{map_str}
"""
    parser = argparse.ArgumentParser(
        description="Report load by client IP for a Lustre mountpoint or SERVER(s).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog)
    parser.add_argument("targets", help="Local Lustre mountpoint (starts with '/') OR list of servers", nargs="*")
    parser.add_argument("-i", "--interval", type=int, default=10, help="report load over NUMBER seconds (default: 10)")
    parser.add_argument("-f", "--fanout", type=int, default=FANOUT, help=f"limit concurrent SSH processes (default: {FANOUT})")
    parser.add_argument("-t", "--threshold", type=int, default=10, help="hide operations with count less than NUMBER (default: 10)")
    parser.add_argument("-n", "--limit", type=int, default=10, help="limit output to NUMBER clients (default: 10)")
    parser.add_argument("-s", "--sort-by", default="req", help="sort by: req (default), bw, or op short name (e.g., cr, op)")
    parser.add_argument("--no-header", action="store_true", help="do not display header")
    parser.add_argument("--remote-shell", default=SSH_CMD, help=f"use shell to execute remote command (default: {SSH_CMD})")

    args = parser.parse_args()

    SSH_CMD = args.remote_shell
    FANOUT = args.fanout

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

    try:
        loop = asyncio.get_event_loop()
        main_task = asyncio.ensure_future(
            main_loop(servers, args.interval, args.threshold, args.limit, not args.no_header, args.sort_by)
        )
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        main_task.cancel()
        # Gather all pending tasks and cancel them
        pending = asyncio.Task.all_tasks(loop=loop)
        for task in pending:
            task.cancel()
        # Run loop momentarily to let cancellations process and avoid warnings
        try:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except asyncio.CancelledError:
            pass
    finally:
        try:
            if not loop.is_running():
                loop.close()
        except:
            pass
        sys.exit(0)

if __name__ == '__main__':
    main()
