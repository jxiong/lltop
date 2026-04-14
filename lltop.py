#!/usr/bin/env python3

import argparse
import asyncio
import subprocess
import sys
import os
import re
import time
from collections import defaultdict

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

        return list(servers)
    except Exception as e:
        print(f"Error discovering servers: {e}", file=sys.stderr)
        return []

async def fetch_metrics(server, ssh_path, timeout_sec):
    cmd = f"{ssh_path} -C -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=2 {server} 'curl -m {timeout_sec} -s http://localhost:32221/metrics'"
    start_time = time.time()
    try:
        process = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        # Give a little extra buffer over the curl timeout
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_sec + 2)
        elapsed = time.time() - start_time
        if process.returncode != 0:
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


def extract_label(line, label):
    prefix = f'{label}="'
    idx = line.find(prefix)
    if idx == -1: return None
    start = idx + len(prefix)
    end = line.find('"', start)
    return line[start:end] if end != -1 else None

def process_metrics(server, metrics_text, history, current_stats, is_first):
    for line in metrics_text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'): continue

        is_bytes = line.startswith("lustre_client_export_bytes_total")
        is_stats = line.startswith("lustre_client_export_stats")
        if not (is_bytes or is_stats): continue

        client_ip = extract_label(line, "nid")
        if not client_ip: continue
        client_ip = client_ip.split('@')[0]

        op = extract_label(line, "name")
        if not op:
            if "write_bytes" in line: op = "write_bytes"
            elif "read_bytes" in line: op = "read_bytes"
            else: op = "unknown"

        # Ignore sample counts for bytes and pings to avoid double counting and key collisions
        if is_stats and op in ("write_bytes", "read_bytes", "ping"):
            continue

        target = extract_label(line, "target") or "unknown"

        try:
            val_str = line.rsplit('}', 1)[-1].strip()
            val = int(val_str)
        except ValueError:
            continue

        metric_type = "bytes" if is_bytes else "stats"
        key = (server, target, client_ip, metric_type, op)
        prev_val = history.get(key, 0)

        diff = val - prev_val
        if diff < 0: diff = 0
        history[key] = val

        if is_first:
            continue

        if diff > 0:
            if is_bytes:
                if op == "write_bytes":
                    current_stats[client_ip]['wr_bytes'] += diff
                elif op == "read_bytes":
                    current_stats[client_ip]['rd_bytes'] += diff
            elif is_stats:
                current_stats[client_ip]['ops'][op] += diff

async def main_loop(servers, interval, threshold, limit, ssh_path, print_header):
    history = {}
    is_first = True

    while True:
        current_stats = defaultdict(lambda: {'wr_bytes': 0, 'rd_bytes': 0, 'ops': defaultdict(int)})

        # Determine a reasonable curl timeout based on the interval
        curl_timeout = max(5, interval - 2)
        tasks = [fetch_metrics(srv, ssh_path, curl_timeout) for srv in servers]
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
                print(f"\033[1;36m{'CLIENT_IP':<18} {'WRITE_MBps':<12} {'READ_MBps':<12}    REQps\033[0m")

            sorted_stats = []
            for ip, stats in current_stats.items():
                wr_mbps = (stats['wr_bytes'] / 1048576.0) / interval
                rd_mbps = (stats['rd_bytes'] / 1048576.0) / interval

                filtered_ops = {k: v for k, v in stats['ops'].items() if v >= threshold}
                total_reqs = sum(filtered_ops.values())

                if stats['wr_bytes'] == 0 and stats['rd_bytes'] == 0 and not filtered_ops:
                    continue

                top_ops = sorted(filtered_ops.items(), key=lambda x: x[1], reverse=True)[:3]
                if top_ops:
                    # Cyan operation names, White operation counts
                    top_ops_details = ",".join([f"\033[36m{op}\033[0m:\033[37m{int(round(count/interval))}\033[0m" for op, count in top_ops])
                    # Bold White for total operations
                    top_ops_str = f"\033[1;37m{int(round(total_reqs/interval))}\033[0m ({top_ops_details})"
                else:
                    top_ops_str = "\033[90m-\033[0m" # Dark grey for empty

                sorted_stats.append((ip, wr_mbps, rd_mbps, top_ops_str, total_reqs))

            # Sort by total_reqs desc, wr_mbps desc, rd_mbps desc
            sorted_stats.sort(key=lambda x: (x[4], x[1], x[2]), reverse=True)

            print_count = 0
            for stat in sorted_stats:
                if limit and print_count >= limit:
                    break
                ip, wr_mbps, rd_mbps, top_ops_str, total_reqs = stat
                # Green IP, Yellow bandwidth numbers
                print(f"\033[92m{ip:<18}\033[0m \033[93m{wr_mbps:<12.1f}\033[0m \033[93m{rd_mbps:<12.1f}\033[0m    {top_ops_str}")
                print_count += 1

            sys.stdout.flush()
            await asyncio.sleep(interval)
        else:
            is_first = False

def main():
    parser = argparse.ArgumentParser(description="Report load by client IP for a Lustre mountpoint or SERVER(s).")
    parser.add_argument("targets", help="Local Lustre mountpoint (starts with '/') OR list of servers", nargs="*")
    parser.add_argument("-i", "--interval", type=int, default=10, help="report load over NUMBER seconds")
    parser.add_argument("-t", "--threshold", type=int, default=10, help="hide operations with count less than NUMBER (default: 10)")
    parser.add_argument("-n", "--limit", type=int, default=0, help="limit output to NUMBER clients")
    parser.add_argument("--no-header", action="store_true", help="do not display header")
    parser.add_argument("--remote-shell", default="/usr/bin/ssh", help="use remote shell at PATH to execute SSH")

    args = parser.parse_args()

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
        # In Python 3.6, cancelling all tasks gracefully is complicated manually.
        # However, run_until_complete returning handles clean shutdown natively
        # if the exception isn't caught until outside.
        loop.run_until_complete(main_loop(servers, args.interval, args.threshold, args.limit, args.remote_shell, not args.no_header))
    except KeyboardInterrupt:
        pass
    finally:
        try:
            # We don't strictly need to close the loop in a short-lived CLI script,
            # but if we do, we should check if it's running. In Py3.6 close() fails if running.
            if not loop.is_running():
                loop.close()
        except:
            pass
        sys.exit(0)

if __name__ == '__main__':
    main()
