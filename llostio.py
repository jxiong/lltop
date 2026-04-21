#!/usr/bin/python3
import sys
import time
import re
import argparse
import asyncio
from util import (
    expand_node,
    expand_hostlist,
    SSHWorker,
    clear_screen,
    c_red,
    c_green,
    c_yellow,
    c_cyan,
    c_white,
    c_grey,
    c_bcyan,
    c_bwhite,
)

# --- Utility Functions ---


def parse_stats(output):
    stats = {}
    current_ost = None
    for line in output.splitlines():
        if "=" in line:
            match = re.match(r"obdfilter\.(.*?)\.stats=", line)
            if match:
                current_ost = match.group(1)
                if current_ost not in stats:
                    stats[current_ost] = {}
            continue

        if current_ost:
            parts = line.split()
            if not parts:
                continue

            name = parts[0]
            if name == "snapshot_time":
                stats[current_ost]["time"] = float(parts[1])
            elif name in ["read_bytes", "write_bytes", "write", "read"]:
                if len(parts) >= 7:
                    stats[current_ost][name] = {
                        "samples": int(parts[1]),
                        "sum": int(parts[6]),
                    }
    return stats


async def fetch_worker_stats(worker, timeout_sec):
    cmd = "lctl get_param obdfilter.*.stats 2>/dev/null"
    stdout, elapsed, error = await worker.run_cmd(cmd, timeout_sec)
    if error:
        return worker.server, {}, error
    return worker.server, parse_stats(stdout), None


# --- Main Application ---


def print_table(current, last, sort_by, top_n, show_header=True):
    ost_metrics = []

    for ost in current.keys():
        if ost not in last:
            continue

        c = current[ost]
        l = last[ost]

        dt = c["time"] - l["time"]
        if dt <= 0:
            continue

        # Write Metrics
        dw_bytes = c.get("write_bytes", {}).get("sum", 0) - l.get(
            "write_bytes", {}
        ).get("sum", 0)
        dw_samples = c.get("write_bytes", {}).get("samples", 0) - l.get(
            "write_bytes", {}
        ).get("samples", 0)
        w_iops = dw_samples / dt
        w_bw = dw_bytes / dt / (1024 * 1024)
        w_sz = dw_bytes / dw_samples / 1024 if dw_samples > 0 else 0

        dw_usecs = c.get("write", {}).get("sum", 0) - l.get("write", {}).get("sum", 0)
        w_lat = dw_usecs / dw_samples if dw_samples > 0 else 0

        # Read Metrics
        dr_bytes = c.get("read_bytes", {}).get("sum", 0) - l.get("read_bytes", {}).get(
            "sum", 0
        )
        dr_samples = c.get("read_bytes", {}).get("samples", 0) - l.get(
            "read_bytes", {}
        ).get("samples", 0)
        r_iops = dr_samples / dt
        r_bw = dr_bytes / dt / (1024 * 1024)
        r_sz = dr_bytes / dr_samples / 1024 if dr_samples > 0 else 0

        dr_usecs = c.get("read", {}).get("sum", 0) - l.get("read", {}).get("sum", 0)
        r_lat = dr_usecs / dr_samples if dr_samples > 0 else 0

        ost_display = ost.split("-")[-1]

        total_bw = w_bw + r_bw
        total_iops = w_iops + r_iops
        avg_lat = (
            (w_lat + r_lat) / 2.0 if (w_lat > 0 and r_lat > 0) else max(w_lat, r_lat)
        )

        ost_metrics.append(
            {
                "ost": ost_display,
                "w_bw": w_bw,
                "w_iops": w_iops,
                "w_sz": w_sz,
                "w_lat": w_lat,
                "r_bw": r_bw,
                "r_iops": r_iops,
                "r_sz": r_sz,
                "r_lat": r_lat,
                "total_bw": total_bw,
                "total_iops": total_iops,
                "avg_lat": avg_lat,
            }
        )

    # Sorting
    if sort_by == "throughput":
        ost_metrics.sort(key=lambda x: x["total_bw"], reverse=True)
    elif sort_by == "iops":
        ost_metrics.sort(key=lambda x: x["total_iops"], reverse=True)
    elif sort_by == "latency":
        ost_metrics.sort(key=lambda x: x["avg_lat"], reverse=True)

    # Top N
    if top_n > 0:
        ost_metrics = ost_metrics[:top_n]

    # Print
    clear_screen()

    header_parts = [
        c_bcyan(f"{'OST':<10}"),
        c_cyan(f"{'W_MBps':>10}"),
        c_cyan(f"{'W_IOPS':>8}"),
        c_cyan(f"{'W_sz_KB':>10}"),
        c_cyan(f"{'W_lat_ms':>10}"),
        c_yellow(f"{'R_MBps':>10}"),
        c_yellow(f"{'R_IOPS':>8}"),
        c_yellow(f"{'R_sz_KB':>10}"),
        c_yellow(f"{'R_lat_ms':>10}"),
    ]
    header = " ".join(header_parts)

    if show_header:
        print(header)
        print(c_grey("-" * 94))

    for m in ost_metrics:
        # Calculate latency in ms
        w_lat_ms = m["w_lat"] / 1000.0 if m["w_lat"] > 0 else 0.0
        r_lat_ms = m["r_lat"] / 1000.0 if m["r_lat"] > 0 else 0.0

        # Highlight high latency (e.g. > 10 ms) in red, otherwise base color
        w_lat_str = (
            c_red(f"{w_lat_ms:>10.1f}")
            if w_lat_ms > 100
            else c_cyan(f"{w_lat_ms:>10.1f}")
        )
        r_lat_str = (
            c_red(f"{r_lat_ms:>10.1f}")
            if r_lat_ms > 100
            else c_yellow(f"{r_lat_ms:>10.1f}")
        )

        # Color throughput based on value (> 1000MB/s red, otherwise base color)
        w_bw_color = c_red if m["w_bw"] > 1000 else c_cyan
        r_bw_color = c_red if m["r_bw"] > 1000 else c_yellow

        w_bw_str = w_bw_color(f"{m['w_bw']:>10.2f}")
        r_bw_str = r_bw_color(f"{m['r_bw']:>10.2f}")

        w_iops_str = c_cyan(f"{m['w_iops']:>8.0f}")
        w_sz_str = c_cyan(f"{m['w_sz']:>10.1f}")
        r_iops_str = c_yellow(f"{m['r_iops']:>8.0f}")
        r_sz_str = c_yellow(f"{m['r_sz']:>10.1f}")

        ost_str = c_white(f"{m['ost']:<10}")

        print(
            f"{ost_str} {w_bw_str} {w_iops_str} {w_sz_str} {w_lat_str} {r_bw_str} {r_iops_str} {r_sz_str} {r_lat_str}"
        )
    print()


async def async_main(args):
    hosts = expand_hostlist(args.nodes) if args.nodes else ["localhost"]
    workers = [SSHWorker(host) for host in hosts]

    # Initial connection
    await asyncio.gather(*[w.connect() for w in workers])

    last_stats = {}

    try:
        while True:
            # Fetch stats from all workers concurrently
            tasks = [fetch_worker_stats(w, args.interval + 5) for w in workers]
            results = await asyncio.gather(*tasks)

            current_stats = {}
            errors = []
            for server, stats, error in results:
                if error:
                    errors.append(f"{server}: {error}")
                else:
                    current_stats.update(stats)

            if errors and not current_stats:
                print("Errors fetching stats:", errors)

            if last_stats and current_stats:
                print_table(
                    current_stats, last_stats, args.sort, args.top, not args.no_header
                )
                if errors:
                    print(f"Warnings/Errors: {', '.join(errors)}")
            elif not current_stats and not errors:
                print("Waiting for stats...")

            last_stats = current_stats
            await asyncio.sleep(args.interval)

    except asyncio.CancelledError:
        pass
    finally:
        for w in workers:
            w.close()


def main():
    parser = argparse.ArgumentParser(description="Lustre OST stats monitor")
    parser.add_argument(
        "nodes",
        type=str,
        nargs="*",
        help="List of OSS nodes (e.g. oss[1-4],oss5). If omitted, runs locally.",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        default=1,
        help="Interval between updates in seconds",
    )
    parser.add_argument(
        "--no-header", action="store_true", help="Do not print the header"
    )
    parser.add_argument(
        "-s",
        "--sort",
        choices=["throughput", "latency", "iops"],
        default="throughput",
        help="Sort metric (default: throughput)",
    )
    parser.add_argument(
        "-t",
        "--top",
        type=int,
        default=10,
        help="Number of top OSTs to display (default: 10, 0 for all)",
    )

    args = parser.parse_args()

    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(async_main(args))
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        if "loop" in locals():
            loop.close()


if __name__ == "__main__":
    main()
