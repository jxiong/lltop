# Lltop

`lltop` is a command-line utility that gathers real-time I/O statistics from Lustre filesystem servers and displays them in a modern, colorized, `top`-like interface, aggregated by client IP address.

It queries the backend MDS and OSS servers using asynchronous SSH and pulls Prometheus metrics directly from the nodes (`http://localhost:32221/metrics`). By comparing metrics between intervals, `lltop` provides an accurate view of current bandwidth (`WRITE_MB`, `READ_MB`) and requested operations (`TOP_OPS`) originating from each connected client.

## Features

*   **Automatic Server Discovery:** Provide a local Lustre mountpoint (e.g., `/mnt/lustre`), and `lltop` will automatically query `lfs getname` and `lctl get_param` to discover all associated MDS and OSS IP addresses.
*   **Custom Server Lists:** Use `clush`-style host range expansions (e.g., `oss[1-10] mds[1,2]`) to query specific groups of servers manually simply by passing them as positional arguments.
*   **Top-like Interface:** Automatically clears the terminal and refreshes metrics every N seconds, color-coding the output for readability.
*   **Operation Filtering:** Filters out noisy, low-count operations (e.g., operations occurring < 10 times per interval) so you can focus on the primary workload types.
*   **Highly Concurrent:** Uses Python's `asyncio` to execute all SSH and `curl` queries concurrently, preventing single slow nodes from blocking the update cycle.
*   **Standalone Binary:** The project ships with a `Makefile` that uses PyInstaller to bundle `lltop.py` into a single, standalone Linux executable with no external Python dependencies.

## Usage

By default, `lltop` is invoked with the path to a Lustre mountpoint (must start with `/`):

```bash
$ ./lltop /testfs
```

Alternatively, you can provide a list of specific servers to query as positional arguments. If the first argument does not start with `/`, `lltop` treats all arguments as server hostnames (supporting `clush` syntax):

```bash
$ ./lltop oss[1-5] mds1
```

### Options

```text
Usage: lltop [OPTION]... TARGETS...

Report load by client IP for a Lustre mountpoint or SERVER(s).

Positional arguments:
  targets               Local Lustre mountpoint (starts with '/') OR list of servers

Optional arguments:
  -h, --help            show this help message and exit
  -i, --interval NUMBER report load over NUMBER seconds (default: 10)
  -t, --threshold NUM   hide operations with count less than NUMBER (default: 10)
  -n, --limit NUMBER    limit output to NUMBER clients (default: 0 = unlimited)
  --no-header           do not display header
  --remote-shell PATH   use remote shell at PATH to execute SSH (default: /usr/bin/ssh)
```

## Output Columns

The output looks like this:

```text
Servers: 12 | Max Fetch Time: 0.45s | Interval: 10s
CLIENT_IP           WRITE_MBps  READ_MBps    REQps
10.128.0.2          0.0         0.0          145 (write:90,read:40,statfs:15)
10.128.0.14         0.0         0.0          52 (statfs:50,open:2)
```

*   **Header Info**: Shows the number of servers being queried, the maximum response time of the slowest server in that interval (`Max Fetch Time`), and the current refresh `Interval`. If the `Max Fetch Time` exceeds the `Interval`, a red warning is displayed.
*   **CLIENT_IP**: The IP address of the Lustre client generating the traffic.
*   **WRITE_MBps**: Megabytes per second written by this client in the last interval.
*   **READ_MBps**: Megabytes per second read by this client in the last interval.
*   **REQps**: A summary of the RPC operations per second performed by the client. It is formatted as `total_operations_per_sec (op1:rate, op2:rate, op3:rate)`. It displays up to the top 3 operations per second, sorted by frequency, that exceed the `--threshold` limit.

## Installation / Building

`lltop` is written in Python but designed to be deployed as a compiled binary.

To build the standalone executable, simply run:

```bash
make
```

This uses `pip` and `PyInstaller` to generate a self-contained `lltop` binary in the current directory. You can then copy this executable to any system in your cluster and run it directly.

## Under the Hood

The tool operates by executing the following command on each backend server:

```bash
ssh -C -o BatchMode=yes -o StrictHostKeyChecking=no <server_ip> 'curl -s http://localhost:32221/metrics'
```

It parses the resulting Prometheus exposition text, specifically looking for `lustre_client_export_bytes_total` and `lustre_client_export_stats` metric families. It tracks the cumulative counters in memory and computes the exact deltas (differences) between each interval to generate the live I/O rates.
