"""
benchmark.py
============
Service-lookup cache benchmark.

For each sampled stop within 1 km of Lancaster centre:
    1. Send repeated GET /services/<NODEID> requests.
    2. Treat the first worker-sized window as the cold or warm-up phase.
    3. Treat the last worker-sized window as the warmed-cache phase.

The script appends aggregate and per-node cold-vs-warm latency results to
record.txt so it is easy to see whether the service_lookup cache is visible.

Examples:
    python benchmark.py
    python benchmark.py --base http://10.32.174.225:8080 --nodes 8 --passes 16
"""

import argparse
import datetime
import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request


DEFAULT_BASE = os.environ.get("BENCHMARK_BASE", "http://172.17.0.2:8080")
DEFAULT_CENTRE = (54.0476, -2.8015)
DEFAULT_RADIUS_KM = 1.0
DEFAULT_NODE_COUNT = 8
DEFAULT_PASSES = 16
DEFAULT_WORKERS = 8
DEFAULT_TIMEOUT = 30

HERE = os.path.dirname(os.path.abspath(__file__))
RECORD = os.path.join(HERE, "record.txt")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmark /services/<node_id> latency to check whether service_lookup cache is visible."
    )
    parser.add_argument("--base", default=DEFAULT_BASE, help="Webserver base URL.")
    parser.add_argument(
        "--nodes",
        type=int,
        default=DEFAULT_NODE_COUNT,
        help="Number of nearby stops to benchmark.",
    )
    parser.add_argument(
        "--passes",
        type=int,
        default=DEFAULT_PASSES,
        help="Sequential requests per stop.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Expected process-pool worker count used to size cold and warm windows.",
    )
    parser.add_argument(
        "--radius-km",
        type=float,
        default=DEFAULT_RADIUS_KM,
        help="Radius around Lancaster centre used to sample stops.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="Timeout in seconds for each HTTP request.",
    )
    args = parser.parse_args()

    if args.nodes <= 0:
        parser.error("--nodes must be positive")
    if args.passes < 2:
        parser.error("--passes must be at least 2")
    if args.workers <= 0:
        parser.error("--workers must be positive")
    if args.radius_km <= 0:
        parser.error("--radius-km must be positive")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    return args


def haversine(lat1, lon1, lat2, lon2):
    radius_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return radius_km * 2 * math.asin(math.sqrt(a))


def mean(values):
    return sum(values) / len(values) if values else 0.0


def percentile(values, fraction):
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = max(0, min(len(sorted_values) - 1, math.ceil(len(sorted_values) * fraction) - 1))
    return sorted_values[index]


def median(values):
    if not values:
        return 0.0
    sorted_values = sorted(values)
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2 == 1:
        return sorted_values[mid]
    return (sorted_values[mid - 1] + sorted_values[mid]) / 2


def fetch_stops_within_radius(base_url, radius_km, timeout):
    lat, lon = DEFAULT_CENTRE
    url = f"{base_url}/map?lat={lat}&long={lon}"
    with urllib.request.urlopen(url, timeout=timeout) as response:
        payload = json.loads(response.read())

    nodes = payload.get("nodes")
    if nodes is None:
        nodes = payload.get("information", {}).get("nodes", [])

    nearby = []
    seen = set()
    for node in nodes:
        node_id = node.get("id")
        location = node.get("location") or {}
        if not node_id or node_id in seen:
            continue
        try:
            node_lat = float(location["lat"])
            node_lon = float(location["long"])
        except (KeyError, TypeError, ValueError):
            continue
        if haversine(lat, lon, node_lat, node_lon) <= radius_km:
            nearby.append(node_id)
            seen.add(node_id)
    return nearby


def service_request(base_url, node_id, timeout):
    encoded_node_id = urllib.parse.quote(node_id, safe="")
    url = f"{base_url}/services/{encoded_node_id}"
    started = time.perf_counter()

    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            status = getattr(response, "status", None) or response.getcode()
            body = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        status = exc.code
        raw = exc.read()
        body = json.loads(raw) if raw else {}

    elapsed = time.perf_counter() - started
    services = body.get("services") if isinstance(body, dict) else []
    services_count = len(services) if isinstance(services, list) else 0
    return elapsed, status, services_count


def benchmark_node(base_url, node_id, passes, window_size, timeout):
    latencies = []
    status_codes = []
    services_count = 0

    for _ in range(passes):
        elapsed, status, current_services_count = service_request(base_url, node_id, timeout)
        latencies.append(elapsed)
        status_codes.append(status)
        services_count = current_services_count

    non_200 = [status for status in status_codes if status != 200]
    if non_200:
        raise RuntimeError(f"non-200 statuses seen: {non_200}")

    cold_samples = latencies[:window_size]
    warm_samples = latencies[-window_size:]
    cold_avg = mean(cold_samples)
    warm_avg = mean(warm_samples)

    return {
        "node_id": node_id,
        "services_count": services_count,
        "first": latencies[0],
        "last": latencies[-1],
        "cold_avg": cold_avg,
        "warm_avg": warm_avg,
        "warm_min": min(warm_samples),
        "warm_max": max(warm_samples),
        "speedup": (cold_avg / warm_avg) if warm_avg > 0 else 0.0,
        "improved": warm_avg < cold_avg,
    }


def summarize_results(results):
    cold_values = [result["cold_avg"] for result in results]
    warm_values = [result["warm_avg"] for result in results]
    speedups = [result["speedup"] for result in results if result["speedup"] > 0]
    improved_count = sum(1 for result in results if result["improved"])

    overall_cold = mean(cold_values)
    overall_warm = mean(warm_values)
    overall_speedup = (overall_cold / overall_warm) if overall_warm > 0 else 0.0

    if not results:
        signal = "No data"
    elif overall_speedup >= 1.20 and improved_count >= math.ceil(len(results) * 0.6):
        signal = "Cache signal detected"
    elif overall_speedup >= 1.05:
        signal = "Weak cache signal"
    else:
        signal = "No clear cache signal"

    return {
        "count": len(results),
        "overall_cold": overall_cold,
        "overall_warm": overall_warm,
        "overall_speedup": overall_speedup,
        "cold_min": min(cold_values) if cold_values else 0.0,
        "cold_max": max(cold_values) if cold_values else 0.0,
        "cold_p95": percentile(cold_values, 0.95),
        "warm_min": min(warm_values) if warm_values else 0.0,
        "warm_max": max(warm_values) if warm_values else 0.0,
        "warm_p95": percentile(warm_values, 0.95),
        "speedup_median": median(speedups),
        "speedup_min": min(speedups) if speedups else 0.0,
        "speedup_max": max(speedups) if speedups else 0.0,
        "improved_count": improved_count,
        "signal": signal,
    }


def write_record(args, sampled_stops, window_size, results, errors, summary):
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    lines = [
        "=" * 70,
        "Benchmark : /services/<node_id>  [cache latency, repeated requests]",
        f"Date      : {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"Server    : {args.base}",
        f"Radius    : {args.radius_km:.2f}km around Lancaster (lat={DEFAULT_CENTRE[0]}, long={DEFAULT_CENTRE[1]})",
        f"Stops     : sampled {len(sampled_stops)} nearby nodes",
        f"Passes    : {args.passes} requests per node",
        f"Window    : first {window_size} passes vs last {window_size} passes",
        f"Workers   : assumed process pool size = {args.workers}",
        "=" * 70,
    ]

    if results:
        lines += [
            f"Completed : {summary['count']}   Errors: {len(errors)}",
            f"Signal    : {summary['signal']}",
            f"Cold avg  : {summary['overall_cold']:.4f}s   p95={summary['cold_p95']:.4f}s   min={summary['cold_min']:.4f}s   max={summary['cold_max']:.4f}s",
            f"Warm avg  : {summary['overall_warm']:.4f}s   p95={summary['warm_p95']:.4f}s   min={summary['warm_min']:.4f}s   max={summary['warm_max']:.4f}s",
            f"Speedup   : overall={summary['overall_speedup']:.2f}x   median={summary['speedup_median']:.2f}x   min={summary['speedup_min']:.2f}x   max={summary['speedup_max']:.2f}x",
            f"Improved  : {summary['improved_count']}/{summary['count']} nodes showed warm < cold average",
        ]

    lines += [
        "",
        f"  {'idx':>3}  {'node_id':<16}  {'services':>8}  {'first':>8}  {'cold_avg':>8}  {'warm_avg':>8}  {'last':>8}  {'speedup':>8}",
    ]

    for index, result in enumerate(results):
        lines.append(
            f"  {index:>3}  {result['node_id']:<16}  {result['services_count']:>8}  "
            f"{result['first']:>7.3f}s  {result['cold_avg']:>7.3f}s  {result['warm_avg']:>7.3f}s  "
            f"{result['last']:>7.3f}s  {result['speedup']:>7.2f}x"
        )

    if errors:
        lines.append("")
        lines.append(f"Errors ({len(errors)}):")
        for node_id, error_message in errors:
            lines.append(f"  {node_id}: {error_message}")

    lines.append("")

    with open(RECORD, "a", encoding="utf-8") as record_file:
        record_file.write("\n".join(lines) + "\n")


def main():
    args = parse_args()
    window_size = min(args.workers, args.passes // 2)

    print("Fetching Lancaster stops ...")
    stops = fetch_stops_within_radius(args.base, args.radius_km, args.timeout)
    sampled_stops = stops[: args.nodes]
    print(f"  Found {len(stops)} stops within {args.radius_km:.2f} km")
    print(f"  Benchmarking {len(sampled_stops)} stops")
    print(
        f"  Comparing first {window_size} requests against last {window_size} requests for each stop\n"
    )

    if not sampled_stops:
        raise RuntimeError("No stops found for benchmarking")

    results = []
    errors = []

    for index, node_id in enumerate(sampled_stops):
        try:
            result = benchmark_node(args.base, node_id, args.passes, window_size, args.timeout)
            results.append(result)
            print(
                f"  [{index:>2}] {node_id:<16} first={result['first']:.3f}s "
                f"cold={result['cold_avg']:.3f}s warm={result['warm_avg']:.3f}s "
                f"speedup={result['speedup']:.2f}x services={result['services_count']}"
            )
        except Exception as exc:
            errors.append((node_id, str(exc)))
            print(f"  [{index:>2}] {node_id:<16} ERROR: {exc}")

    summary = summarize_results(results)

    print()
    if results:
        print("=" * 60)
        print(f"  Nodes benchmarked : {summary['count']}  (errors: {len(errors)})")
        print(f"  Cold avg         : {summary['overall_cold']:.4f}s")
        print(f"  Warm avg         : {summary['overall_warm']:.4f}s")
        print(f"  Overall speedup  : {summary['overall_speedup']:.2f}x")
        print(f"  Improved nodes   : {summary['improved_count']}/{summary['count']}")
        print(f"  Signal           : {summary['signal']}")
        print("=" * 60)

    write_record(args, sampled_stops, window_size, results, errors, summary)
    print(f"\n[Results appended to {RECORD}]")


if __name__ == "__main__":
    main()
