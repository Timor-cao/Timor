#!/usr/bin/env python3
import argparse
import asyncio
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List, Optional

try:
    import aiohttp
except ImportError:  # pragma: no cover - runtime guard
    print(
        "Missing dependency 'aiohttp'. Install with: pip install aiohttp",
        file=sys.stderr,
    )
    sys.exit(1)


@dataclass
class Stats:
    sent: int = 0
    completed: int = 0
    success: int = 0
    failed: int = 0
    validation_failed: int = 0
    bytes_received: int = 0
    latency_sum: float = 0.0
    latency_min: Optional[float] = None
    latency_max: Optional[float] = None
    status_counts: Counter = field(default_factory=Counter)
    error_counts: Counter = field(default_factory=Counter)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    async def record_sent(self, count: int) -> None:
        async with self.lock:
            self.sent += count

    async def record_response(
        self,
        status: int,
        ok: bool,
        latency: float,
        body_size: int,
        validation_failed: bool,
    ) -> None:
        async with self.lock:
            self.completed += 1
            if ok:
                self.success += 1
            else:
                self.failed += 1
                if validation_failed:
                    self.validation_failed += 1
            self.status_counts[status] += 1
            self.bytes_received += body_size
            self.latency_sum += latency
            if self.latency_min is None or latency < self.latency_min:
                self.latency_min = latency
            if self.latency_max is None or latency > self.latency_max:
                self.latency_max = latency

    async def record_error(self, error_type: str, latency: Optional[float]) -> None:
        async with self.lock:
            self.completed += 1
            self.failed += 1
            self.error_counts[error_type] += 1
            if latency is not None:
                self.latency_sum += latency
                if self.latency_min is None or latency < self.latency_min:
                    self.latency_min = latency
                if self.latency_max is None or latency > self.latency_max:
                    self.latency_max = latency

    async def snapshot(self) -> dict:
        async with self.lock:
            return {
                "sent": self.sent,
                "completed": self.completed,
                "success": self.success,
                "failed": self.failed,
                "validation_failed": self.validation_failed,
                "bytes_received": self.bytes_received,
                "latency_sum": self.latency_sum,
                "latency_min": self.latency_min,
                "latency_max": self.latency_max,
                "status_counts": dict(self.status_counts),
                "error_counts": dict(self.error_counts),
            }


def parse_headers(header_list: List[str]) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for item in header_list:
        if ":" not in item:
            raise ValueError(f"Invalid header: {item!r}. Use 'Key: Value'.")
        key, value = item.split(":", 1)
        headers[key.strip()] = value.strip()
    return headers


async def request_worker(
    worker_id: int,
    queue: asyncio.Queue,
    session: aiohttp.ClientSession,
    stats: Stats,
    method: str,
    url: str,
    headers: Dict[str, str],
    data: Optional[str],
    expect_status: int,
    expect_text: Optional[str],
    max_read: int,
) -> None:
    loop = asyncio.get_running_loop()
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        start = loop.time()
        try:
            async with session.request(
                method=method,
                url=url,
                headers=headers,
                data=data,
            ) as response:
                body = await response.content.read(max_read)
                latency = loop.time() - start
                status_ok = response.status == expect_status
                text_ok = True
                if expect_text is not None:
                    text_ok = expect_text in body.decode(errors="ignore")
                ok = status_ok and text_ok
                await stats.record_response(
                    response.status,
                    ok=ok,
                    latency=latency,
                    body_size=len(body),
                    validation_failed=not ok,
                )
        except Exception as exc:  # noqa: BLE001 - load test needs catch-all
            latency = loop.time() - start
            await stats.record_error(type(exc).__name__, latency)
        finally:
            queue.task_done()


async def producer(
    queue: asyncio.Queue,
    stats: Stats,
    rate: int,
    duration: int,
) -> None:
    loop = asyncio.get_running_loop()
    start = loop.time()
    for second in range(duration):
        target = start + second + 1
        for _ in range(rate):
            queue.put_nowait(1)
        await stats.record_sent(rate)
        sleep_for = target - loop.time()
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)


def format_counter(counter: dict, limit: int = 6) -> str:
    if not counter:
        return "-"
    items = sorted(counter.items(), key=lambda kv: (-kv[1], str(kv[0])))
    parts = [f"{k}:{v}" for k, v in items[:limit]]
    if len(items) > limit:
        parts.append("...")
    return " ".join(parts)


async def reporter(stats: Stats, interval: float, stop_event: asyncio.Event, start_ts: float) -> None:
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        snapshot = await stats.snapshot()
        elapsed = time.monotonic() - start_ts
        completed = snapshot["completed"]
        rps = completed / elapsed if elapsed > 0 else 0.0
        avg_latency = snapshot["latency_sum"] / completed if completed else 0.0
        print(
            f"[{elapsed:6.1f}s] sent={snapshot['sent']} "
            f"done={completed} ok={snapshot['success']} "
            f"fail={snapshot['failed']} "
            f"rps={rps:0.1f} avg={avg_latency*1000:0.1f}ms",
            flush=True,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Async load generator: send 500 rps for 5 minutes."
    )
    parser.add_argument("--url", required=True, help="Target URL.")
    parser.add_argument("--method", default="GET", help="HTTP method.")
    parser.add_argument("--rate", type=int, default=500, help="Requests per second.")
    parser.add_argument("--duration", type=int, default=300, help="Duration in seconds.")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Max concurrent requests (default: rate*2).",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="Total timeout.")
    parser.add_argument(
        "--report-interval",
        type=float,
        default=1.0,
        help="Progress output interval in seconds.",
    )
    parser.add_argument(
        "--expect-status",
        type=int,
        default=200,
        help="Expected HTTP status for success.",
    )
    parser.add_argument(
        "--expect-text",
        default=None,
        help="Optional substring to validate in response.",
    )
    parser.add_argument(
        "--max-read",
        type=int,
        default=65536,
        help="Max bytes to read from response body.",
    )
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        help="HTTP header (repeatable). Format: 'Key: Value'.",
    )
    parser.add_argument(
        "--data",
        default=None,
        help="Optional request body (string).",
    )
    return parser


async def run(args: argparse.Namespace) -> int:
    if args.rate <= 0:
        raise ValueError("--rate must be > 0")
    if args.duration <= 0:
        raise ValueError("--duration must be > 0")
    if args.concurrency is None:
        args.concurrency = max(100, args.rate * 2)
    if args.concurrency <= 0:
        raise ValueError("--concurrency must be > 0")
    if args.report_interval <= 0:
        raise ValueError("--report-interval must be > 0")

    headers = parse_headers(args.header)
    stats = Stats()
    queue: asyncio.Queue = asyncio.Queue()
    stop_event = asyncio.Event()

    connector = aiohttp.TCPConnector(limit=args.concurrency)
    timeout = aiohttp.ClientTimeout(total=args.timeout)

    start_ts = time.monotonic()
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        workers = [
            asyncio.create_task(
                request_worker(
                    worker_id=i,
                    queue=queue,
                    session=session,
                    stats=stats,
                    method=args.method,
                    url=args.url,
                    headers=headers,
                    data=args.data,
                    expect_status=args.expect_status,
                    expect_text=args.expect_text,
                    max_read=args.max_read,
                )
            )
            for i in range(args.concurrency)
        ]
        reporter_task = asyncio.create_task(
            reporter(stats, args.report_interval, stop_event, start_ts)
        )

        await producer(queue, stats, args.rate, args.duration)
        await queue.join()

        for _ in range(args.concurrency):
            queue.put_nowait(None)
        await asyncio.gather(*workers)
        stop_event.set()
        await reporter_task

    snapshot = await stats.snapshot()
    elapsed = time.monotonic() - start_ts
    avg_latency = (
        snapshot["latency_sum"] / snapshot["completed"]
        if snapshot["completed"]
        else 0.0
    )
    print("\n=== Summary ===")
    print(f"elapsed: {elapsed:0.2f}s")
    print(f"sent: {snapshot['sent']}")
    print(f"completed: {snapshot['completed']}")
    print(f"success: {snapshot['success']}")
    print(f"failed: {snapshot['failed']}")
    print(f"validation_failed: {snapshot['validation_failed']}")
    print(f"avg_latency: {avg_latency*1000:0.2f}ms")
    if snapshot["latency_min"] is not None:
        print(f"min_latency: {snapshot['latency_min']*1000:0.2f}ms")
    if snapshot["latency_max"] is not None:
        print(f"max_latency: {snapshot['latency_max']*1000:0.2f}ms")
    print(f"bytes_received: {snapshot['bytes_received']}")
    print(f"status_counts: {format_counter(snapshot['status_counts'])}")
    print(f"error_counts: {format_counter(snapshot['error_counts'])}")
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130
    except Exception as exc:  # noqa: BLE001 - CLI guard
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
