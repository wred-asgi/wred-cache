"""Cross-process cache coherence against a disposable standalone Redis instance.

REDIS_URL is required. Two phases bracket an acknowledged namespace invalidation.
This measures handler/storage calls, not HTTP capacity. Entries expire after 60s;
metadata expires after 300s. The unique fill counter is deleted at the end.
"""

import argparse
import asyncio
import json
import multiprocessing
import os
import time
import uuid
from concurrent.futures import ProcessPoolExecutor

from wred import Request
from wred_cache import Cache, RedisStore


def worker(options, namespace, revision, count):
    async def run():
        store = RedisStore(os.environ["REDIS_URL"])
        samples, errors = [], 0
        indices = iter(range(count))
        async with Cache(store=store, namespace=namespace) as cache:

            @cache.cache(key=lambda r: r.path)
            async def handler(request):
                await store.client.incr(namespace + ":fills")
                return {"revision": revision}

            async def lane():
                nonlocal errors
                for index in indices:
                    request = Request(
                        {"path": f"/{index % 10}", "method": "GET"}, None, None
                    )
                    started = time.perf_counter()
                    try:
                        result = await handler(request)
                        errors += result != {"revision": revision}
                    except Exception:
                        errors += 1
                    samples.append((time.perf_counter() - started) * 1000)

            started = time.perf_counter()
            await asyncio.gather(*(lane() for _ in range(options["concurrency"])))
            return samples, errors, started, time.perf_counter()

    return asyncio.run(run())


async def metadata(namespace, *, invalidate=False):
    async with Cache(
        store=RedisStore(os.environ["REDIS_URL"]), namespace=namespace
    ) as cache:
        if invalidate:
            await cache.invalidate()
            return None
        fills = int(await cache.store.client.get(namespace + ":fills") or 0)
        await cache.store.client.delete(namespace + ":fills")
        return fills


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=25)
    parser.add_argument("--requests", type=int, default=10000, help="Per phase")
    options = vars(parser.parse_args())
    if min(options.values()) < 1:
        parser.error("Counts must be positive")
    namespace = "wred-cache-benchmark-" + uuid.uuid4().hex
    reports = []
    with ProcessPoolExecutor(
        max_workers=options["workers"], mp_context=multiprocessing.get_context("spawn")
    ) as pool:
        for revision in (1, 2):
            if revision == 2:
                asyncio.run(metadata(namespace, invalidate=True))
            count, remainder = divmod(options["requests"], options["workers"])
            futures = [
                pool.submit(
                    worker, options, namespace, revision, count + (index < remainder)
                )
                for index in range(options["workers"])
            ]
            results = [future.result() for future in futures]
            samples = sorted(sample for values, _, _, _ in results for sample in values)
            elapsed = max(row[3] for row in results) - min(row[2] for row in results)
            reports.append(
                {
                    "revision": revision,
                    "requests": len(samples),
                    "errors": sum(row[1] for row in results),
                    "seconds": round(elapsed, 3),
                    "requests_per_second": round(len(samples) / elapsed, 1),
                    "p95_ms": round(
                        samples[min(len(samples) - 1, int(len(samples) * 0.95))], 3
                    ),
                }
            )
    print(
        json.dumps(
            {**options, "phases": reports, "fills": asyncio.run(metadata(namespace))},
            indent=2,
        )
    )
    if any(report["errors"] for report in reports):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
