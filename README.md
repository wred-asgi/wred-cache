# wred-cache

Opt-in GET response caching for Wred 0.13–0.15. Python 3.11+.

```sh
pip install './extensions/wred-cache[redis]'
```

```python
from wred import App
from wred_cache import Cache, RedisStore

app = App()
cache = Cache(store=RedisStore("redis://localhost:6379/0"), namespace="catalog")


@app.route("/products")
@cache.cache(ttl=60, tags=["products"])
async def products(request):
    return {"items": ["example"]}


# After changing the underlying data:
# await cache.invalidate(tag='products')
```

`Cache(store=None, namespace='wred', timeout=0.5, max_entry_bytes=1024*1024)`
defaults to `MemoryStore(maximum=1024, maximum_bytes=64*1024*1024,
maximum_versions=3072, maximum_locks=1024)`. Values use LRU eviction and a retained
encoded-byte budget (Python object overhead and temporary serialization are extra).
Oversized entries bypass storage. Fill capacity exhaustion bypasses caching.

`RedisStore(url, lease=30, wait=0.5, version_ttl=300, connection_options=None)`
shares values, locks, and invalidation versions. Defaults are 100 connections,
0.2-second connect/socket timeouts, and no command retries. Cache `timeout` bounds
each lookup, lock acquisition, write, release, and invalidation operation; it does
not impose a handler deadline. Supply `connection_options` to tune redis-py;
Redis URL query options take precedence. Use standalone Redis (or a managed endpoint
with standalone semantics); Redis Cluster is not supported by this two-key script.
Set Redis `maxmemory` and an appropriate eviction policy for a bounded deployment.
Custom stores implement async `get`, `set`, `version`, `bump`, `aclose`, and an async
`lock` context manager, matching the included implementations. `version()` must
atomically create a unique generation on every missing key; never return a stable
empty value. Stores with expiring locks must implement `set_locked(key, value, ttl,
lease)` to publish only while the yielded opaque lease is still owned.

`cache(ttl=60, key=None, tags=(), private=False)` caches only successful GET results.
Default keys include handler identity, path, raw query string, request headers, and
route kwargs. Tags must be stable strings. Explicit keys are sync or async callbacks
accepting the request and must include all response-varying context.

Public caching bypasses requests with Authorization, Cookie, X-API-Key, or a Wred
principal. For authenticated caching, place authentication outside caching and use
`private=True` with an explicit key containing user/tenant identity, filters, and any
other relevant variation. Applications using custom credential headers must explicitly
configure private caching or avoid applying caching to those endpoints.

`Set-Cookie`, private/no-store/no-cache response directives, `Vary: *`, non-2xx responses, and callable
ASGI responses are never cached. Bytes and Wred response tuples preserve their type,
status, and headers. JSON-compatible values are deserialized per hit to prevent
mutable-object sharing. Unsupported values bypass storage.

`invalidate(tag='products')`, `invalidate(key='explicit-key')`, or `invalidate()`
updates shared versions. In-flight fills of old versions cannot become current.
Old values expire naturally. Invalidation failures propagate so callers can retry.
Request-path cache failures bypass caching, and handler errors are never retried.
Redis fill locks expire after `lease` seconds; configure this above maximum expected
handler duration. Fills longer than the lease can duplicate work, but cannot overwrite a newer fill. A timed-out lock
wait bypasses caching. This is not a distributed exactly-once execution facility.

Use `async with cache` or `await cache.aclose()`. Redis version keys expire after
`version_ttl`; memory versions use bounded LRU eviction. Recreated generations are
unique, so losing metadata causes misses rather than resurrecting stale responses.
Use bounded tags and retire namespaces across incompatible deployments. High key
cardinality can still consume Redis memory during the TTL; enforce Redis capacity.
Backend failures emit warnings with operation/error type, excluding keys and URLs.
Request `Cache-Control: no-cache` or `no-store` bypasses an otherwise warm cache.

Tests: `pip install -e '.[test,redis]'`, then `pytest`. Set `WRED_TEST_REDIS` to run
Redis concurrency and invalidation checks.

## Extension compatibility

Follows the [Wred extension contract v1.1](https://github.com/wred-asgi/.github/blob/main/EXTENSIONS.md).
Keep `@app.route` outermost for
route decorators; register routes on the inner Wred app when using ASGI wrappers.

Default keys include forwarded arguments. Nonserializable injected values bypass
caching; place caching outside validation/pagination to cache those handlers.
Authentication and any quota that must apply to hits belong outside caching.
