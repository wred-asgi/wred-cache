# wred-cache

Opt-in GET response caching for Wred 0.13–0.14. Python 3.11+.

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

`Cache(store=None, namespace='wred')` defaults to `MemoryStore(maximum=1024)`.
`RedisStore(url, lease=30, wait=5)` shares values, locks, and invalidation versions.
Custom stores implement async `get`, `set`, `version`, `bump`, `aclose`, and an async
`lock` context manager, matching the included implementations.

`cache(ttl=60, key=None, tags=(), private=False)` caches only successful GET results.
Default keys include handler identity, path, raw query string, request headers, and
route kwargs. Tags must be stable strings. Explicit keys are sync or async callbacks
accepting the request and must include all response-varying context.

Public caching bypasses requests with Authorization, Cookie, X-API-Key, or a Wred
principal. For authenticated caching, place authentication outside caching and use
`private=True` with an explicit key containing user/tenant identity, filters, and any
other relevant variation. Applications using custom credential headers must explicitly
configure private caching or avoid applying caching to those endpoints.

`Set-Cookie`, private/no-store response directives, non-2xx responses, and callable
ASGI responses are never cached. Bytes and Wred response tuples preserve their type,
status, and headers. JSON-compatible values are deserialized per hit to prevent
mutable-object sharing. Unsupported values bypass storage.

`invalidate(tag='products')`, `invalidate(key='explicit-key')`, or `invalidate()`
updates shared versions. In-flight fills of old versions cannot become current.
Old values expire naturally. Invalidation failures propagate so callers can retry.
Request-path cache failures bypass caching, and handler errors are never retried.
Redis fill locks expire after `lease` seconds; configure this above maximum expected
handler duration. Fills longer than the lease can duplicate work. A timed-out lock
wait bypasses caching. This is not a distributed exactly-once execution facility.

Use `async with cache` or `await cache.aclose()`. Version keys persist; use a bounded
set of tags/explicit invalidation keys or retire an entire application namespace.

Tests: `pip install -e '.[test,redis]'`, then `pytest`. Set `WRED_TEST_REDIS` to run
Redis concurrency and invalidation checks.

## Extension compatibility

Follows the [Wred extension contract v1](https://github.com/wred-asgi/.github/blob/main/EXTENSIONS.md).
Keep `@app.route` outermost for
route decorators; register routes on the inner Wred app when using ASGI wrappers.

Default keys include forwarded arguments. Nonserializable injected values bypass
caching; place caching outside validation/pagination to cache those handlers.
Authentication and any quota that must apply to hits belong outside caching.
