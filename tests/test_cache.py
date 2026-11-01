import asyncio
import os
import uuid
import pytest
from wred import Request
from wred_cache import Cache, MemoryStore, RedisStore


def request(headers=(), path="/"):
    return Request(
        {"path": path, "method": "GET", "headers": headers, "query_string": b""},
        None,
        None,
    )


@pytest.fixture(params=["memory", "redis"])
async def cache(request):
    if request.param == "redis":
        url = os.getenv("WRED_TEST_REDIS")
        if not url:
            pytest.skip("Set WRED_TEST_REDIS")
        store = RedisStore(url)
    else:
        store = MemoryStore()
    cache = Cache(store=store, namespace=uuid.uuid4().hex)
    yield cache
    await cache.aclose()


async def test_stampede_mutation_expiry_and_invalidation(cache):
    calls = 0

    @cache.cache(ttl=2, tags=["orders"], key=lambda r: "orders")
    async def handler(request):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return (
            {"items": [calls]},
            [("content-type", "application/json"), ("x-test", "yes")],
            200,
        )

    results = await asyncio.gather(*(handler(request()) for _ in range(10)))
    assert calls == 1
    results[0][0]["items"].append("changed")
    assert (await handler(request()))[0] == {"items": [1]}
    await cache.invalidate(tag="orders")
    assert (await handler(request()))[0] == {"items": [2]}
    await cache.invalidate(key="orders")
    assert (await handler(request()))[0] == {"items": [3]}
    await cache.invalidate()
    assert (await handler(request()))[0] == {"items": [4]}
    await asyncio.sleep(2.05)
    assert (await handler(request()))[0] == {"items": [5]}


async def test_private_keys_and_public_bypass(cache):
    calls = 0

    @cache.cache()
    async def public(request):
        nonlocal calls
        calls += 1
        return {"calls": calls}

    r = request([(b"authorization", b"Bearer alice")])
    assert (await public(r)) != (await public(r))

    @cache.cache(private=True, key=lambda r: r.headers["authorization"])
    async def private(request):
        return {"user": request.headers["authorization"]}

    assert (await private(r))["user"] == "Bearer alice"
    assert (await private(request([(b"authorization", b"Bearer bob")])))[
        "user"
    ] == "Bearer bob"
    assert (await private(r))["user"] == "Bearer alice"


async def test_exclusions_and_handler_not_retried(cache):
    calls = 0

    @cache.cache()
    async def failing(request):
        nonlocal calls
        calls += 1
        raise RuntimeError("application error")

    with pytest.raises(RuntimeError):
        await failing(request())
    assert calls == 1

    @cache.cache()
    async def cookie(request):
        nonlocal calls
        calls += 1
        return b"hello", [("set-cookie", "a=b")], 200

    await cookie(request())
    await cookie(request())
    assert calls == 3

    async def stream(scope, receive, send):
        pass

    @cache.cache()
    async def streaming(request):
        return stream

    assert await streaming(request()) is stream


async def test_outage_bypasses_cache():
    cache = Cache(store=RedisStore("redis://127.0.0.1:1"))

    @cache.cache()
    async def handler(request):
        return {"ok": True}

    assert await handler(request()) == {"ok": True}
    await cache.aclose()
