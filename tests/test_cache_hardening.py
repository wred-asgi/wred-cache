import asyncio
import os
import uuid
from contextlib import asynccontextmanager

import pytest
from wred_cache import Cache, MemoryStore, RedisStore

from wred import Request


def request(headers=()):
    return Request({"path": "/", "method": "GET", "headers": headers}, None, None)


@pytest.mark.parametrize("header", [("cache-control", "no-cache"), ("vary", "*")])
async def test_responses_requiring_revalidation_are_not_reused(header):
    calls = 0
    async with Cache() as cache:

        @cache.cache()
        async def handler(request):
            nonlocal calls
            calls += 1
            return {"revision": calls}, [header], 200

        assert (await handler(request()))[0] == {"revision": 1}
        assert (await handler(request()))[0] == {"revision": 2}


async def test_request_no_cache_bypasses_a_warm_cache():
    calls = 0
    async with Cache() as cache:

        @cache.cache(key=lambda r: "shared")
        async def handler(request):
            nonlocal calls
            calls += 1
            return {"revision": calls}

        assert await handler(request()) == {"revision": 1}
        assert await handler(request([(b"cache-control", b"no-cache")])) == {
            "revision": 2
        }


async def test_default_entry_limit_does_not_retain_large_responses():
    calls = 0
    async with Cache() as cache:

        @cache.cache()
        async def handler(request):
            nonlocal calls
            calls += 1
            return b"x" * (1024 * 1024 + 1)

        for _ in range(2):
            assert len(await handler(request())) == 1024 * 1024 + 1
        assert calls == 2


async def test_memory_version_eviction_never_reuses_an_old_generation():
    store = MemoryStore(maximum=2)
    original = await store.version("target")
    await store.bump("target")
    for index in range(100):
        await store.bump(str(index))
    # Capacity eviction must rotate the missing generation, not resurrect the
    # original cache keys by returning the same empty/default version again.
    assert await store.version("target") != original
    old = await store.version("oldest")
    for index in range(100):
        await store.version(str(index))
    assert await store.version("oldest") != old


async def test_memory_byte_budget_and_fill_capacity_are_enforced():
    store = MemoryStore(maximum=10, maximum_bytes=6, maximum_locks=1)
    await store.set("a", b"aaa", 60)
    await store.set("b", b"bbb", 60)
    assert await store.get("a") == b"aaa"  # Keep the recently read entry.
    await store.set("c", b"ccc", 60)
    assert await store.get("b") is None
    assert await store.get("a") == b"aaa"
    assert await store.get("c") == b"ccc"
    await store.set("oversized", b"1234567", 60)
    assert await store.get("oversized") is None
    async with store.lock("occupied"):
        with pytest.raises(TimeoutError):
            async with store.lock("other"):
                pytest.fail("Fill capacity was exceeded")
    async with store.lock("other"):
        assert await store.get("a") == b"aaa"


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_release_failure_preserves_the_handler_exception(failure):
    class FailedReleaseStore(MemoryStore):
        @asynccontextmanager
        async def lock(self, key):
            try:
                yield
            finally:
                raise OSError("backend disconnected")

    error = failure("handler failed")
    calls = 0
    async with Cache(store=FailedReleaseStore()) as cache:

        @cache.cache()
        async def handler(request):
            nonlocal calls
            calls += 1
            raise error

        with pytest.raises(failure) as caught:
            await handler(request())
        assert caught.value is error
        assert calls == 1


async def test_slow_backend_has_a_default_deadline_and_calls_handler_once():
    class StalledStore(MemoryStore):
        async def get(self, key):
            await asyncio.Event().wait()

    calls = 0
    async with Cache(store=StalledStore()) as cache:

        @cache.cache()
        async def handler(request):
            nonlocal calls
            calls += 1
            return {"ok": True}

        assert await asyncio.wait_for(handler(request()), 2) == {"ok": True}
        assert calls == 1


@pytest.fixture
async def redis_store():
    url = os.getenv("WRED_TEST_REDIS")
    if not url:
        pytest.skip("Set WRED_TEST_REDIS")
    store = RedisStore(url, lease=0.05, wait=0.5)
    try:
        yield store
    finally:
        await store.aclose()


async def test_redis_metadata_eviction_does_not_resurrect_old_cache_keys(redis_store):
    key = "wred-cache-test-version:" + uuid.uuid4().hex
    first = await redis_store.version(key)
    await redis_store.bump(key)
    await redis_store.client.delete(key)  # Redis eviction/restart loses metadata.
    assert await redis_store.version(key) != first
    await redis_store.client.delete(key)


async def test_expired_fill_lease_cannot_overwrite_a_newer_result(redis_store):
    cache = Cache(store=redis_store, namespace=uuid.uuid4().hex)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    @cache.cache()
    async def handler(request):
        nonlocal calls
        calls += 1
        revision = calls
        if revision == 1:
            entered.set()
            await release.wait()
        return {"revision": revision}

    pending = asyncio.create_task(handler(request()))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0.1)
        assert await asyncio.wait_for(handler(request()), 1) == {"revision": 2}
        release.set()
        assert await asyncio.wait_for(pending, 1) == {"revision": 1}
        assert await handler(request()) == {"revision": 2}
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)


async def test_cancellation_releases_fill_lock_without_retrying_handler():
    entered = asyncio.Event()
    calls = 0
    async with Cache() as cache:

        @cache.cache()
        async def handler(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                await asyncio.Event().wait()
            return {"revision": calls}

        pending = asyncio.create_task(handler(request()))
        await asyncio.wait_for(entered.wait(), 1)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert await asyncio.wait_for(handler(request()), 1) == {"revision": 2}
        assert await handler(request()) == {"revision": 2}


@pytest.mark.parametrize("ttl", [float("nan"), float("inf"), True])
def test_invalid_ttl_is_rejected_before_serving_requests(ttl):
    with pytest.raises(ValueError):
        Cache().cache(ttl=ttl)
