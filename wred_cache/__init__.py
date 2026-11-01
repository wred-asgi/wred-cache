"""Opt-in response caching with isolated keys and shared invalidation."""

import asyncio
import base64
import hashlib
import inspect
import logging
import math
import time
import uuid
from collections import OrderedDict
from contextlib import AsyncExitStack, asynccontextmanager
from functools import wraps

import orjson

__version__ = "0.2.0"
logger = logging.getLogger(__name__)


def _positive(value, name):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{name} must be positive and finite")


def _capacity(value, name):
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _unavailable(operation, exc):
    logger.warning(
        "Cache backend unavailable; bypassing cache",
        extra={"operation": operation, "error_type": type(exc).__name__},
    )


class MemoryStore:
    def __init__(
        self,
        *,
        maximum=1024,
        maximum_bytes=64 * 1024 * 1024,
        maximum_versions=None,
        maximum_locks=1024,
    ):
        maximum_versions = maximum * 3 if maximum_versions is None else maximum_versions
        for name, value in (
            ("maximum", maximum),
            ("maximum_bytes", maximum_bytes),
            ("maximum_versions", maximum_versions),
            ("maximum_locks", maximum_locks),
        ):
            _capacity(value, name)
        self.maximum, self.maximum_bytes = maximum, maximum_bytes
        self.maximum_versions, self.maximum_locks = maximum_versions, maximum_locks
        self.values, self.versions, self.locks = OrderedDict(), OrderedDict(), {}
        self._bytes = 0

    def _remove(self, key):
        item = self.values.pop(key, None)
        if item is not None:
            self._bytes -= len(item[1])

    async def get(self, key):
        item = self.values.get(key)
        if item and item[0] > time.monotonic():
            self.values.move_to_end(key)
            return item[1]
        self._remove(key)

    async def set(self, key, value, ttl):
        _positive(ttl, "ttl")
        self._remove(key)
        if len(value) > self.maximum_bytes:
            return
        while self.values and (
            len(self.values) >= self.maximum
            or self._bytes + len(value) > self.maximum_bytes
        ):
            self._remove(next(iter(self.values)))
        self.values[key] = time.monotonic() + ttl, value
        self._bytes += len(value)

    async def version(self, key):
        if key not in self.versions:
            await self.bump(key)
        self.versions.move_to_end(key)
        return self.versions[key]

    async def bump(self, key):
        if key not in self.versions and len(self.versions) >= self.maximum_versions:
            self.versions.popitem(last=False)
        self.versions[key] = uuid.uuid4().hex
        self.versions.move_to_end(key)

    @asynccontextmanager
    async def lock(self, key):
        if key not in self.locks and len(self.locks) >= self.maximum_locks:
            raise TimeoutError("Cache fill capacity exhausted")
        lock, users = self.locks.setdefault(key, (asyncio.Lock(), 0))
        self.locks[key] = lock, users + 1
        try:
            async with lock:
                yield
        finally:
            _, users = self.locks[key]
            if users == 1:
                self.locks.pop(key)
            else:
                self.locks[key] = lock, users - 1

    async def aclose(self):
        pass


class RedisStore:
    def __init__(
        self, url, *, lease=30, wait=0.5, version_ttl=300, connection_options=None
    ):
        from redis.asyncio import Redis
        from redis.asyncio.retry import Retry
        from redis.backoff import NoBackoff

        for name, value in (
            ("lease", lease),
            ("wait", wait),
            ("version_ttl", version_ttl),
        ):
            _positive(value, name)
        options = dict(
            socket_connect_timeout=0.2,
            socket_timeout=0.2,
            max_connections=100,
            retry=Retry(NoBackoff(), 0),
        )
        options.update(connection_options or {})
        self.client = Redis.from_url(url, **options)
        self.lease, self.wait = lease, wait
        self.version_ttl = max(1, int(version_ttl * 1000))

    async def get(self, key):
        return await self.client.get(key)

    async def set(self, key, value, ttl):
        await self.client.psetex(key, max(1, int(ttl * 1000)), value)

    async def version(self, key):
        # A missing/evicted version must never reuse a prior generation. Metadata
        # expires independently; expiration can cause a miss, never resurrection.
        value = await self.client.eval(
            "local v = redis.call('GET', KEYS[1]); "
            "if not v then v = ARGV[1]; redis.call('SET', KEYS[1], v, 'PX', ARGV[2]); "
            "elseif redis.call('PTTL', KEYS[1]) < 0 then redis.call('PEXPIRE', KEYS[1], ARGV[2]); end; return v",
            1,
            key,
            uuid.uuid4().hex,
            self.version_ttl,
        )
        return value.decode()

    async def bump(self, key):
        await self.client.psetex(key, self.version_ttl, uuid.uuid4().hex)

    async def set_locked(self, key, value, ttl, token):
        """Only the current fill lease may publish a response."""
        await self.client.eval(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "return redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3]); end; return 0",
            2,
            key + ":lock",
            key,
            token,
            value,
            max(1, int(ttl * 1000)),
        )

    @asynccontextmanager
    async def lock(self, key):
        from redis.exceptions import LockError

        lock = self.client.lock(
            key + ":lock",
            timeout=self.lease,
            blocking_timeout=self.wait,
            sleep=0.01,
            thread_local=False,
        )
        token = uuid.uuid4().hex
        if not await lock.acquire(token=token):
            raise TimeoutError("Cache fill lock timed out")
        try:
            yield token
        finally:
            try:
                await lock.release()
            except LockError:
                pass

    async def aclose(self):
        await self.client.aclose()


def _encode(result, maximum):
    if callable(result):
        return None
    is_tuple = isinstance(result, tuple)
    body, headers, status = result if is_tuple else (result, [], 200)
    if not 200 <= status < 300:
        return None
    for name, value in headers:
        if name.lower() == "set-cookie":
            return None
        if name.lower() == "vary" and "*" in (
            part.strip() for part in value.split(",")
        ):
            return None
        if name.lower() == "cache-control" and any(
            x in value.lower() for x in ("private", "no-store", "no-cache")
        ):
            return None
    binary = isinstance(body, bytes)
    if isinstance(body, (bytes, str)) and len(body) > maximum:
        return None
    try:
        encoded = orjson.dumps(
            {
                "body": base64.b64encode(body).decode() if binary else body,
                "binary": binary,
                "tuple": is_tuple,
                "headers": headers,
                "status": status,
            }
        )
        return encoded if len(encoded) <= maximum else None
    except TypeError:
        return None


def _decode(value):
    data = orjson.loads(value)
    body = base64.b64decode(data["body"]) if data["binary"] else data["body"]
    return (
        (body, [tuple(h) for h in data["headers"]], data["status"])
        if data["tuple"]
        else body
    )


class Cache:
    def __init__(
        self, *, store=None, namespace="wred", timeout=0.5, max_entry_bytes=1024 * 1024
    ):
        _positive(timeout, "timeout")
        _capacity(max_entry_bytes, "max_entry_bytes")
        if not isinstance(namespace, str) or not namespace:
            raise ValueError("namespace must be a nonempty string")
        self.store = store if store is not None else MemoryStore()
        self.prefix = "wred-cache:" + hashlib.sha256(namespace.encode()).hexdigest()
        self.timeout, self.max_entry_bytes = timeout, max_entry_bytes

    @asynccontextmanager
    async def _lock(self, key):
        stack = AsyncExitStack()
        async with asyncio.timeout(self.timeout):
            lease = await stack.enter_async_context(self.store.lock(key))
        try:
            yield lease
        except BaseException:
            try:
                async with asyncio.timeout(self.timeout):
                    await stack.aclose()
            except Exception as exc:
                _unavailable("unlock", exc)
            raise
        else:
            async with asyncio.timeout(self.timeout):
                await stack.aclose()

    def _version_key(self, kind, value=""):
        return (
            self.prefix + ":" + kind + ":" + hashlib.sha256(value.encode()).hexdigest()
        )

    async def invalidate(self, *, tag=None, key=None):
        """Invalidate a tag, an explicit key, or this entire namespace."""
        if tag is not None and key is not None:
            raise ValueError("Choose tag or key")
        kind, value = (
            ("tag", tag)
            if tag is not None
            else ("key", str(key))
            if key is not None
            else ("all", "")
        )
        async with asyncio.timeout(self.timeout):
            await self.store.bump(self._version_key(kind, value))

    def cache(self, ttl=60, *, key=None, tags=(), private=False):
        _positive(ttl, "ttl")
        if private and key is None:
            raise ValueError(
                "Positive TTL and an explicit user-specific key for private caching are required"
            )
        tags = tuple(tags)
        if key is not None and not callable(key):
            raise TypeError("key must be callable")
        if not all(isinstance(tag, str) and tag for tag in tags):
            raise ValueError("tags must be nonempty strings")

        def decorate(handler):
            route = f"{handler.__module__}.{handler.__qualname__}"

            @wraps(handler)
            async def wrapped(request, *args, **kwargs):
                if request.method != "GET" or any(
                    value in request.headers.get("cache-control", "").lower()
                    for value in ("no-store", "no-cache")
                ):
                    return await handler(request, *args, **kwargs)
                credentials = any(
                    h in request.headers
                    for h in ("authorization", "cookie", "x-api-key")
                )
                if not private and (credentials or request.scope.get("wred.auth")):
                    return await handler(request, *args, **kwargs)
                explicit = key(request) if key is not None else None
                if inspect.isawaitable(explicit):
                    explicit = await explicit
                if key is not None and explicit is None:
                    return await handler(request, *args, **kwargs)
                try:
                    raw = (
                        str(explicit)
                        if key is not None
                        else orjson.dumps(
                            [
                                request.path,
                                base64.b64encode(
                                    request.scope.get("query_string", b"")
                                ).decode(),
                                sorted(request.headers.items()),
                                args,
                                kwargs,
                            ]
                        ).decode()
                    )
                except (TypeError, ValueError):
                    return await handler(request, *args, **kwargs)
                try:
                    async with asyncio.timeout(self.timeout):
                        versions = [
                            await self.store.version(self._version_key("all")),
                            await self.store.version(self._version_key("key", raw)),
                        ]
                        versions += [
                            await self.store.version(self._version_key("tag", tag))
                            for tag in tags
                        ]
                        cache_key = (
                            self.prefix
                            + ":"
                            + hashlib.sha256(
                                orjson.dumps([route, raw, versions])
                            ).hexdigest()
                        )
                        hit = await self.store.get(cache_key)
                        if hit is not None:
                            return _decode(hit)
                except Exception as exc:
                    _unavailable("lookup", exc)
                    return await handler(request, *args, **kwargs)
                # Never catch a handler exception as a cache outage or execute a handler twice.
                invoked = False
                completed = False
                result = None
                try:
                    async with self._lock(cache_key) as lease:
                        async with asyncio.timeout(self.timeout):
                            hit = await self.store.get(cache_key)
                        if hit is not None:
                            return _decode(hit)
                        invoked = True
                        result = await handler(request, *args, **kwargs)
                        completed = True
                        payload = _encode(result, self.max_entry_bytes)
                        if payload is not None:
                            try:
                                async with asyncio.timeout(self.timeout):
                                    writer = getattr(self.store, "set_locked", None)
                                    if writer is not None:
                                        await writer(cache_key, payload, ttl, lease)
                                    else:
                                        await self.store.set(cache_key, payload, ttl)
                            except Exception as exc:
                                _unavailable("store", exc)
                        return result
                except Exception as exc:
                    if completed:
                        _unavailable("unlock", exc)
                        return result
                    if invoked:
                        raise
                    _unavailable("fill", exc)
                    return await handler(request, *args, **kwargs)

            return wrapped

        return decorate

    async def aclose(self):
        await self.store.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()
