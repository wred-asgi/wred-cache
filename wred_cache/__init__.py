"""Opt-in response caching with isolated keys and shared invalidation."""

import asyncio
import base64
import hashlib
import inspect
import time
import uuid
from contextlib import asynccontextmanager
from functools import wraps

import orjson

__version__ = "0.1.0"


class MemoryStore:
    def __init__(self, *, maximum=1024):
        if maximum < 1:
            raise ValueError("maximum must be positive")
        self.maximum, self.values, self.versions, self.locks = maximum, {}, {}, {}

    async def get(self, key):
        item = self.values.get(key)
        if item and item[0] > time.monotonic():
            return item[1]
        self.values.pop(key, None)

    async def set(self, key, value, ttl):
        if len(self.values) >= self.maximum:
            self.values.pop(next(iter(self.values)))
        self.values[key] = time.monotonic() + ttl, value

    async def version(self, key):
        return self.versions.get(key, "")

    async def bump(self, key):
        self.versions[key] = uuid.uuid4().hex

    @asynccontextmanager
    async def lock(self, key):
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
    def __init__(self, url, *, lease=30, wait=5):
        from redis.asyncio import Redis

        self.client = Redis.from_url(url)
        self.lease, self.wait = lease, wait

    async def get(self, key):
        return await self.client.get(key)

    async def set(self, key, value, ttl):
        await self.client.psetex(key, max(1, int(ttl * 1000)), value)

    async def version(self, key):
        return (await self.client.get(key) or b"").decode()

    async def bump(self, key):
        await self.client.set(key, uuid.uuid4().hex)

    @asynccontextmanager
    async def lock(self, key):
        from redis.exceptions import LockError

        lock = self.client.lock(
            key + ":lock",
            timeout=self.lease,
            blocking_timeout=self.wait,
            thread_local=False,
        )
        if not await lock.acquire():
            raise TimeoutError("Cache fill lock timed out")
        try:
            yield
        finally:
            try:
                await lock.release()
            except LockError:
                pass

    async def aclose(self):
        await self.client.aclose()


def _encode(result):
    if callable(result):
        return None
    is_tuple = isinstance(result, tuple)
    body, headers, status = result if is_tuple else (result, [], 200)
    if not 200 <= status < 300:
        return None
    for name, value in headers:
        if name.lower() == "set-cookie":
            return None
        if name.lower() == "cache-control" and any(
            x in value.lower() for x in ("private", "no-store")
        ):
            return None
    binary = isinstance(body, bytes)
    try:
        return orjson.dumps(
            {
                "body": base64.b64encode(body).decode() if binary else body,
                "binary": binary,
                "tuple": is_tuple,
                "headers": headers,
                "status": status,
            }
        )
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
    def __init__(self, *, store=None, namespace="wred"):
        self.store = store if store is not None else MemoryStore()
        self.prefix = "wred-cache:" + hashlib.sha256(namespace.encode()).hexdigest()

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
        await self.store.bump(self._version_key(kind, value))

    def cache(self, ttl=60, *, key=None, tags=(), private=False):
        if ttl <= 0 or (private and key is None):
            raise ValueError(
                "Positive TTL and an explicit user-specific key for private caching are required"
            )
        tags = tuple(tags)

        def decorate(handler):
            route = f"{handler.__module__}.{handler.__qualname__}"

            @wraps(handler)
            async def wrapped(request, *args, **kwargs):
                if (
                    request.method != "GET"
                    or "no-store" in request.headers.get("cache-control", "").lower()
                ):
                    return await handler(request, *args, **kwargs)
                credentials = any(
                    h in request.headers
                    for h in ("authorization", "cookie", "x-api-key")
                )
                if not private and (credentials or request.scope.get("wred.auth")):
                    return await handler(request, *args, **kwargs)
                explicit = key(request) if key else None
                if inspect.isawaitable(explicit):
                    explicit = await explicit
                if private and explicit is None:
                    return await handler(request, *args, **kwargs)
                try:
                    raw = (
                        str(explicit)
                        if key
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
                except Exception:
                    return await handler(request, *args, **kwargs)
                # Never catch a handler exception as a cache outage or execute a handler twice.
                invoked = False
                completed = False
                result = None
                try:
                    async with self.store.lock(cache_key):
                        hit = await self.store.get(cache_key)
                        if hit is not None:
                            return _decode(hit)
                        invoked = True
                        result = await handler(request, *args, **kwargs)
                        completed = True
                        payload = _encode(result)
                        if payload is not None:
                            try:
                                await self.store.set(cache_key, payload, ttl)
                            except Exception:
                                pass
                        return result
                except Exception:
                    if completed:
                        return result
                    if invoked:
                        raise
                    return await handler(request, *args, **kwargs)

            return wrapped

        return decorate

    async def aclose(self):
        await self.store.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.aclose()
