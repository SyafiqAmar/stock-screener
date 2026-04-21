"""
Asynchronous Redis caching layer.
Defaults to local in-memory dict if Redis is not available.
"""
import time
import json
import logging
import asyncio
from typing import Any, Optional

from backend.config import REDIS_URL, CACHE_TTL_SECONDS

logger = logging.getLogger(__name__)

_redis_client = None
_memory_cache: dict[str, tuple[float, any]] = {}

async def _get_redis():
    """Returns an async Redis client."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    
    if REDIS_URL:
        try:
            from redis import asyncio as aioredis
            _redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
            # Test connection
            await _redis_client.ping()
            logger.info("Async Redis connected successfully.")
            return _redis_client
        except Exception as e:
            logger.warning(f"Async Redis not available, using in-memory fallback: {e}")
            _redis_client = False # Mark as failed
    return None

async def cache_get(key: str) -> Any:
    """Get value from cache (Async)."""
    r = await _get_redis()
    if r:
        try:
            val = await r.get(key)
            return json.loads(val) if val else None
        except Exception:
            return None
    else:
        if key in _memory_cache:
            expiry, val = _memory_cache[key]
            if time.time() < expiry:
                return val
            else:
                del _memory_cache[key]
        return None

async def cache_set(key: str, value: Any, ttl: Optional[int] = None):
    """Set value in cache with TTL (Async)."""
    ttl = ttl or CACHE_TTL_SECONDS
    r = await _get_redis()
    if r:
        try:
            await r.setex(key, ttl, json.dumps(value, default=str))
        except Exception as e:
            logger.error(f"Redis set error: {e}")
    else:
        _memory_cache[key] = (time.time() + ttl, value)

async def cache_delete(key: str):
    """Delete a cache key (Async)."""
    r = await _get_redis()
    if r:
        try:
            await r.delete(key)
        except Exception:
            pass
    elif key in _memory_cache:
        del _memory_cache[key]

async def cache_clear():
    """Clear all cache (Async)."""
    global _memory_cache
    r = await _get_redis()
    if r:
        try:
            await r.flushdb()
        except Exception:
            pass
    _memory_cache.clear()
