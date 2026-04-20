"""
In-memory cache with optional Redis backend.
Falls back to Python dict if Redis is not available.
"""
import time
import json
import logging

from backend.config import REDIS_URL, CACHE_TTL_SECONDS

logger = logging.getLogger(__name__)

_redis_client = None
_memory_cache: dict[str, tuple[float, any]] = {}


def _get_redis():
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    if REDIS_URL:
        try:
            import redis
            _redis_client = redis.from_url(REDIS_URL)
            _redis_client.ping()
            logger.info("Redis connected")
            return _redis_client
        except Exception as e:
            logger.warning(f"Redis not available, using in-memory cache: {e}")
    return None


def cache_get(key: str):
    """Get value from cache."""
    r = _get_redis()
    if r:
        val = r.get(key)
        return json.loads(val) if val else None
    else:
        if key in _memory_cache:
            expiry, val = _memory_cache[key]
            if time.time() < expiry:
                return val
            else:
                del _memory_cache[key]
        return None


def cache_set(key: str, value, ttl: int | None = None):
    """Set value in cache with TTL."""
    ttl = ttl or CACHE_TTL_SECONDS
    r = _get_redis()
    if r:
        r.setex(key, ttl, json.dumps(value, default=str))
    else:
        _memory_cache[key] = (time.time() + ttl, value)


def cache_delete(key: str):
    """Delete a cache key."""
    r = _get_redis()
    if r:
        r.delete(key)
    elif key in _memory_cache:
        del _memory_cache[key]


def cache_clear():
    """Clear all cache."""
    global _memory_cache
    r = _get_redis()
    if r:
        r.flushdb()
    _memory_cache.clear()
