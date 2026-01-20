"""
Redis Cache Module

Production caching layer with:
- Connection pooling
- Automatic serialization
- TTL management
- Cache invalidation patterns
"""

import json
from typing import Any, Optional, Union
from datetime import timedelta

import structlog
from redis.asyncio import Redis, ConnectionPool
from redis.asyncio.client import Pipeline

from src.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()

# Global Redis connection
_redis_pool: Optional[ConnectionPool] = None
_redis_client: Optional[Redis] = None


async def init_redis() -> Redis:
    """Initialize Redis connection pool"""
    global _redis_pool, _redis_client
    
    if _redis_client is not None:
        return _redis_client
    
    _redis_pool = ConnectionPool.from_url(
        settings.redis.url,
        max_connections=settings.redis.max_connections,
        socket_timeout=settings.redis.socket_timeout,
        decode_responses=settings.redis.decode_responses,
    )
    
    _redis_client = Redis(connection_pool=_redis_pool)
    
    # Test connection
    try:
        await _redis_client.ping()
        logger.info("Redis connection established")
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        raise
    
    return _redis_client


async def close_redis() -> None:
    """Close Redis connection pool"""
    global _redis_pool, _redis_client
    
    if _redis_client:
        await _redis_client.close()
        _redis_client = None
    
    if _redis_pool:
        await _redis_pool.disconnect()
        _redis_pool = None
    
    logger.info("Redis connection closed")


def get_redis() -> Redis:
    """Get Redis client instance"""
    if _redis_client is None:
        raise RuntimeError("Redis not initialized. Call init_redis() first.")
    return _redis_client


async def cache_get(key: str) -> Optional[Any]:
    """
    Get value from cache.
    
    Args:
        key: Cache key
        
    Returns:
        Cached value or None if not found
    """
    client = get_redis()
    value = await client.get(key)
    
    if value is None:
        return None
    
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


async def cache_set(
    key: str,
    value: Any,
    ttl: Optional[Union[int, timedelta]] = None,
) -> bool:
    """
    Set value in cache.
    
    Args:
        key: Cache key
        value: Value to cache (will be JSON serialized)
        ttl: Time-to-live in seconds or timedelta
        
    Returns:
        True if successful
    """
    client = get_redis()
    
    try:
        serialized = json.dumps(value, default=str)
    except (TypeError, ValueError) as e:
        logger.warning(f"Failed to serialize value for cache: {e}")
        return False
    
    if ttl:
        if isinstance(ttl, timedelta):
            ttl = int(ttl.total_seconds())
        await client.setex(key, ttl, serialized)
    else:
        await client.set(key, serialized)
    
    return True


async def cache_delete(key: str) -> bool:
    """Delete key from cache"""
    client = get_redis()
    result = await client.delete(key)
    return result > 0


async def cache_delete_pattern(pattern: str) -> int:
    """Delete all keys matching pattern"""
    client = get_redis()
    keys = await client.keys(pattern)
    
    if not keys:
        return 0
    
    return await client.delete(*keys)


class CacheManager:
    """
    Cache manager with namespace support and automatic key generation.
    
    Example:
        cache = CacheManager("orders")
        await cache.set("123", order_data, ttl=3600)
        order = await cache.get("123")
    """
    
    def __init__(self, namespace: str, default_ttl: int = 3600):
        self.namespace = namespace
        self.default_ttl = default_ttl
    
    def _key(self, key: str) -> str:
        """Generate namespaced key"""
        return f"{self.namespace}:{key}"
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        return await cache_get(self._key(key))
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """Set value in cache"""
        return await cache_set(self._key(key), value, ttl or self.default_ttl)
    
    async def delete(self, key: str) -> bool:
        """Delete key from cache"""
        return await cache_delete(self._key(key))
    
    async def invalidate_all(self) -> int:
        """Invalidate all keys in namespace"""
        return await cache_delete_pattern(f"{self.namespace}:*")
    
    async def get_or_set(
        self,
        key: str,
        factory: callable,
        ttl: Optional[int] = None,
    ) -> Any:
        """
        Get from cache or compute and cache.
        
        Args:
            key: Cache key
            factory: Async function to compute value if not cached
            ttl: Time-to-live
            
        Returns:
            Cached or computed value
        """
        value = await self.get(key)
        
        if value is not None:
            return value
        
        value = await factory()
        await self.set(key, value, ttl)
        
        return value


# Pre-configured cache managers
orders_cache = CacheManager("orders", default_ttl=300)
products_cache = CacheManager("products", default_ttl=3600)
customers_cache = CacheManager("customers", default_ttl=1800)
analytics_cache = CacheManager("analytics", default_ttl=600)
