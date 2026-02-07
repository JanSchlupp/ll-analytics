"""Simple in-memory TTL cache for API responses.

Data only changes when the scraper runs, so even short TTLs (5 min)
eliminate redundant computation for concurrent page loads and repeat visits.
"""

import time
from typing import Any


class ResponseCache:
    """Thread-safe in-memory cache with per-key TTL.

    Usage:
        cache = ResponseCache(default_ttl=300)

        # In an endpoint:
        key = f"dashboard:{season_id}:{rundle_id}"
        cached = cache.get(key)
        if cached is not None:
            return cached

        result = expensive_computation()
        cache.set(key, result)
        return result
    """

    def __init__(self, default_ttl: int = 300):
        self._store: dict[str, tuple[float, Any]] = {}
        self.default_ttl = default_ttl

    def get(self, key: str) -> Any | None:
        """Get a cached value if it exists and hasn't expired."""
        entry = self._store.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.monotonic() > expires_at:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Store a value with optional custom TTL."""
        expires_at = time.monotonic() + (ttl if ttl is not None else self.default_ttl)
        self._store[key] = (expires_at, value)

    def clear(self, prefix: str | None = None) -> int:
        """Clear all entries, or only those matching a prefix.

        Returns the number of entries cleared.
        """
        if prefix is None:
            count = len(self._store)
            self._store.clear()
            return count

        keys = [k for k in self._store if k.startswith(prefix)]
        for k in keys:
            del self._store[k]
        return len(keys)

    def cleanup(self) -> int:
        """Remove expired entries. Returns count removed."""
        now = time.monotonic()
        expired = [k for k, (exp, _) in self._store.items() if now > exp]
        for k in expired:
            del self._store[k]
        return len(expired)


# Singleton instance shared across all routes.
# 5 min default — long enough to help, short enough to stay fresh.
response_cache = ResponseCache(default_ttl=300)
