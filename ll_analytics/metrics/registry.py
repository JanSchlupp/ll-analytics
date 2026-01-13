"""Metric registry for auto-discovery and management."""

from typing import Type
import sqlite3
import json
from datetime import datetime, timedelta

from .base import BaseMetric, Scope, MetricResult, MetricInfo


class MetricRegistry:
    """
    Central registry for all available metrics.

    Metrics are auto-registered using the @metric decorator.
    The registry provides discovery, lookup, and execution with caching.
    """

    _metrics: dict[str, BaseMetric] = {}

    @classmethod
    def register(cls, metric_instance: BaseMetric) -> None:
        """Register a metric instance."""
        if not metric_instance.id:
            raise ValueError(f"Metric {metric_instance.__class__.__name__} has no id")
        cls._metrics[metric_instance.id] = metric_instance

    @classmethod
    def get(cls, metric_id: str) -> BaseMetric | None:
        """Get a metric by ID."""
        return cls._metrics.get(metric_id)

    @classmethod
    def all(cls) -> list[BaseMetric]:
        """Get all registered metrics."""
        return list(cls._metrics.values())

    @classmethod
    def all_info(cls) -> list[MetricInfo]:
        """Get metadata for all registered metrics."""
        return [m.get_info() for m in cls._metrics.values()]

    @classmethod
    def by_scope(cls, scope: Scope) -> list[BaseMetric]:
        """Get all metrics that support a given scope."""
        return [m for m in cls._metrics.values() if scope in m.scopes]

    @classmethod
    def calculate(
        cls,
        conn: sqlite3.Connection,
        metric_id: str,
        scope: Scope,
        use_cache: bool = True,
        **kwargs
    ) -> MetricResult:
        """
        Calculate a metric with optional caching.

        Args:
            conn: Database connection
            metric_id: The metric to calculate
            scope: The scope of calculation
            use_cache: Whether to use cached results if available
            **kwargs: Scope-specific parameters

        Returns:
            MetricResult from the metric calculation

        Raises:
            KeyError: If metric_id not found
            ValueError: If scope not supported by metric
        """
        metric = cls.get(metric_id)
        if not metric:
            raise KeyError(f"Metric '{metric_id}' not found")

        metric.validate_scope(scope)

        # Check cache if enabled
        if use_cache and metric.cacheable:
            cached = cls._get_cached(conn, metric, scope, **kwargs)
            if cached:
                return cached

        # Calculate fresh result
        result = metric.calculate(conn, scope, **kwargs)

        # Store in cache if enabled
        if metric.cacheable:
            cls._set_cached(conn, metric, scope, result, **kwargs)

        return result

    @classmethod
    def _get_cached(
        cls,
        conn: sqlite3.Connection,
        metric: BaseMetric,
        scope: Scope,
        **kwargs
    ) -> MetricResult | None:
        """Retrieve cached result if valid."""
        cache_key = metric.cache_key(scope, **kwargs)
        row = conn.execute(
            """
            SELECT result, computed_at FROM metric_cache
            WHERE metric_id = ? AND cache_key = ?
            """,
            (metric.id, cache_key)
        ).fetchone()

        if not row:
            return None

        # Check if cache is still valid
        computed_at = datetime.fromisoformat(row["computed_at"])
        if datetime.now() - computed_at > timedelta(seconds=metric.cache_ttl):
            return None

        # Reconstruct MetricResult from cached JSON
        data = json.loads(row["result"])
        return MetricResult(
            metric_id=data["metric_id"],
            title=data["title"],
            description=data["description"],
            data=data["data"],
            visualization=metric.default_visualization,
            scope=scope,
            columns=data.get("columns"),
            chart_config=data.get("chart_config"),
        )

    @classmethod
    def _set_cached(
        cls,
        conn: sqlite3.Connection,
        metric: BaseMetric,
        scope: Scope,
        result: MetricResult,
        **kwargs
    ) -> None:
        """Store result in cache."""
        cache_key = metric.cache_key(scope, **kwargs)
        result_json = json.dumps(result.to_dict())

        conn.execute(
            """
            INSERT OR REPLACE INTO metric_cache (metric_id, cache_key, result, computed_at)
            VALUES (?, ?, ?, ?)
            """,
            (metric.id, cache_key, result_json, datetime.now().isoformat())
        )
        conn.commit()

    @classmethod
    def clear_cache(cls, conn: sqlite3.Connection, metric_id: str | None = None) -> int:
        """
        Clear cached results.

        Args:
            conn: Database connection
            metric_id: Optional specific metric to clear (clears all if None)

        Returns:
            Number of cache entries cleared
        """
        if metric_id:
            cursor = conn.execute(
                "DELETE FROM metric_cache WHERE metric_id = ?",
                (metric_id,)
            )
        else:
            cursor = conn.execute("DELETE FROM metric_cache")

        conn.commit()
        return cursor.rowcount


def metric(cls: Type[BaseMetric]) -> Type[BaseMetric]:
    """
    Decorator to auto-register a metric class.

    Usage:
        @metric
        class MyMetric(BaseMetric):
            id = "my_metric"
            ...
    """
    instance = cls()
    MetricRegistry.register(instance)
    return cls
