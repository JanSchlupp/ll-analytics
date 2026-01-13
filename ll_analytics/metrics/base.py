"""Base classes for the pluggable metrics framework."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
import sqlite3


class VisualizationType(Enum):
    """How a metric should be visualized in the UI."""
    LINE_CHART = "line"          # Time series
    BAR_CHART = "bar"            # Comparisons
    LEADERBOARD = "leaderboard"  # Ranked table
    HEATMAP = "heatmap"          # Category x Time grids
    SCATTER = "scatter"          # Correlations
    HISTOGRAM = "histogram"      # Distributions


class Scope(Enum):
    """What data scope a metric supports."""
    PLAYER = "player"            # Individual player analysis
    SEASON = "season"            # Season-wide analysis
    RUNDLE = "rundle"            # Rundle comparison
    HEAD_TO_HEAD = "h2h"         # Player vs player


@dataclass
class MetricResult:
    """Standardized output from any metric calculation."""
    metric_id: str
    title: str
    description: str
    data: Any
    visualization: VisualizationType
    scope: Scope
    columns: list[str] | None = None
    chart_config: dict | None = None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "metric_id": self.metric_id,
            "title": self.title,
            "description": self.description,
            "data": self.data,
            "visualization": self.visualization.value,
            "scope": self.scope.value,
            "columns": self.columns,
            "chart_config": self.chart_config,
        }


@dataclass
class MetricInfo:
    """Metadata about a metric for API discovery."""
    id: str
    name: str
    description: str
    scopes: list[str]
    visualization: str

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "scopes": self.scopes,
            "visualization": self.visualization,
        }


class BaseMetric(ABC):
    """
    Abstract base class for all metrics.

    To create a new metric:
    1. Create a new file in ll_analytics/metrics/
    2. Inherit from BaseMetric
    3. Implement all abstract properties and methods
    4. Decorate with @metric to auto-register

    Example:
        @metric
        class MyMetric(BaseMetric):
            id = "my_metric"
            name = "My Custom Metric"
            description = "What this metric measures"
            scopes = [Scope.PLAYER, Scope.SEASON]
            default_visualization = VisualizationType.LINE_CHART

            def calculate(self, conn, scope, **kwargs):
                # Your calculation logic here
                return MetricResult(...)
    """

    # Override these in subclasses
    id: str = ""
    name: str = ""
    description: str = ""
    scopes: list[Scope] = field(default_factory=list)
    default_visualization: VisualizationType = VisualizationType.LEADERBOARD

    # Optional caching settings
    cacheable: bool = False
    cache_ttl: int = 3600  # seconds

    def get_info(self) -> MetricInfo:
        """Get metric metadata for API discovery."""
        return MetricInfo(
            id=self.id,
            name=self.name,
            description=self.description,
            scopes=[s.value for s in self.scopes],
            visualization=self.default_visualization.value,
        )

    @abstractmethod
    def calculate(
        self,
        conn: sqlite3.Connection,
        scope: Scope,
        **kwargs
    ) -> MetricResult:
        """
        Calculate the metric.

        Args:
            conn: Database connection
            scope: The scope of calculation (player, season, etc.)
            **kwargs: Scope-specific parameters:
                - PLAYER: player_id, season_id (optional)
                - SEASON: season_id
                - RUNDLE: rundle_id
                - HEAD_TO_HEAD: player1_id, player2_id, season_id (optional)

        Returns:
            MetricResult with the calculated data
        """
        pass

    def cache_key(self, scope: Scope, **kwargs) -> str:
        """Generate a unique cache key for this calculation."""
        sorted_kwargs = sorted(kwargs.items())
        kwargs_str = "_".join(f"{k}={v}" for k, v in sorted_kwargs)
        return f"{self.id}:{scope.value}:{kwargs_str}"

    def validate_scope(self, scope: Scope) -> None:
        """Raise ValueError if scope not supported."""
        if scope not in self.scopes:
            supported = ", ".join(s.value for s in self.scopes)
            raise ValueError(
                f"Metric '{self.id}' does not support scope '{scope.value}'. "
                f"Supported scopes: {supported}"
            )
