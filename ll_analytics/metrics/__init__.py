"""
Pluggable Metrics Framework for Learned League Analytics.

This package provides a flexible system for defining and calculating
custom metrics on Learned League data.

To add a new metric:
1. Create a new file in this package (e.g., my_metric.py)
2. Define a class inheriting from BaseMetric
3. Decorate it with @metric for auto-registration

Example:
    from .base import BaseMetric, MetricResult, Scope, VisualizationType
    from .registry import metric

    @metric
    class MyMetric(BaseMetric):
        id = "my_metric"
        name = "My Custom Metric"
        description = "What it measures"
        scopes = [Scope.PLAYER, Scope.SEASON]
        default_visualization = VisualizationType.LINE_CHART

        def calculate(self, conn, scope, **kwargs):
            # Your logic here
            return MetricResult(...)
"""

# Export public API
from .base import (
    BaseMetric,
    MetricResult,
    MetricInfo,
    Scope,
    VisualizationType,
)
from .registry import MetricRegistry, metric

# Import all metrics to trigger @metric decorator registration
# Add new metric modules here as they are created
from . import surprise
from . import late_spike
from . import luck

__all__ = [
    "BaseMetric",
    "MetricResult",
    "MetricInfo",
    "Scope",
    "VisualizationType",
    "MetricRegistry",
    "metric",
]
