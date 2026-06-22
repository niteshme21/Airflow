"""
Prometheus metrics for dependency management
"""

from prometheus_client import Counter, Histogram, Gauge
import time

# Counters
dependency_checks_total = Counter(
    "dependency_checks_total",
    "Total number of dependency checks performed",
    ["status"],  # SUCCESS, FAILED, TIMEOUT
)

dependency_created_total = Counter(
    "dependency_created_total", "Total number of dependencies created"
)

circular_dependencies_detected_total = Counter(
    "circular_dependencies_detected_total", "Total circular dependencies detected"
)

# Histograms
dependency_check_duration_seconds = Histogram(
    "dependency_check_duration_seconds",
    "Time taken to check a dependency",
    buckets=(0.5, 1, 2, 5, 10, 30, 60),
)

dag_execution_duration_seconds = Histogram(
    "dag_execution_duration_seconds", "DAG execution duration", ["dag_id"]
)

# Gauges
active_dependencies = Gauge(
    "active_dependencies_total", "Total number of active dependencies"
)

pending_dependencies = Gauge(
    "pending_dependencies_total", "Number of dependencies waiting to be satisfied"
)

failed_dependencies = Gauge(
    "failed_dependencies_total", "Number of failed dependencies"
)


class MetricsCollector:
    """Helper class for recording metrics"""

    @staticmethod
    def record_dependency_check(status: str, duration: float):
        """Record a dependency check metric"""
        dependency_checks_total.labels(status=status).inc()
        dependency_check_duration_seconds.observe(duration)

    @staticmethod
    def update_dependency_counts(active: int, pending: int, failed: int):
        """Update dependency count gauges"""
        active_dependencies.set(active)
        pending_dependencies.set(pending)
        failed_dependencies.set(failed)
