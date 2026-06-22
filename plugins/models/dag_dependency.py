"""
Cross-DAG Dependency Models
Enterprise-grade DAG orchestration with dependency tracking, audit logging, and circular dependency detection.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy import (
    Column,
    String,
    DateTime,
    Integer,
    Text,
    Boolean,
    Index,
    ForeignKey,
    Enum,
)
from sqlalchemy.orm import relationship
from airflow.models.base import Base
import enum
import json


class DependencyStatus(str, enum.Enum):
    """Status of a cross-DAG dependency"""

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    SKIPPED = "SKIPPED"


class DependencyType(str, enum.Enum):
    """Type of dependency relationship"""

    DAG_TO_DAG = "DAG_TO_DAG"
    TASK_TO_TASK = "TASK_TO_TASK"
    TASK_TO_DAG = "TASK_TO_DAG"


class EventTriggerType(str, enum.Enum):
    """Type of event trigger"""

    DATASET = "DATASET"
    EXTERNAL_EVENT = "EXTERNAL_EVENT"
    MANUAL = "MANUAL"


class CrossDAGDependency(Base):
    """
    Model representing a dependency between two DAGs
    Tracks when DAG B should wait for DAG A to complete
    """

    __tablename__ = "cross_dag_dependency"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Source DAG (the one that must complete first)
    source_dag_id = Column(String(250), nullable=False, index=True)
    source_task_id = Column(String(250), nullable=True)  # Optional: specific task

    # Dependent DAG (the one that waits)
    dependent_dag_id = Column(String(250), nullable=False, index=True)
    dependent_task_id = Column(String(250), nullable=True)  # Optional: specific task

    # Dependency metadata
    dependency_type = Column(Enum(DependencyType), default=DependencyType.DAG_TO_DAG)
    timeout_seconds = Column(Integer, default=3600)  # 1 hour default
    skip_on_failure = Column(Boolean, default=False)

    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_by = Column(String(250), nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    updated_by = Column(String(250), nullable=True)

    # Governance
    is_active = Column(Boolean, default=True)
    requires_approval = Column(Boolean, default=False)
    approved_by = Column(String(250), nullable=True)

    # Configuration
    metadata_json = Column(Text, default="{}")  # Store additional config as JSON

    __table_args__ = (
        Index("idx_source_dependent", "source_dag_id", "dependent_dag_id"),
        Index("idx_dependency_type", "dependency_type"),
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "id": self.id,
            "source_dag_id": self.source_dag_id,
            "source_task_id": self.source_task_id,
            "dependent_dag_id": self.dependent_dag_id,
            "dependent_task_id": self.dependent_task_id,
            "dependency_type": self.dependency_type.value,
            "timeout_seconds": self.timeout_seconds,
            "skip_on_failure": self.skip_on_failure,
            "is_active": self.is_active,
        }


class DependencyExecution(Base):
    """
    Track execution state of each dependency
    Records when dependencies are checked and their status
    """

    __tablename__ = "dependency_execution"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Link to dependency definition
    dependency_id = Column(
        Integer, ForeignKey("cross_dag_dependency.id"), nullable=False
    )

    # Execution context
    source_dag_run_id = Column(String(250), nullable=False)
    dependent_dag_run_id = Column(String(250), nullable=False)
    dependent_task_instance_key = Column(String(500), nullable=False)

    # Status tracking
    status = Column(
        Enum(DependencyStatus), default=DependencyStatus.PENDING, index=True
    )

    # Timing
    check_started_at = Column(DateTime, nullable=True)
    check_completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Integer, nullable=True)

    # Audit
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        Index("idx_source_dependent_run", "source_dag_run_id", "dependent_dag_run_id"),
        Index("idx_status_check", "status", "check_completed_at"),
    )


class CircularDependencyDetection(Base):
    """
    Track detected circular dependencies
    Prevents infinite loops in DAG execution
    """

    __tablename__ = "circular_dependency_detection"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Cycle information
    cycle_path = Column(Text, nullable=False)  # JSON array of DAG IDs forming the cycle
    detection_timestamp = Column(DateTime, default=datetime.utcnow)
    is_resolved = Column(Boolean, default=False)

    # Severity
    severity = Column(String(20), default="HIGH")  # LOW, MEDIUM, HIGH, CRITICAL

    created_at = Column(DateTime, default=datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)
    resolved_by = Column(String(250), nullable=True)


class DependencyAuditLog(Base):
    """
    Complete audit trail of all dependency-related operations
    Enterprise compliance and troubleshooting
    """

    __tablename__ = "dependency_audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Operation details
    operation_type = Column(
        String(50), nullable=False
    )  # CREATE, UPDATE, DELETE, CHECK, APPROVE
    dependency_id = Column(Integer, nullable=True)

    # Who and when
    user_id = Column(String(250), nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)

    # What changed
    changes_json = Column(Text)  # JSON of changes made

    # Context
    source_system = Column(String(100), nullable=True)
    request_id = Column(String(250), nullable=True)

    __table_args__ = (
        Index("idx_user_timestamp", "user_id", "timestamp"),
        Index("idx_operation_type", "operation_type"),
    )


class EventTriggerRule(Base):
    """
    Define external event triggers for DAG execution
    Event-driven workflow triggering
    """

    __tablename__ = "event_trigger_rule"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Target DAG
    dag_id = Column(String(250), nullable=False, index=True)
    task_id = Column(String(250), nullable=True)

    # Trigger configuration
    trigger_type = Column(Enum(EventTriggerType), nullable=False)
    event_pattern = Column(String(500), nullable=False)  # Regex or dataset pattern

    # Payload mapping
    payload_template = Column(Text)  # JSON template for dag_run configuration

    # Governance
    is_active = Column(Boolean, default=True)
    requires_approval = Column(Boolean, default=False)

    # Audit
    created_at = Column(DateTime, default=datetime.utcnow)
    created_by = Column(String(250), nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (Index("idx_dag_trigger", "dag_id", "trigger_type"),)


class DependencyMetrics(Base):
    """
    Store aggregated metrics for observability
    Prometheus scrapes these for dashboards
    """

    __tablename__ = "dependency_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Metric details
    metric_name = Column(
        String(250), nullable=False
    )  # dependency_check_duration, circular_dependencies_detected, etc.
    metric_value = Column(Integer, nullable=False)

    # Labeling for Prometheus
    dag_id = Column(String(250), nullable=True)
    status = Column(String(50), nullable=True)

    # Time series
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (Index("idx_metric_timestamp", "metric_name", "timestamp"),)
