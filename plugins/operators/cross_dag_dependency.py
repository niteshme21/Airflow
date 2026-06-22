"""
Cross-DAG Dependency Operators and Sensors
"""

from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.sensors.base import BaseSensorOperator, poke_mode_only
from airflow.utils.context import Context
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.exceptions import AirflowException
from airflow.utils.state import DagRunState, TaskInstanceState
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session
import logging
import json
from .models.dag_dependency import (
    CrossDAGDependency,
    DependencyExecution,
    CircularDependencyDetection,
    DependencyAuditLog,
    DependencyStatus,
    DependencyType,
)
from .hooks.dependency_hook import DependencyManagementHook

logger = logging.getLogger(__name__)


@poke_mode_only
class CrossDAGDependencySensor(BaseSensorOperator):
    """
    Sensor that waits for a dependent DAG/task to reach a successful state

    Example:
        sensor = CrossDAGDependencySensor(
            task_id='wait_for_etl',
            source_dag_id='upstream_etl_dag',
            timeout_seconds=3600,
            poke_interval=60,
        )
    """

    template_fields = ["source_dag_id", "source_task_id"]

    def __init__(
        self,
        source_dag_id: str,
        source_task_id: Optional[str] = None,
        timeout_seconds: int = 3600,
        poke_interval: int = 60,
        skip_on_failure: bool = False,
        dependency_id: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_dag_id = source_dag_id
        self.source_task_id = source_task_id
        self.timeout_seconds = timeout_seconds
        self.poke_interval = poke_interval
        self.skip_on_failure = skip_on_failure
        self.dependency_id = dependency_id
        self.hook = DependencyManagementHook()

    def poke(self, context: Context) -> bool:
        """
        Check if upstream DAG/task has completed successfully
        """
        execution_date = context["execution_date"]
        dag_run = context["dag_run"]
        task_instance = context["task_instance"]

        try:
            # Check if source DAG run exists and is in success state
            success = self._check_source_status(execution_date)

            if success:
                # Log successful dependency check
                self._log_dependency_check(
                    task_instance, DependencyStatus.SUCCESS, context
                )
                return True
            else:
                # Not ready yet
                return False

        except Exception as e:
            logger.error(f"Error checking cross-DAG dependency: {str(e)}")
            if not self.skip_on_failure:
                raise AirflowException(f"Dependency check failed: {str(e)}")
            return True

    def _check_source_status(self, execution_date: datetime) -> bool:
        """Check if source DAG/task completed successfully"""
        from airflow.models import DagRun, TaskInstance
        from airflow.utils.state import DagRunState, TaskInstanceState

        session = Session()
        try:
            # Find the source DAG run
            source_run = (
                session.query(DagRun)
                .filter(
                    and_(
                        DagRun.dag_id == self.source_dag_id,
                        DagRun.execution_date <= execution_date,
                    )
                )
                .order_by(DagRun.execution_date.desc())
                .first()
            )

            if not source_run:
                logger.info(f"No run found for source DAG {self.source_dag_id}")
                return False

            # Check if specific task was requested
            if self.source_task_id:
                task_instance = (
                    session.query(TaskInstance)
                    .filter(
                        and_(
                            TaskInstance.dag_id == self.source_dag_id,
                            TaskInstance.task_id == self.source_task_id,
                            TaskInstance.execution_date == source_run.execution_date,
                        )
                    )
                    .first()
                )

                is_success = (
                    task_instance and task_instance.state == TaskInstanceState.SUCCESS
                )
            else:
                # Check if entire DAG run succeeded
                is_success = source_run.state == DagRunState.SUCCESS

            return is_success

        finally:
            session.close()

    def _log_dependency_check(
        self, task_instance, status: DependencyStatus, context: Context
    ):
        """Log dependency check to audit trail"""
        self.hook.log_dependency_check(
            source_dag_id=self.source_dag_id,
            dependent_dag_id=context["dag"].dag_id,
            status=status,
            user_id=getattr(task_instance, "owner", "system") or "system",
            metadata={
                "source_task_id": self.source_task_id,
                "dependent_task_id": task_instance.task_id,
            },
        )


class RegisterCrossDAGDependencyOperator(BaseOperator):
    """
    Register or update a cross-DAG dependency definition
    Used for dependency lifecycle management

    Example:
        register = RegisterCrossDAGDependencyOperator(
            task_id='register_dependency',
            source_dag_id='source_dag',
            dependent_dag_id='dependent_dag',
            timeout_seconds=7200,
        )
    """

    def __init__(
        self,
        source_dag_id: str,
        dependent_dag_id: str,
        source_task_id: Optional[str] = None,
        dependent_task_id: Optional[str] = None,
        timeout_seconds: int = 3600,
        skip_on_failure: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_dag_id = source_dag_id
        self.dependent_dag_id = dependent_dag_id
        self.source_task_id = source_task_id
        self.dependent_task_id = dependent_task_id
        self.timeout_seconds = timeout_seconds
        self.skip_on_failure = skip_on_failure
        self.hook = DependencyManagementHook()

    def execute(self, context: Context) -> Dict[str, Any]:
        """Register the dependency"""
        # Check for circular dependencies
        if self.hook.detect_circular_dependency(
            self.source_dag_id, self.dependent_dag_id
        ):
            raise AirflowException(
                f"Circular dependency detected between {self.source_dag_id} "
                f"and {self.dependent_dag_id}"
            )

        # Register dependency
        dependency = self.hook.create_dependency(
            source_dag_id=self.source_dag_id,
            dependent_dag_id=self.dependent_dag_id,
            source_task_id=self.source_task_id,
            dependent_task_id=self.dependent_task_id,
            timeout_seconds=self.timeout_seconds,
            skip_on_failure=self.skip_on_failure,
            user_id=getattr(context["task_instance"], "owner", "system") or "system",
        )

        logger.info(f"Registered dependency: {dependency.id}")
        return {"dependency_id": dependency.id}


class DependencyVisualizationOperator(BaseOperator):
    """
    Generate dependency graph visualization
    Creates a graph showing all DAG dependencies
    """

    def __init__(self, output_path: str = "/tmp/dependency_graph.json", **kwargs):
        super().__init__(**kwargs)
        self.output_path = output_path
        self.hook = DependencyManagementHook()

    def execute(self, context: Context) -> str:
        """Generate visualization"""
        dependencies = self.hook.get_all_dependencies()

        # Build graph structure
        graph: Dict[str, List[Dict[str, Any]]] = {"nodes": [], "edges": []}

        seen_dags = set()
        for dep in dependencies:
            # Add nodes
            if dep.source_dag_id not in seen_dags:
                graph["nodes"].append({"id": dep.source_dag_id, "type": "dag"})
                seen_dags.add(dep.source_dag_id)

            if dep.dependent_dag_id not in seen_dags:
                graph["nodes"].append({"id": dep.dependent_dag_id, "type": "dag"})
                seen_dags.add(dep.dependent_dag_id)

            # Add edges
            graph["edges"].append(
                {
                    "from": dep.source_dag_id,
                    "to": dep.dependent_dag_id,
                    "timeout": dep.timeout_seconds,
                    "type": dep.dependency_type.value,
                }
            )

        # Save to file
        with open(self.output_path, "w") as f:
            json.dump(graph, f, indent=2)

        logger.info(f"Dependency graph saved to {self.output_path}")
        return self.output_path


class HealthCheckOperator(BaseOperator):
    """
    Verify the health of the dependency management system
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.hook = DependencyManagementHook()

    def execute(self, context: Context) -> Dict[str, Any]:
        """Run health checks"""
        health_report: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(),
            "status": "HEALTHY",
            "checks": {},
        }

        try:
            # Check database connectivity
            health_report["checks"][
                "database"
            ] = self.hook.check_database_connectivity()

            # Check for unresolved circular dependencies
            circular_deps = self.hook.get_unresolved_circular_dependencies()
            health_report["checks"]["circular_dependencies"] = {
                "count": len(circular_deps),
                "status": "OK" if len(circular_deps) == 0 else "WARNING",
            }
            if len(circular_deps) > 0:
                health_report["status"] = "DEGRADED"

            # Check for stuck dependencies
            stuck_deps = self.hook.get_stuck_dependencies(timeout_minutes=60)
            health_report["checks"]["stuck_dependencies"] = {
                "count": len(stuck_deps),
                "status": "OK" if len(stuck_deps) == 0 else "WARNING",
            }
            if len(stuck_deps) > 0:
                health_report["status"] = "DEGRADED"

        except Exception as e:
            health_report["status"] = "UNHEALTHY"
            health_report["error"] = str(e)

        logger.info(f"Health check complete: {health_report['status']}")
        return health_report
