"""
Dependency Management Hook
Core business logic for dependency tracking and validation
"""

from typing import List, Optional, Dict, Any, Set
from sqlalchemy import and_, or_, func
from sqlalchemy.orm import Session
from airflow.hooks.base import BaseHook
from airflow.models import DagModel
import logging
import json
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DependencyManagementHook(BaseHook):
    """
    Hook for managing cross-DAG dependencies
    Handles creation, validation, and querying of dependencies
    """
    
    def __init__(self):
        super().__init__()
        self.conn_id = 'airflow_db'

    def get_session(self) -> Session:
        """Get database session"""
        from airflow.models import DagModel
        from airflow.settings import Session as AirflowSession
        return AirflowSession()

    def create_dependency(
        self,
        source_dag_id: str,
        dependent_dag_id: str,
        source_task_id: Optional[str] = None,
        dependent_task_id: Optional[str] = None,
        timeout_seconds: int = 3600,
        skip_on_failure: bool = False,
        user_id: str = 'system',
    ):
        """Create a new cross-DAG dependency"""
        from .models.dag_dependency import CrossDAGDependency, DependencyAuditLog, DependencyType
        
        session = self.get_session()
        try:
            # Validate both DAGs exist
            self._validate_dags_exist(source_dag_id, dependent_dag_id, session)
            
            # Create dependency
            dependency = CrossDAGDependency(
                source_dag_id=source_dag_id,
                source_task_id=source_task_id,
                dependent_dag_id=dependent_dag_id,
                dependent_task_id=dependent_task_id,
                dependency_type=DependencyType.DAG_TO_DAG if not source_task_id else DependencyType.TASK_TO_DAG,
                timeout_seconds=timeout_seconds,
                skip_on_failure=skip_on_failure,
                created_by=user_id,
                updated_by=user_id,
            )
            
            session.add(dependency)
            session.flush()
            
            # Audit log
            audit = DependencyAuditLog(
                operation_type='CREATE',
                dependency_id=dependency.id,
                user_id=user_id,
                changes_json=json.dumps(dependency.to_dict()),
            )
            session.add(audit)
            session.commit()
            
            logger.info(f"Created dependency {dependency.id}: {source_dag_id} -> {dependent_dag_id}")
            return dependency
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to create dependency: {str(e)}")
            raise
        finally:
            session.close()

    def get_all_dependencies(self) -> List:
        """Get all active dependencies"""
        from .models.dag_dependency import CrossDAGDependency
        
        session = self.get_session()
        try:
            dependencies = session.query(CrossDAGDependency).filter(
                CrossDAGDependency.is_active == True
            ).all()
            return dependencies
        finally:
            session.close()

    def detect_circular_dependency(self, source_dag_id: str, dependent_dag_id: str) -> bool:
        """
        Detect if adding this dependency would create a circular reference
        Uses depth-first search
        """
        from .models.dag_dependency import CrossDAGDependency
        
        session = self.get_session()
        try:
            # Build adjacency list
            all_deps = session.query(CrossDAGDependency).filter(
                CrossDAGDependency.is_active == True
            ).all()
            
            graph: Dict[str, List[str]] = {}
            for dep in all_deps:
                if dep.source_dag_id not in graph:
                    graph[dep.source_dag_id] = []
                graph[dep.source_dag_id].append(dep.dependent_dag_id)
            
            # DFS to check for cycle
            def has_cycle(node, visited, rec_stack):
                visited.add(node)
                rec_stack.add(node)
                
                for neighbor in graph.get(node, []):
                    if neighbor not in visited:
                        if has_cycle(neighbor, visited, rec_stack):
                            return True
                    elif neighbor in rec_stack:
                        return True
                
                rec_stack.remove(node)
                return False
            
            # Simulate adding the new edge
            if source_dag_id not in graph:
                graph[source_dag_id] = []
            graph[source_dag_id].append(dependent_dag_id)
            
            # Check for cycles
            visited: Set[str] = set()
            for node in graph:
                if node not in visited:
                    if has_cycle(node, visited, set()):
                        logger.warning(f"Circular dependency detected: {source_dag_id} -> {dependent_dag_id}")
                        return True
            
            return False
            
        finally:
            session.close()

    def get_unresolved_circular_dependencies(self) -> List:
        """Get circular dependencies that haven't been resolved"""
        from .models.dag_dependency import CircularDependencyDetection
        
        session = self.get_session()
        try:
            unresolved = session.query(CircularDependencyDetection).filter(
                CircularDependencyDetection.is_resolved == False
            ).all()
            return unresolved
        finally:
            session.close()

    def get_stuck_dependencies(self, timeout_minutes: int = 60) -> List:
        """Get dependencies that have been pending longer than timeout"""
        from .models.dag_dependency import DependencyExecution, DependencyStatus
        
        session = self.get_session()
        try:
            cutoff_time = datetime.utcnow() - timedelta(minutes=timeout_minutes)
            
            stuck = session.query(DependencyExecution).filter(
                and_(
                    DependencyExecution.status == DependencyStatus.PENDING,
                    DependencyExecution.created_at < cutoff_time,
                )
            ).all()
            return stuck
        finally:
            session.close()

    def log_dependency_check(
        self,
        source_dag_id: str,
        dependent_dag_id: str,
        status: Any,
        user_id: str,
        metadata: Dict[str, Any],
    ):
        """Log a dependency check operation"""
        from .models.dag_dependency import DependencyAuditLog
        
        session = self.get_session()
        try:
            audit = DependencyAuditLog(
                operation_type='CHECK',
                user_id=user_id,
                changes_json=json.dumps({
                    'source_dag_id': source_dag_id,
                    'dependent_dag_id': dependent_dag_id,
                    'status': status.value if hasattr(status, 'value') else str(status),
                    **metadata
                }),
            )
            session.add(audit)
            session.commit()
        except Exception as e:
            logger.error(f"Failed to log dependency check: {str(e)}")
        finally:
            session.close()

    def check_database_connectivity(self) -> Dict[str, Any]:
        """Check if database is accessible"""
        try:
            session = self.get_session()
            session.execute("SELECT 1")
            session.close()
            return {'status': 'OK', 'message': 'Database is accessible'}
        except Exception as e:
            return {'status': 'ERROR', 'message': str(e)}

    def _validate_dags_exist(self, source_dag_id: str, dependent_dag_id: str, session: Session):
        """Validate that both DAGs exist in Airflow"""
        from airflow.models import DagModel
        
        source_dag = session.query(DagModel).filter(DagModel.dag_id == source_dag_id).first()
        if not source_dag:
            raise ValueError(f"Source DAG {source_dag_id} does not exist")
        
        dependent_dag = session.query(DagModel).filter(DagModel.dag_id == dependent_dag_id).first()
        if not dependent_dag:
            raise ValueError(f"Dependent DAG {dependent_dag_id} does not exist")

    def get_dependency_graph(self) -> Dict[str, Any]:
        """Get complete dependency graph"""
        from .models.dag_dependency import CrossDAGDependency
        
        session = self.get_session()
        try:
            dependencies = session.query(CrossDAGDependency).filter(
                CrossDAGDependency.is_active == True
            ).all()
            
            graph = {
                'nodes': list(set(
                    [d.source_dag_id for d in dependencies] +
                    [d.dependent_dag_id for d in dependencies]
                )),
                'edges': [
                    {
                        'from': d.source_dag_id,
                        'to': d.dependent_dag_id,
                        'timeout': d.timeout_seconds,
                    } for d in dependencies
                ]
            }
            return graph
        finally:
            session.close()
