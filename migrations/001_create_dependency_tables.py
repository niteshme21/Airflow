"""
Database migration: Create cross-DAG dependency tables
This should be run during Airflow initialization
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# Alembic migration format
def upgrade():
    """Create dependency tables"""
    
    # Cross-DAG Dependency table
    op.create_table(
        'cross_dag_dependency',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('source_dag_id', sa.String(length=250), nullable=False),
        sa.Column('source_task_id', sa.String(length=250), nullable=True),
        sa.Column('dependent_dag_id', sa.String(length=250), nullable=False),
        sa.Column('dependent_task_id', sa.String(length=250), nullable=True),
        sa.Column('dependency_type', sa.String(length=50), nullable=False, server_default='DAG_TO_DAG'),
        sa.Column('timeout_seconds', sa.Integer(), nullable=False, server_default='3600'),
        sa.Column('skip_on_failure', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('created_by', sa.String(length=250), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.Column('updated_by', sa.String(length=250), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='1'),
        sa.Column('requires_approval', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('approved_by', sa.String(length=250), nullable=True),
        sa.Column('metadata_json', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_source_dependent', 'cross_dag_dependency', ['source_dag_id', 'dependent_dag_id'])
    op.create_index('idx_dependency_type', 'cross_dag_dependency', ['dependency_type'])

    # Dependency Execution table
    op.create_table(
        'dependency_execution',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dependency_id', sa.Integer(), nullable=False),
        sa.Column('source_dag_run_id', sa.String(length=250), nullable=False),
        sa.Column('dependent_dag_run_id', sa.String(length=250), nullable=False),
        sa.Column('dependent_task_instance_key', sa.String(length=500), nullable=False),
        sa.Column('status', sa.String(length=50), nullable=False, server_default='PENDING'),
        sa.Column('check_started_at', sa.DateTime(), nullable=True),
        sa.Column('check_completed_at', sa.DateTime(), nullable=True),
        sa.Column('duration_seconds', sa.Integer(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.ForeignKeyConstraint(['dependency_id'], ['cross_dag_dependency.id']),
    )
    op.create_index('idx_source_dependent_run', 'dependency_execution', ['source_dag_run_id', 'dependent_dag_run_id'])
    op.create_index('idx_status_check', 'dependency_execution', ['status', 'check_completed_at'])

    # Circular Dependency Detection table
    op.create_table(
        'circular_dependency_detection',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('cycle_path', sa.Text(), nullable=False),
        sa.Column('detection_timestamp', sa.DateTime(), nullable=False),
        sa.Column('is_resolved', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('severity', sa.String(length=20), nullable=False, server_default='HIGH'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('resolved_at', sa.DateTime(), nullable=True),
        sa.Column('resolved_by', sa.String(length=250), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )

    # Dependency Audit Log table
    op.create_table(
        'dependency_audit_log',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('operation_type', sa.String(length=50), nullable=False),
        sa.Column('dependency_id', sa.Integer(), nullable=True),
        sa.Column('user_id', sa.String(length=250), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('changes_json', sa.Text(), nullable=True),
        sa.Column('source_system', sa.String(length=100), nullable=True),
        sa.Column('request_id', sa.String(length=250), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_user_timestamp', 'dependency_audit_log', ['user_id', 'timestamp'])
    op.create_index('idx_operation_type', 'dependency_audit_log', ['operation_type'])

    # Event Trigger Rule table
    op.create_table(
        'event_trigger_rule',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dag_id', sa.String(length=250), nullable=False),
        sa.Column('task_id', sa.String(length=250), nullable=True),
        sa.Column('trigger_type', sa.String(length=50), nullable=False),
        sa.Column('event_pattern', sa.String(length=500), nullable=False),
        sa.Column('payload_template', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='1'),
        sa.Column('requires_approval', sa.Boolean(), nullable=False, server_default='0'),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('created_by', sa.String(length=250), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_dag_trigger', 'event_trigger_rule', ['dag_id', 'trigger_type'])

    # Dependency Metrics table
    op.create_table(
        'dependency_metrics',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('metric_name', sa.String(length=250), nullable=False),
        sa.Column('metric_value', sa.Integer(), nullable=False),
        sa.Column('dag_id', sa.String(length=250), nullable=True),
        sa.Column('status', sa.String(length=50), nullable=True),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_metric_timestamp', 'dependency_metrics', ['metric_name', 'timestamp'])


def downgrade():
    """Drop dependency tables"""
    op.drop_table('dependency_metrics')
    op.drop_table('event_trigger_rule')
    op.drop_table('dependency_audit_log')
    op.drop_table('circular_dependency_detection')
    op.drop_table('dependency_execution')
    op.drop_table('cross_dag_dependency')
