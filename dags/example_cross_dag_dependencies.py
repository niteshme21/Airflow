"""
Example DAGs demonstrating cross-DAG dependency usage
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

# Add plugins to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from plugins.operators.cross_dag_dependency import (
    CrossDAGDependencySensor,
    RegisterCrossDAGDependencyOperator,
    DependencyVisualizationOperator,
)

default_args = {
    "owner": "enterprise_platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}

# ============================================================================
# DAG 1: Source/Upstream DAG
# ============================================================================

dag_1 = DAG(
    "etl_source_data_extraction",
    default_args=default_args,
    description="Extract data from source systems",
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "enterprise"],
)


def extract_data():
    print("Extracting data from sources...")
    return {"status": "success", "records": 1000}


with dag_1:
    extract = PythonOperator(
        task_id="extract_from_api",
        python_callable=extract_data,
    )

    validate = BashOperator(
        task_id="validate_data",
        bash_command="echo \"Validating {{ task_instance.xcom_pull(task_ids='extract_from_api') }}\"",
    )

    extract >> validate


# ============================================================================
# DAG 2: Dependent DAG that waits for DAG 1
# ============================================================================

dag_2 = DAG(
    "analytics_transformation_pipeline",
    default_args=default_args,
    description="Transform data from source DAG",
    schedule_interval="@daily",
    catchup=False,
    tags=["analytics", "enterprise"],
)

with dag_2:
    # Register dependency: this DAG depends on etl_source_data_extraction
    register_dep = RegisterCrossDAGDependencyOperator(
        task_id="register_dependency",
        source_dag_id="etl_source_data_extraction",
        dependent_dag_id="analytics_transformation_pipeline",
        timeout_seconds=7200,  # 2 hours
    )

    # Wait for the source DAG to complete
    wait_for_source = CrossDAGDependencySensor(
        task_id="wait_for_source_completion",
        source_dag_id="etl_source_data_extraction",
        timeout_seconds=7200,
        poke_interval=60,
    )

    # Transform the data
    transform = PythonOperator(
        task_id="transform_data",
        python_callable=lambda: print("Transforming data..."),
    )

    load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=lambda: print("Loading to data warehouse..."),
    )

    register_dep >> wait_for_source >> transform >> load


# ============================================================================
# DAG 3: Report DAG that depends on both DAG 1 and DAG 2
# ============================================================================

dag_3 = DAG(
    "executive_reporting_pipeline",
    default_args=default_args,
    description="Generate executive reports",
    schedule_interval="@daily",
    catchup=False,
    tags=["reporting", "enterprise"],
)

with dag_3:
    # Wait for analytics transformation to complete
    wait_for_analytics = CrossDAGDependencySensor(
        task_id="wait_for_analytics_completion",
        source_dag_id="analytics_transformation_pipeline",
        timeout_seconds=7200,
    )

    # Generate reports
    generate_report = BashOperator(
        task_id="generate_executive_report",
        bash_command='echo "Generating reports for executives"',
    )

    # Send to stakeholders
    notify = PythonOperator(
        task_id="send_to_stakeholders",
        python_callable=lambda: print("Sending reports to stakeholders..."),
    )

    wait_for_analytics >> generate_report >> notify


# ============================================================================
# Monitoring DAG: Visualize all dependencies
# ============================================================================

dag_monitor = DAG(
    "dependency_management_monitor",
    default_args=default_args,
    description="Monitor and visualize all cross-DAG dependencies",
    schedule_interval="@hourly",
    catchup=False,
    tags=["monitoring", "enterprise"],
)

with dag_monitor:
    visualize_deps = DependencyVisualizationOperator(
        task_id="generate_dependency_graph",
        output_path="/tmp/dependency_graph.json",
    )

    health_check = BashOperator(
        task_id="health_check_system",
        bash_command='echo "Running health checks..."',
    )

    visualize_deps >> health_check


# Export DAGs
all_dags = [dag_1, dag_2, dag_3, dag_monitor]
