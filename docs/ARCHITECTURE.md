# Architecture Design Document

## System Overview

This document describes the architectural decisions, design patterns, and tradeoffs in the Airflow Enterprise Cross-DAG Dependency Orchestration Platform.

## Problem Statement

**Native Airflow Gap:** Airflow lacks native support for:
- Cross-DAG dependencies (DAG A must complete before DAG B starts)
- Event-driven triggering between DAGs
- Circular dependency detection and prevention
- Enterprise audit logging for compliance
- Governance and approval workflows

**Business Impact:**
- Manual DAG scheduling errors
- Data pipeline SLA violations
- Lack of compliance audit trail
- Limited multi-team governance

## Architectural Principles

1. **Non-invasive**: Extend Airflow without forking
2. **Scalable**: Handle thousands of DAGs and dependencies
3. **Observable**: Complete metrics and logging
4. **Governance**: Full audit trail for compliance
5. **Resilient**: Graceful handling of failures
6. **Testable**: Unit tested, production-ready

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Airflow Webserver/Scheduler                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              DAG Execution Layer                          │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐                │  │
│  │  │  Task A  │  │  Task B  │  │  Task C  │                │  │
│  │  └──────────┘  └──────────┘  └──────────┘                │  │
│  └───────────────────────────────────────────────────────────┘  │
│                           │                                      │
│                           ▼                                      │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │       Cross-DAG Dependency Engine (Custom Layer)          │  │
│  │                                                            │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │  │
│  │  │  Sensors     │  │  Operators   │  │  Hooks       │   │  │
│  │  │  (Poke)      │  │  (Register)  │  │  (Logic)     │   │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘   │  │
│  │         │                 │                  │            │  │
│  │         └─────────────────┴──────────────────┘            │  │
│  │                      │                                     │  │
│  └───────────────────────┼──────────────────────────────────┘  │
│                          │                                      │
│         ┌────────────────┼────────────────┐                    │
│         │                │                │                    │
│         ▼                ▼                ▼                    │
│  ┌────────────┐   ┌────────────┐   ┌──────────────┐           │
│  │  Postgres  │   │  Metrics   │   │  Logs        │           │
│  │  Database  │   │  Prometheus│   │  CloudWatch  │           │
│  └────────────┘   └────────────┘   └──────────────┘           │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Design Decisions & Tradeoffs

### 1. Operator-Sensor Approach (vs DAG-level Modification)

**Decision:** Use custom operators and sensors rather than modifying core Airflow

**Rationale:**
- ✅ Non-invasive: Works with any Airflow version
- ✅ Easier upgrades: No code merges with Airflow updates
- ✅ Plugin architecture: Follows Airflow patterns
- ❌ Slight overhead: Additional database queries
- ❌ Not native: Not built into Airflow core

**Alternatives Considered:**
1. Fork Airflow and modify scheduler
   - Pro: Most performant
   - Con: Maintenance nightmare, difficult upgrades
   
2. External orchestration layer
   - Pro: Complete separation
   - Con: Requires separate infrastructure, complexity

**Interview Answer:**
> "We chose the plugin approach because it provides upgrade compatibility and follows Airflow's plugin architecture. This means we can upgrade Airflow versions without rebasing our changes. The slight performance overhead is negligible compared to network latency and database queries."

### 2. Polling Sensor Architecture (vs Push Events)

**Decision:** Use polling-based CrossDAGDependencySensor instead of event push

**Rationale:**
- ✅ Reliable: Works with any upstream system
- ✅ Debuggable: Easy to understand flow
- ✅ Resilient: Automatic retries on transient failures
- ❌ Resource intensive: Database queries every poke interval
- ❌ Latency: Slight delay between completion and detection

**Tradeoffs:**
```
Polling Sensor              Event Push Architecture
- ✅ Simple implementation  - ✅ Low latency
- ✅ Reliable              - ✅ Event-driven
- ✅ No external deps      - ❌ Complex error handling
- ❌ Latency 60-300s       - ❌ Requires message queue
- ❌ Database load        - ❌ More infrastructure
```

**Optimization:** Poke interval is configurable per dependency

### 3. Database Storage (vs Configuration Files)

**Decision:** Store dependencies in PostgreSQL tables, not YAML files

**Rationale:**
- ✅ Dynamic: Changes without DAG redeploy
- ✅ Queryable: Can analyze dependency graph
- ✅ Auditable: Complete version history
- ✅ Scalable: Handles millions of dependencies
- ❌ Not GitOps: Changes not versioned in repo
- ❌ Database dependency: Requires operational overhead

**How We Address GitOps Gap:**
- Audit log captures all changes with user/timestamp
- DAG-to-DAG dependencies can be declared in DAG code (RegisterOperator)
- Export/import functionality for versioning

### 4. Circular Detection Algorithm (DFS vs Other)

**Decision:** Depth-First Search for cycle detection

**Rationale:**
- ✅ O(V+E) complexity: Efficient for large graphs
- ✅ Simple implementation: Standard algorithm
- ✅ No external dependencies: Pure Python
- ❌ Real-time check only: Not continuous monitoring

**Alternatives:**
```
Algorithm                Complexity    Implementation
DFS (chosen)            O(V+E)        Simple, standard
Topological Sort        O(V+E)        More complex
Tarjan's Algorithm      O(V+E)        Better for SCCs, overkill
Color-based approach    O(V+E)        Equivalent to DFS
```

### 5. Kubernetes Deployment (vs ECS/Lambda)

**Decision:** Deploy to EKS (Kubernetes on AWS)

**Rationale:**
- ✅ Portable: Run anywhere (on-prem, any cloud)
- ✅ Declarative: Infrastructure as code with manifests
- ✅ Ecosystem: Rich tooling (Helm, Operators, etc.)
- ✅ Multi-cloud: Can move to GKE/AKS
- ❌ Operational overhead: More to manage than serverless
- ❌ Cost: Minimum resources even at idle

**Alternatives:**
```
EKS                    ECS                   Fargate
- ✅ Portable         - ✅ AWS-native       - ✅ Serverless
- ✅ Standard K8s    - ✅ Simpler ops      - ✅ No node mgmt
- ❌ More complex    - ❌ AWS lock-in      - ❌ Cold starts
-  Costs ~$70/mo     - No cluster cost     - Best for bursty
```

### 6. Aurora PostgreSQL (vs Airflow SQLite/RDS)

**Decision:** Aurora PostgreSQL cluster with read replicas

**Rationale:**
- ✅ High availability: Multi-AZ failover
- ✅ Performance: Better than single RDS instance
- ✅ Scaling: Read replicas for reporting
- ✅ Cost-effective: Pay for used storage
- ❌ More operational complexity
- ❌ Slightly higher cost than single instance

**Complexity by Choice:**
```
SQLite              Single RDS          Aurora Cluster
- ✅ Simplest       - ✅ Standard       - ✅ Most resilient
- ❌ No HA         - ✅ HA via snapshots - Moderate complexity
- ❌ No scaling    - ❌ Single point    - ✅ Auto-scaling
- ✅ Zero cost     - ~$200/mo          - ~$300-500/mo
```

### 7. Prometheus Metrics (vs CloudWatch/DataDog)

**Decision:** Prometheus for metrics collection with CloudWatch integration

**Rationale:**
- ✅ Open source: No vendor lock-in
- ✅ Easy integration: Works with Kubernetes
- ✅ Community standard: Industry adoption
- ✅ Cost: Free self-hosted
- ❌ Operational overhead: Need to operate Prometheus
- ❌ Limited alerting: Basic compared to commercial tools

**Comparison:**
```
Prometheus          CloudWatch         DataDog
- ✅ Open source   - ✅ AWS native    - ✅ Full-featured
- ✅ Multi-cloud  - ❌ AWS only      - ✅ Best-in-class
- ❌ Self-hosted  - ✅ Managed       - ❌ Most expensive
- Free             - ~$50/mo         - ~$500+/mo
```

### 8. GitHub Actions (vs Jenkins/GitLab CI)

**Decision:** GitHub Actions for CI/CD pipeline

**Rationale:**
- ✅ Native GitHub integration
- ✅ Free for public repos
- ✅ Generous free tier (2000 min/month)
- ✅ Simple YAML configuration
- ✅ No infrastructure to maintain
- ❌ Weaker than Jenkins for complex workflows
- ❌ Longer execution times than self-hosted

**Comparison:**
```
GitHub Actions      Jenkins              GitLab CI
- ✅ Easy setup    - ✅ Most powerful   - ✅ Good UX
- ✅ Low ops       - ❌ Complex setup   - ✅ Simpler than Jenkins
- ✅ Free          - ❌ Self-hosted     - ✅ Managed option
- Limited          - Unlimited         - Good limits
```

## Data Model

### Cross-DAG Dependency Table

```sql
CREATE TABLE cross_dag_dependency (
    id INT PRIMARY KEY,
    source_dag_id VARCHAR(250),      -- Must complete first
    dependent_dag_id VARCHAR(250),   -- Waits for source
    dependency_type ENUM,            -- DAG_TO_DAG, TASK_TO_DAG
    timeout_seconds INT,             -- Max wait time
    skip_on_failure BOOLEAN,         -- Continue if source fails?
    created_at DATETIME,
    created_by VARCHAR(250),         -- Audit trail
    is_active BOOLEAN
);
```

**Key Design Choices:**
- Separate source/dependent instead of edges list
  - Simpler queries
  - Better for visualization
  - Easier to understand business logic

- Metadata JSON column for extensibility
  - Allows adding attributes without schema change
  - Future-proof design

### Dependency Execution Table

```sql
CREATE TABLE dependency_execution (
    id INT PRIMARY KEY,
    dependency_id INT FOREIGN KEY,   -- Link to definition
    source_dag_run_id VARCHAR(250),
    dependent_dag_run_id VARCHAR(250),
    status ENUM (PENDING, SUCCESS, FAILED, TIMEOUT),
    check_started_at DATETIME,
    check_completed_at DATETIME,
    duration_seconds INT,            -- For metrics
);
```

**Key Design Choices:**
- Separate execution from definition
  - Allows tracking individual checks
  - Historical analysis
  - Performance metrics

- Stores run IDs, not just DAG IDs
  - Critical for DAG re-runs
  - Correct dependency versioning

### Audit Log Table

```sql
CREATE TABLE dependency_audit_log (
    id INT PRIMARY KEY,
    operation_type VARCHAR(50),      -- CREATE, UPDATE, DELETE, CHECK
    dependency_id INT,
    user_id VARCHAR(250),
    timestamp DATETIME,
    changes_json TEXT,               -- What changed
    source_system VARCHAR(100),      -- UI, API, CLI, Airflow
);
```

**Key Design Choices:**
- JSON changes column
  - Flexible: Stores any structured change
  - Searchable: Can query specific fields
  - Human-readable: JSON is standard

- Timestamp for every change
  - Enables compliance audits
  - Troubleshooting timeline

## API Design

### REST API Endpoints

```
POST   /api/v1/dependencies              # Create dependency
GET    /api/v1/dependencies              # List all
GET    /api/v1/dependencies/{id}         # Get specific
PUT    /api/v1/dependencies/{id}         # Update
DELETE /api/v1/dependencies/{id}         # Delete

GET    /api/v1/dependencies/graph        # Visualization
POST   /api/v1/dependencies/validate     # Check for issues
GET    /api/v1/dependencies/audit-log    # Compliance data
```

## Scalability Considerations

### Current Scale: 100 DAGs, 500 dependencies

**Bottlenecks:**
- Sensor poke interval: ~1 query/60 seconds per dependency
- Circular detection: O(V+E) on each registration
- Audit log storage: ~1KB per operation

### Scale to 1000 DAGs

**Required Changes:**
- Index optimization on dependency tables
- Batch poke checks (poke multiple dependencies at once)
- Archive old audit logs quarterly

### Scale to 10000 DAGs

**Required Architecture Changes:**
- Separate dependency service (microservice)
- Event-driven architecture instead of polling
- Distributed circular detection
- Time-series database for metrics (InfluxDB)
- Cache layer (Redis) for frequent queries

## Security Model

### Authentication
- Airflow RBAC: Leverages existing Airflow authentication
- API: Token-based or OAuth2

### Authorization
- Role-based: Admin, Editor, Viewer roles
- Approval workflow: Critical dependencies require approval
- RBAC on audit logs

### Data Protection
- Encrypted at rest: RDS encryption
- Encrypted in transit: TLS for all APIs
- Secrets management: Kubernetes secrets
- PII handling: Mask sensitive DAG parameters in logs

## Deployment Strategy

### Blue-Green Deployment

```
Blue (v1.0)          Green (v1.1)
- Running            - Ready to receive traffic
- All traffic here   - Tested in parallel
                     - Switch when ready
                     - Instant rollback if needed
```

### Canary Deployment

```
1. Deploy to 1% of nodes (canary)
2. Monitor metrics
3. Gradually increase (5%, 25%, 50%, 100%)
4. Automatic rollback if errors detected
```

## Monitoring & Observability

### Golden Signals

1. **Latency**: Time to check dependency
   - P50: < 100ms
   - P99: < 1s

2. **Traffic**: Dependency checks per minute
   - Baseline: ~10 checks/min per dependency
   - Peak: ~100 checks/min during DAG runs

3. **Errors**: Failed dependency checks
   - Target: < 0.1% failure rate
   - Alert if > 1% failures

4. **Saturation**: Resource utilization
   - Database connection pool: < 80%
   - Memory: < 80%
   - CPU: < 70%

## Disaster Recovery

### RTO (Recovery Time Objective): 15 minutes
### RPO (Recovery Point Objective): 5 minutes

**Strategy:**
1. Cross-region RDS replica
2. Weekly snapshots
3. Kubernetes backup (Velero)
4. Tested failover monthly

## Summary: Architecture Decisions

| Decision | Choice | Rationale | Tradeoff |
|----------|--------|-----------|----------|
| Extension approach | Plugin/Sensor | Non-invasive, upgradable | Slight overhead |
| Dependency detection | Polling sensor | Reliable, debuggable | Latency, DB load |
| Storage | PostgreSQL | Dynamic, auditable | GitOps gap |
| Cycle detection | DFS | O(V+E), simple | Complexity at scale |
| Deployment | Kubernetes | Portable, standard | Operational overhead |
| Database | Aurora | HA, scaling | Higher cost |
| Metrics | Prometheus | Open source | Need to operate |
| CI/CD | GitHub Actions | Native, free | Less powerful |

Each decision was made with specific production requirements in mind. The architecture can be evolved as the platform scales.
