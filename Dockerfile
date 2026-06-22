FROM apache/airflow:2.7.0-python3.11

USER root

# Install additional dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy plugins
COPY plugins/ ${AIRFLOW_HOME}/plugins/

# Copy DAGs
COPY dags/ ${AIRFLOW_HOME}/dags/

# Copy migrations
COPY migrations/ ${AIRFLOW_HOME}/migrations/

# Install the package
COPY --chown=airflow:0 . /tmp/airflow-enterprise
RUN cd /tmp/airflow-enterprise && pip install .

EXPOSE 8080

CMD ["airflow", "webserver"]
