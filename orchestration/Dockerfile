FROM apache/airflow:3.3.2-python3.12

USER airflow
WORKDIR /opt/airflow/project
COPY --chown=airflow:root . /opt/airflow/project
RUN python -m venv /opt/airflow/telemetry-venv
RUN /opt/airflow/telemetry-venv/bin/pip install --no-cache-dir -r requirements.lock
RUN /opt/airflow/telemetry-venv/bin/pip install --no-deps --no-build-isolation -e /opt/airflow/project
RUN mkdir -p /opt/airflow/telemetry/landing /opt/airflow/telemetry/state
ENV TELEMETRY_PYTHON=/opt/airflow/telemetry-venv/bin/python
ENV PYTHONPATH=/opt/airflow/project
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/project/orchestration/dags
