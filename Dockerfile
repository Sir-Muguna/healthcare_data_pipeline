FROM quay.io/astronomer/astro-runtime:12.1.1

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate

RUN pip install dbt-core==1.8.8 dbt-bigquery==1.8.3

