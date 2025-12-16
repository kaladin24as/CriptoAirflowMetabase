FROM apache/airflow:2.10.2

USER root
# Install git for dbt packages if needed
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt and specific adapter
RUN pip install --no-cache-dir \
    dbt-core \
    dbt-postgres \
    requests \
    pandas \
    