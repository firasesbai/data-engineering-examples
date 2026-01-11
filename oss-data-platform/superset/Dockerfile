FROM apache/superset:3.1.2

USER root

# Install DuckDB engine
RUN pip install --no-cache-dir \
    duckdb-engine \
    duckdb==0.10.2

USER superset
