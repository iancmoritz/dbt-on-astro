FROM quay.io/astronomer/astro-runtime:11.7.0

# install dbt into a virtual environment
# here's a dockerfile change
RUN pip install https://github.com/iancmoritz/astronomer-cosmos/archive/main.zip
RUN python -m venv dbt_venv_postgres && source dbt_venv_postgres/bin/activate && \
    pip install --no-cache-dir dbt-postgres>=1.8.0 && deactivate