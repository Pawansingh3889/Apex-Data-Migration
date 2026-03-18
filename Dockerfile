FROM python:3.11-slim

WORKDIR /apex

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# --- Copy and install your requirements before copying the rest of your files ---
COPY requirements.txt /apex/requirements.txt
RUN pip install --no-cache-dir \
    dbt-duckdb==1.10.1 \
    polars \
    pyarrow \
    scikit-learn \
    xgboost \
    jupyter \
    papermill \
    prefect==3.4.0 \
    pandas \
    matplotlib \
    seaborn \
    requests \
    ipykernel \
    ollama

RUN python -m ipykernel install --user --name python3 --display-name "Python 3"

COPY . .

ENV PYTHONPATH=/apex
ENV DILLONAI_HOST=[host.docker.internal](http://host.docker.internal:11434)

CMD ["python", "airflow_orchestration/apex_flow.py"]
