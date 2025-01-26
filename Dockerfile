FROM apache/airflow:2.7.2

USER airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
