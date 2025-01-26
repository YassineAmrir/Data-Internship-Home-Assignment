from datetime import datetime, timedelta  # Added timedelta import
import os
import json
import sqlite3
import pandas as pd
import re
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# SQL Queries for creating tables
TABLES_CREATION_QUERY_1 = """
CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);
"""

TABLES_CREATION_QUERY_2 = """
CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);
"""

TABLES_CREATION_QUERY_3 = """
CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);
"""

TABLES_CREATION_QUERY_4 = """
CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);
"""

TABLES_CREATION_QUERY_5 = """
CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);
"""

TABLES_CREATION_QUERY_6 = """
CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
);
"""

def extract_context_data(**kwargs):
    file_path = '/opt/airflow/dags/source/jobs.csv'

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)

    # Check if the 'context' column is present and has valid data
    if 'context' not in df.columns or df['context'].isnull().all():
        print("No valid 'context' data found in the file.")
        return

    context_data = df['context'].dropna().tolist()

    print("Data extracted successfully.")
    output_dir = '/opt/airflow/dags/staging/extracted'

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for i, context in enumerate(context_data):
        output_file = os.path.join(output_dir, f'context_{i+1}.txt')
        with open(output_file, 'w') as f:
            f.write(context)
        print(f"Context {i+1} written to {output_file}")

def transform_data(**kwargs):
    extracted_dir = '/opt/airflow/dags/staging/extracted'
    transformed_dir = '/opt/airflow/dags/staging/transformed'

    if not os.path.exists(transformed_dir):
        os.makedirs(transformed_dir)

    keywords = {
        "job": {
            "title": "job title",
            "industry": "industry",
            "description": "description",
            "employment_type": "employment type",
            "date_posted": "date posted"
        },
        "company": {
            "name": "company name",
            "link": "company link"
        },
        "education": {
            "required_credential": "education requirements"
        },
        "experience": {
            "months_of_experience": "months of experience",
            "seniority_level": "seniority level"
        },
        "salary": {
            "currency": "currency",
            "min_value": "minimum salary",
            "max_value": "maximum salary",
            "unit": "unit"
        },
        "location": {
            "country": "country",
            "locality": "locality",
            "region": "region",
            "postal_code": "postal code",
            "street_address": "street address",
            "latitude": "latitude",
            "longitude": "longitude"
        }
    }

    def search_keywords(text, keyword):
        """Search for a keyword in the text and extract the associated value."""
        match = re.search(rf'({keyword})\s*[:\-=\s]*([^\n]+)', text, re.IGNORECASE)
        if match:
            return match.group(2).strip()
        return ""

    for i, file_name in enumerate(os.listdir(extracted_dir)):
        if file_name.endswith(".txt"):
            file_path = os.path.join(extracted_dir, file_name)
            with open(file_path, 'r', encoding='utf-8') as file:
                try:
                    raw_data = file.read()
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")
                    continue

            transformed_data = {
                "job": {
                    "title": search_keywords(raw_data, keywords["job"]["title"]),
                    "industry": search_keywords(raw_data, keywords["job"]["industry"]),
                    "description": search_keywords(raw_data, keywords["job"]["description"]),
                    "employment_type": search_keywords(raw_data, keywords["job"]["employment_type"]),
                    "date_posted": search_keywords(raw_data, keywords["job"]["date_posted"]),
                },
                "company": {
                    "name": search_keywords(raw_data, keywords["company"]["name"]),
                    "link": search_keywords(raw_data, keywords["company"]["link"]),
                },
                "education": {
                    "required_credential": search_keywords(raw_data, keywords["education"]["required_credential"]),
                },
                "experience": {
                    "months_of_experience": search_keywords(raw_data, keywords["experience"]["months_of_experience"]),
                    "seniority_level": search_keywords(raw_data, keywords["experience"]["seniority_level"]),
                },
                "salary": {
                    "currency": search_keywords(raw_data, keywords["salary"]["currency"]),
                    "min_value": search_keywords(raw_data, keywords["salary"]["min_value"]),
                    "max_value": search_keywords(raw_data, keywords["salary"]["max_value"]),
                    "unit": search_keywords(raw_data, keywords["salary"]["unit"]),
                },
                "location": {
                    "country": search_keywords(raw_data, keywords["location"]["country"]),
                    "locality": search_keywords(raw_data, keywords["location"]["locality"]),
                    "region": search_keywords(raw_data, keywords["location"]["region"]),
                    "postal_code": search_keywords(raw_data, keywords["location"]["postal_code"]),
                    "street_address": search_keywords(raw_data, keywords["location"]["street_address"]),
                    "latitude": search_keywords(raw_data, keywords["location"]["latitude"]),
                    "longitude": search_keywords(raw_data, keywords["location"]["longitude"]),
                },
            }

            print(f"Transformed data for {file_name}: {json.dumps(transformed_data, indent=2)}")

            output_file_path = os.path.join(transformed_dir, file_name.replace(".txt", ".json"))
            with open(output_file_path, 'w', encoding='utf-8') as output_file:
                json.dump(transformed_data, output_file, indent=4)

            print(f"Processed and saved: {output_file_path}")

def load_to_database(**kwargs):
    transformed_dir = '/opt/airflow/dags/staging/transformed'
    db_path = '/opt/airflow/airflow.db'

    if not os.path.exists(transformed_dir):
        raise FileNotFoundError(f"Transformed directory not found: {transformed_dir}")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for file_name in os.listdir(transformed_dir):
        if file_name.endswith('.json'):
            file_path = os.path.join(transformed_dir, file_name)
            with open(file_path, 'r') as f:
                transformed_data = json.load(f)

            job_data = transformed_data.get('job', {})
            cursor.execute("""
                INSERT INTO job (title, industry, description, employment_type, date_posted)
                VALUES (?, ?, ?, ?, ?)
            """, (
                job_data.get('title', ''),
                job_data.get('industry', ''),
                job_data.get('description', ''),
                job_data.get('employment_type', ''),
                job_data.get('date_posted', '')
            ))
            job_id = cursor.lastrowid

            company_data = transformed_data.get('company', {})
            cursor.execute("""
                INSERT INTO company (job_id, name, link)
                VALUES (?, ?, ?)
            """, (
                job_id,
                company_data.get('name', ''),
                company_data.get('link', '')
            ))

            education_data = transformed_data.get('education', {})
            cursor.execute("""
                INSERT INTO education (job_id, required_credential)
                VALUES (?, ?)
            """, (
                job_id,
                education_data.get('required_credential', '')
            ))

            experience_data = transformed_data.get('experience', {})
            cursor.execute("""
                INSERT INTO experience (job_id, months_of_experience, seniority_level)
                VALUES (?, ?, ?)
            """, (
                job_id,
                experience_data.get('months_of_experience', ''),
                experience_data.get('seniority_level', '')
            ))

            salary_data = transformed_data.get('salary', {})
            cursor.execute("""
                INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                VALUES (?, ?, ?, ?, ?)
            """, (
                job_id,
                salary_data.get('currency', ''),
                salary_data.get('min_value', ''),
                salary_data.get('max_value', ''),
                salary_data.get('unit', '')
            ))

            location_data = transformed_data.get('location', {})
            cursor.execute("""
                INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job_id,
                location_data.get('country', ''),
                location_data.get('locality', ''),
                location_data.get('region', ''),
                location_data.get('postal_code', ''),
                location_data.get('street_address', ''),
                location_data.get('latitude', ''),
                location_data.get('longitude', '')
            ))

            conn.commit()
    conn.close()
    print("Data loaded successfully into the database.")

# Airflow DAG setup
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('job_extraction_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    create_table_tasks = [
        SqliteOperator(
            task_id=f'create_table_{i+1}',
            sqlite_conn_id='sqlite_default',
            sql=query
        ) for i, query in enumerate([
            TABLES_CREATION_QUERY_1,
            TABLES_CREATION_QUERY_2,
            TABLES_CREATION_QUERY_3,
            TABLES_CREATION_QUERY_4,
            TABLES_CREATION_QUERY_5,
            TABLES_CREATION_QUERY_6
        ])
    ]
    
    extract_task = PythonOperator(
        task_id='extract_context_data',
        python_callable=extract_context_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database
    )

    end_task = EmptyOperator(task_id='end_task')

    create_table_tasks >> extract_task >> transform_task >> load_task >> end_task
