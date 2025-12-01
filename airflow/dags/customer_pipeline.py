from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import subprocess
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def run_spark_job():
    """Execute the PySpark job"""
    try:
        # Using Python with PySpark directly
        result = subprocess.run([
            'python',
            '/opt/airflow/scripts/process_data.py',
            '/opt/airflow/data/customers.csv'
        ], capture_output=True, text=True, cwd='/opt/airflow/scripts')
        
        print("STDOUT:", result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
            
        if result.returncode == 0:
            print("PySpark job completed successfully!")
        else:
            print(f"PySpark job failed with return code: {result.returncode}")
            raise Exception("PySpark job execution failed")
            
    except Exception as e:
        print(f"Error running PySpark job: {e}")
        raise

def verify_postgres_data():
    """Verify data was loaded into PostgreSQL"""
    import psycopg2
    from psycopg2 import OperationalError
    
    try:
        connection = psycopg2.connect(
            host="postgres",
            database="customer_analytics",
            user="airflow",
            password="airflow",
            port="5432"
        )
        
        cursor = connection.cursor()
        
        # Check customer data
        cursor.execute("SELECT COUNT(*) FROM customer_summary")
        customer_count = cursor.fetchone()[0]
        print(f"Customer records in database: {customer_count}")
        
        # Check product data
        cursor.execute("SELECT COUNT(*) FROM product_category_stats")
        product_count = cursor.fetchone()[0]
        print(f"Product category records in database: {product_count}")
        
        # Show sample data
        cursor.execute("SELECT * FROM customer_summary ORDER BY customer_id LIMIT 5")
        sample_customers = cursor.fetchall()
        print("Sample customer data:")
        for row in sample_customers:
            print(f"  Customer {row[0]}: ${row[1]:.2f} total, {row[2]} transactions")
        
        cursor.close()
        connection.close()
        
        if customer_count > 0 and product_count > 0:
            print("Data verification successful!")
        else:
            raise Exception("No data found in database")
            
    except OperationalError as e:
        print(f"Database connection error: {e}")
        raise
    except Exception as e:
        print(f"Error verifying data: {e}")
        raise

with DAG(
    'customer_data_pipeline',
    default_args=default_args,
    description='PySpark data pipeline for customer data',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['pyspark', 'postgres', 'etl'],
) as dag:

    start = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "Starting PySpark customer data pipeline at $(date)"',
    )

    process_data = PythonOperator(
        task_id='process_data_with_spark',
        python_callable=run_spark_job,
    )

    verify_data = PythonOperator(
        task_id='verify_postgres_data',
        python_callable=verify_postgres_data,
    )

    end = BashOperator(
        task_id='end_pipeline',
        bash_command='echo "PySpark pipeline completed successfully at $(date)"',
    )

    start >> process_data >> verify_data >> end