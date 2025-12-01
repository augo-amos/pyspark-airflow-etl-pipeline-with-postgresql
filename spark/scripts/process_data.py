import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
import pandas as pd

def create_spark_session():
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName("CustomerDataProcessing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def process_customer_data(spark, input_path):
    """Process customer data using PySpark"""
    
    print("Reading data from:", input_path)
    
    # Read CSV data with explicit schema
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("marital_status", StringType(), True),
        StructField("income_level", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("amount_spent", DoubleType(), True),
        StructField("product_category", StringType(), True)
    ])
    
    df = spark.read.option("header", "true").schema(schema).csv(input_path)
    
    print(f"Initial record count: {df.count()}")
    df.show(5)
    
    # Convert data types
    df = df.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
    
    # Customer-level aggregations
    customer_summary = df.groupBy("customer_id", "age", "gender", "marital_status", "income_level") \
        .agg(
            sum("amount_spent").alias("total_spent"),
            count("*").alias("transaction_count"),
            avg("amount_spent").alias("avg_transaction"),
            max("transaction_date").alias("last_transaction_date")
        )
    
    print("Customer summary:")
    customer_summary.show()
    
    # Product category aggregations
    product_stats = df.groupBy("product_category") \
        .agg(
            sum("amount_spent").alias("total_revenue"),
            count("*").alias("transaction_count"),
            avg("amount_spent").alias("avg_transaction_amount")
        )
    
    print("Product stats:")
    product_stats.show()
    
    # Convert to pandas for easier PostgreSQL insertion
    customer_pandas = customer_summary.toPandas()
    product_pandas = product_stats.toPandas()
    
    return customer_pandas, product_pandas

def load_to_postgres(customer_data, product_data):
    """Load processed data into PostgreSQL"""
    
    try:
        connection = psycopg2.connect(
            host="postgres",
            database="customer_analytics",
            user="airflow",
            password="airflow",
            port="5432"
        )
        
        cursor = connection.cursor()
        
        # Load customer summary
        for _, row in customer_data.iterrows():
            cursor.execute("""
                INSERT INTO customer_summary 
                (customer_id, total_spent, transaction_count, avg_transaction, last_transaction_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) 
                DO UPDATE SET 
                    total_spent = EXCLUDED.total_spent,
                    transaction_count = EXCLUDED.transaction_count,
                    avg_transaction = EXCLUDED.avg_transaction,
                    last_transaction_date = EXCLUDED.last_transaction_date
            """, (
                int(row['customer_id']), 
                float(row['total_spent']), 
                int(row['transaction_count']), 
                float(row['avg_transaction']), 
                row['last_transaction_date'].strftime('%Y-%m-%d') if pd.notna(row['last_transaction_date']) else None
            ))
        
        # Load product stats
        for _, row in product_data.iterrows():
            cursor.execute("""
                INSERT INTO product_category_stats 
                (product_category, total_revenue, transaction_count, avg_transaction_amount)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (product_category) 
                DO UPDATE SET 
                    total_revenue = EXCLUDED.total_revenue,
                    transaction_count = EXCLUDED.transaction_count,
                    avg_transaction_amount = EXCLUDED.avg_transaction_amount
            """, (
                str(row['product_category']), 
                float(row['total_revenue']), 
                int(row['transaction_count']), 
                float(row['avg_transaction_amount'])
            ))
        
        connection.commit()
        print("Data successfully loaded into PostgreSQL")
        
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
        if 'connection' in locals():
            connection.rollback()
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def main():
    if len(sys.argv) != 2:
        print("Usage: process_data.py <input_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    print(f"Processing data from: {input_path}")
    
    # Check if file exists
    if not os.path.exists(input_path):
        print(f"Error: Input file {input_path} does not exist")
        sys.exit(1)
    
    # Initialize Spark
    spark = create_spark_session()
    print("Spark session created successfully")
    
    try:
        # Process data
        print("Processing customer data with PySpark...")
        customer_data, product_data = process_customer_data(spark, input_path)
        
        print(f"Processed {len(customer_data)} customer records")
        print(f"Processed {len(product_data)} product categories")
        
        # Load to PostgreSQL
        print("Loading data into PostgreSQL...")
        load_to_postgres(customer_data, product_data)
        
        print("Data processing completed successfully!")
        
    except Exception as e:
        print(f"Error in data processing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    main()