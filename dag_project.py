from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from airflow.utils.dates import days_ago
from pyspark.sql.functions import col, sum, mean, desc, count

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'penjualan_barang',
    default_args=default_args,
    description='A sample DAG for reading and transforming data',
    schedule_interval='@daily',
)

def read_and_save_data():
    from spark_project import read_and_save_data
    read_and_save_data()

def transform_data():
    spark = SparkSession.builder \
        .appName("PostgreSQL Spark Integration") \
        .master("local[*]")\
        .config("spark.jars", "/home/arifa/lib/postgresql-42.2.24.jar") \
        .getOrCreate()

    # Load data from HDFS or PostgreSQL
    df_output_t_co_d = spark.read.parquet("output_t_co_d.parquet")
    df_output_t_so_d = spark.read.parquet("output_t_so_d.parquet")
    df_output_t_invoice_d = spark.read.parquet("output_t_invoice_d.parquet")
    df_output_t_invoice_h = spark.read.parquet("output_t_invoice_h.parquet")

    # Perform transformations
     # Transformation for df_output_t_invoice_d
    total_sales_per_product = df_output_t_invoice_d.groupBy("barang_id_1") \
        .agg(sum("harga_barang").alias("total_sales"), mean("harga_barang").alias("average_price"))

    sorted_sales_per_product = total_sales_per_product.sort(desc("total_sales"))
    sorted_sales_per_product.show()

    # Transformation for df_output_t_invoice_h
    total_invoices_per_customer = df_output_t_invoice_h.groupBy("pelanggan_id") \
        .agg(sum("nilai_inv").alias("total_invoice_amount"), count("id").alias("number_of_invoices"))

    sorted_invoices_per_customer = total_invoices_per_customer.sort(desc("total_invoice_amount"))
    sorted_invoices_per_customer.show()

    # Transformation for df_output_t_so_d
    total_qty_per_product_so = df_output_t_so_d.groupBy("barang_id") \
        .agg(sum("qty").alias("total_qty_sold"))

    sorted_qty_per_product_so = total_qty_per_product_so.sort(desc("total_qty_sold"))
    sorted_qty_per_product_so.show()

    # Transformation for df_output_t_co_d
    total_qty_per_product_co = df_output_t_co_d.groupBy("barang_id_1") \
        .agg(sum("qly").alias("total_qty_sold"))

    sorted_qty_per_product_co = total_qty_per_product_co.sort(desc("total_qty_sold"))
    sorted_qty_per_product_co.show()

    url = "jdbc:postgresql://localhost:5432/postgres"
    properties = {
        "user": "postgres",
        "password": "arifa",
        "driver": "org.postgresql.Driver"
    }
    # Stop the Spark session
    spark.stop()

read_and_save_data = PythonOperator(
    task_id='read_and_save_data',
    python_callable=read_and_save_data,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

read_and_save_data >> transform_data
