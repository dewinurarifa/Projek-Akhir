from pyspark.sql import SparkSession

def read_and_save_data():
    spark = SparkSession.builder \
        .appName("PostgreSQL Spark Integration") \
        .master("local[*]")\
        .config("spark.jars", "/home/arifa/lib/postgresql-42.2.24.jar") \
        .getOrCreate()

    # PostgreSQL connection properties
    url = "jdbc:postgresql://localhost:5432/postgres"
    properties = {
        "user": "postgres",
        "password": "arifa",
        "driver": "org.postgresql.Driver"
    }

    # Load data from PostgreSQL tables
    df_output_t_co_d = spark.read.jdbc(url=url, table="output_t_co_d", properties=properties)
    df_output_t_so_d = spark.read.jdbc(url=url, table="output_t_so_d", properties=properties)
    df_output_t_invoice_d = spark.read.jdbc(url=url, table="output_t_invoice_d", properties=properties)
    df_output_t_invoice_h = spark.read.jdbc(url=url, table="output_t_invoice_h", properties=properties)

    # Save dataframes to postgres as Parquet files
    df_output_t_co_d.write.mode("overwrite").parquet("output_t_co_d.parquet")
    df_output_t_so_d.write.mode("overwrite").parquet("output_t_so_d.parquet")
    df_output_t_invoice_d.write.mode("overwrite").parquet("output_t_invoice_d.parquet")
    df_output_t_invoice_h.write.mode("overwrite").parquet("output_t_invoice_h.parquet")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    read_and_save_data()
