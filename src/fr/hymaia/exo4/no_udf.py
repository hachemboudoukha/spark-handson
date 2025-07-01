import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import time

def main():
    start = time.time()
    spark = SparkSession.builder.appName("exo4_python_udf").master("local[*]").getOrCreate()
    df = spark.read.option('header', True).csv("src/resources/exo4/sell.csv")

    # Ensure the date column is in the correct format
    df = df.withColumn("date", f.to_date("date", "yyyy-MM-dd"))

    # Add category_name column
    df = df.withColumn("category_name", f.when(f.col("category") < "6", "food").otherwise("furniture"))

    # Define a window partitioned by category and date
    daily_window = Window.partitionBy("category", "date")

    # Calculate total_price_per_category_per_day
    df = df.withColumn("total_price_per_category_per_day", f.sum("price").over(daily_window))

    # Convert the date column to a Unix timestamp
    df = df.withColumn("date_unix", f.unix_timestamp("date"))

    # Define the rolling window with the Unix timestamp
    rolling_window = Window.partitionBy("category").orderBy("date_unix").rangeBetween(-2592000, 0)

    # Apply the window function
    df = df.withColumn("total_price_per_category_per_day_last_30_days", f.sum("price").over(rolling_window))

    # Write the result to a parquet file
    df.show()
    # df.write.mode("overwrite").parquet("output/exo4/no_udf")
    end = time.time()
    print(f"Temps d'exécution : {end - start:.2f} secondes")

if __name__ == "__main__":
    main()