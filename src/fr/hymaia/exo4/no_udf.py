from pyspark.sql import SparkSession
from pyspark.sql.functions import when, to_date
import time

def main():
    spark = SparkSession.builder \
        .appName("Spark Native Without Window") \
        .master("local[*]") \
        .getOrCreate()

    start_read = time.time()
    df = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    count_initial = df.count()
    end_read = time.time()
    print(f"Temps de lecture: {end_read - start_read:.3f} secondes")

    start_transfo = time.time()

    df = df.withColumn("category", df["category"].cast("int"))
    df = df.withColumn("price", df["price"].cast("double"))
    df = df.withColumn("date", to_date(df["date"], "yyyy-MM-dd"))
    df = df.withColumn(
        "category_name",
        when(df["category"] < 6, "food").otherwise("furniture")
    )

    count_transfo = df.count()
    end_transfo = time.time()
    print(f"Durée de la transformation (sans window) : {end_transfo - start_transfo:.3f} secondes")

    df.show(10)

    start_write = time.time()
    df.write.mode("overwrite").parquet("output/exo4/native_no_window")
    end_write = time.time()
    print(f"Durée d'écriture: {end_write - start_write:.3f} secondes")

    print("\n--- Résumé ---")
    print(f"Lecture:        {end_read - start_read:.3f} secondes")
    print(f"Transformation: {end_transfo - start_transfo:.3f} secondes")
    print(f"Écriture:       {end_write - start_write:.3f} secondes")
    print(f"Total:          {(end_read - start_read) + (end_transfo - start_transfo) + (end_write - start_write):.3f} secondes")

    spark.stop()

if __name__ == "__main__":
    main()
