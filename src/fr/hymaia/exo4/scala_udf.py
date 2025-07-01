from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time

def addCategoryName(spark, col):
    sc = spark.sparkContext
    scala_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(scala_udf.apply(_to_seq(sc, [col], _to_java_column)))

def main():
    spark = SparkSession.builder \
        .appName("Scala UDF") \
        .master("local[*]") \
        .config("spark.jars", "src/resources/exo4/udf.jar") \
        .getOrCreate()

    start_read = time.time()
    df = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    count_initial = df.count()
    end_read = time.time()
    print(f"Temps de lecture: {end_read - start_read:.3f} secondes")

    start_udf = time.time()
    df_udf = df.withColumn("category_name", addCategoryName(spark, df["category"])) 
    count_result = df_udf.count()
    end_udf = time.time()
    print(f"Durée de la transformation UDF Scala: {end_udf - start_udf:.3f} secondes")

    df_udf.show(10)

    start_write = time.time()
    df_udf.write.mode("overwrite").parquet("output/exo4/scala_udf")
    end_write = time.time()
    print(f"Durée d'écriture: {end_write - start_write:.3f} secondes")

    print("\n--- Résumé ---")
    print(f"Lecture:        {end_read - start_read:.3f} secondes")
    print(f"Transformation: {end_udf - start_udf:.3f} secondes")
    print(f"Écriture:       {end_write - start_write:.3f} secondes")
    print(f"Total:          {(end_read - start_read) + (end_udf - start_udf) + (end_write - start_write):.3f} secondes")

    spark.stop()

if __name__ == "__main__":
    main()