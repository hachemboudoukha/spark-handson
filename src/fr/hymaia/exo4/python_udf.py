import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import time

spark = SparkSession.builder.appName("exo4_python_udf").master("local[*]").getOrCreate()

@f.udf(returnType=StringType())
def add_catogery_name(category):
    if category < "6":
        return "food"
    else:
        return "fourniture"

def main():

    start_read = time.time()
    df = spark.read.option('header', True).csv("src/resources/exo4/sell.csv")

    count_initial = df.count()
    end_read = time.time()
    print(f"Temps de lecture: {end_read - start_read:.3f} secondes")

    start_udf = time.time()
    resultat_python = df.withColumn("category_name", add_catogery_name(f.col("category")))

    count_result = resultat_python.count()
    end_udf = time.time()
    duree_udf = end_udf - start_udf
    print(f"Durée de la transformation UDF: {duree_udf:.3f} secondes")
    

    resultat_python.show(10)
    

    start_write = time.time()
    resultat_python.write.mode("overwrite").parquet("output/exo4/python_udf")
    end_write = time.time()
    duree_write = end_write - start_write
    print(f"Durée d'écriture: {duree_write:.3f} secondes")
    
    print(f"Lecture:        {end_read - start_read:.3f} secondes")
    print(f"Transformation: {duree_udf:.3f} secondes")
    print(f"Écriture:       {duree_write:.3f} secondes")
    print(f"Total:          {(end_read - start_read) + duree_udf + duree_write:.3f} secondes")
    
if __name__ == "__main__":
    main()