from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Createfieldwithrelationshipsofpreviouslymadefield(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def calculate(iterator):
        Product_Association_lag1 = 0.0

        for row in iterator:
            MBR_RELSHP_CD = row["MBR_RELSHP_CD"]
            Product_Association_new = concat(concat(Product_Association_lag1, ""), MBR_RELSHP_CD)
            Product_Association_lag1 = Product_Association_new
            newRow = list(row)
            newRow.append(Product_Association_new)
            yield newRow

    resultRDD = in0.repartition(col("SUB_SK")).sortWithinPartitions(col("SUB_SK")).rdd.mapPartitions(calculate)
    newSchema = StructType([field for field in in0.schema.fields if field.name != "Product Association"])
    newSchema.add("Product Association", DoubleType())
    out0 = spark.createDataFrame(resultRDD, newSchema)

    return out0
