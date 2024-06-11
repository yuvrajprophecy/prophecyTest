from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_3754(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def calculate(iterator):
        weight_field_lag1 = 0.0

        for row in iterator:
            weight_field_new = int(weight_field_lag1) + 1
            weight_field_lag1 = weight_field_new
            newRow = list(row)
            newRow.append(weight_field_new)
            yield newRow

    resultRDD = in0\
        .repartition(col("MBR_INDV_BE_KEY"), col("RecordNum"))\
        .sortWithinPartitions(col("MBR_INDV_BE_KEY"), col("RecordNum"))\
        .rdd\
        .mapPartitions(
        calculate
    )
    newSchema = StructType([field for field in in0.schema.fields if field.name != "weight field"])
    newSchema.add("weight field", DoubleType())
    out0 = spark.createDataFrame(resultRDD, newSchema)

    return out0
