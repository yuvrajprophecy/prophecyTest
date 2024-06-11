from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_1907(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def calculate(iterator):
        Counter_lag1 = 0.0

        for row in iterator:
            Counter_new = int(Counter_lag1) + 1
            Counter_lag1 = Counter_new
            newRow = list(row)
            newRow.append(Counter_new)
            yield newRow

    resultRDD = in0.repartition(col("MBR_INDV_BE_KEY")).sortWithinPartitions(col("MBR_INDV_BE_KEY")).rdd.mapPartitions(
        calculate
    )
    newSchema = StructType([field for field in in0.schema.fields if field.name != "Counter"])
    newSchema.add("Counter", DoubleType())
    out0 = spark.createDataFrame(resultRDD, newSchema)

    return out0
