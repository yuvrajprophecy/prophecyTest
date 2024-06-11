from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_2341(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def calculate(iterator):
        target_lag1 = 0.0

        for row in iterator:
            target = row["target"]
            target_new = target_lag1 + target
            target_lag1 = target_new
            newRow = list(row)
            newRow[row.__fields__.index("target")] = target_new
            yield newRow

    resultRDD = in0.repartition(col("MBR_SK")).sortWithinPartitions(col("MBR_SK")).rdd.mapPartitions(calculate)
    out0 = spark.createDataFrame(resultRDD, in0.schema)

    return out0
