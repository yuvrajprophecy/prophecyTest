from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_4354(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def calculate(iterator):
        for row in iterator:
            Target_Forecasted_lead1 = row["Target Forecasted_lead1"]
            Target_Forecasted_lead2 = row["Target Forecasted_lead2"]
            Target_Forecasted_lead3 = row["Target Forecasted_lead3"]
            Target_Forecasted = row["Target Forecasted"]
            Target_Forecasted_new = 2 if ((not Target_Forecasted == None) and (int(Target_Forecasted) == 2)) else 1 if (
                (
                  not ((Target_Forecasted_lead1 == None) or (Target_Forecasted_lead2 == None))
                  or (Target_Forecasted_lead3 == None)
                )
                and (
                  (
                    (int(Target_Forecasted_lead1) == 2)
                    or (int(Target_Forecasted_lead2) == 2)
                  )
                  or (int(Target_Forecasted_lead3) == 2)
                )
            ) else Target_Forecasted
            newRow = list(row)
            newRow[row.__fields__.index("Target Forecasted")] = Target_Forecasted_new
            yield newRow

    resultRDD = in0.repartition(col("MBR_INDV_BE_KEY")).sortWithinPartitions(col("MBR_INDV_BE_KEY")).rdd.mapPartitions(
        calculate
    )
    out0 = spark.createDataFrame(resultRDD, in0.schema)

    return out0
