from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_2527(spark: SparkSession, in0: DataFrame) -> DataFrame:
    

    def calculate(iterator):
        MBR_INDV_BE_KEY_lag1 = 0.0
        Spouses_Product_lag1 = 0.0

        for row in iterator:
            MBR_INDV_BE_KEY = row["MBR_INDV_BE_KEY"]
            Spouses_Product = row["Spouse's Product"]
            Spouses_Product_new = (
                int(Spouses_Product_lag1)
                + 0
            ) if (
                (
                  not (MBR_INDV_BE_KEY_lag1 == None)
                  or (MBR_INDV_BE_KEY == None)
                )
                and (MBR_INDV_BE_KEY_lag1 == MBR_INDV_BE_KEY)
            ) else Spouses_Product
            MBR_INDV_BE_KEY_lag1 = MBR_INDV_BE_KEY
            Spouses_Product_lag1 = Spouses_Product_new
            newRow = list(row)
            newRow[row.__fields__.index("Spouse's Product")] = Spouses_Product_new
            yield newRow

    resultRDD = in0.repartition().sortWithinPartitions().rdd.mapPartitions(calculate)
    out0 = spark.createDataFrame(resultRDD, in0.schema)

    return out0
