from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_180(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("id", monotonically_increasing_id())\
        .withColumn("Sum_Sum_TTLCalls_lag1", lag(col("Sum_Sum_TTLCalls"), 1).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag2", lag(col("Sum_Sum_TTLCalls"), 2).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag13", lag(col("Sum_Sum_TTLCalls"), 13).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag4", lag(col("Sum_Sum_TTLCalls"), 4).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag5", lag(col("Sum_Sum_TTLCalls"), 5).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag6", lag(col("Sum_Sum_TTLCalls"), 6).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag7", lag(col("Sum_Sum_TTLCalls"), 7).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag8", lag(col("Sum_Sum_TTLCalls"), 8).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag9", lag(col("Sum_Sum_TTLCalls"), 9).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag10", lag(col("Sum_Sum_TTLCalls"), 10).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag11", lag(col("Sum_Sum_TTLCalls"), 11).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag12", lag(col("Sum_Sum_TTLCalls"), 12).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag13", lag(col("Sum_Sum_TTLCalls"), 13).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag14", lag(col("Sum_Sum_TTLCalls"), 14).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag15", lag(col("Sum_Sum_TTLCalls"), 15).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag16", lag(col("Sum_Sum_TTLCalls"), 16).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag17", lag(col("Sum_Sum_TTLCalls"), 17).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag18", lag(col("Sum_Sum_TTLCalls"), 18).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag19", lag(col("Sum_Sum_TTLCalls"), 19).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLCalls_lag20", lag(col("Sum_Sum_TTLCalls"), 20).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag1", lag(col("Sum_Sum_TTLHours"), 1).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag2", lag(col("Sum_Sum_TTLHours"), 2).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag3", lag(col("Sum_Sum_TTLHours"), 3).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag4", lag(col("Sum_Sum_TTLHours"), 4).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag5", lag(col("Sum_Sum_TTLHours"), 5).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag6", lag(col("Sum_Sum_TTLHours"), 6).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag7", lag(col("Sum_Sum_TTLHours"), 7).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag8", lag(col("Sum_Sum_TTLHours"), 8).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag9", lag(col("Sum_Sum_TTLHours"), 9).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag10", lag(col("Sum_Sum_TTLHours"), 10).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag11", lag(col("Sum_Sum_TTLHours"), 11).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag12", lag(col("Sum_Sum_TTLHours"), 12).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag13", lag(col("Sum_Sum_TTLHours"), 13).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag14", lag(col("Sum_Sum_TTLHours"), 14).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag15", lag(col("Sum_Sum_TTLHours"), 15).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag16", lag(col("Sum_Sum_TTLHours"), 16).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag17", lag(col("Sum_Sum_TTLHours"), 17).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag18", lag(col("Sum_Sum_TTLHours"), 18).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag19", lag(col("Sum_Sum_TTLHours"), 19).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn("Sum_Sum_TTLHours_lag20", lag(col("Sum_Sum_TTLHours"), 20).over(Window.partitionBy(lit(1)).orderBy(col("id"))))\
        .withColumn(
          "RT Handle Time",
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    col("Sum_Sum_TTLCalls_lag1")
                                                    + col(
                                                      "Sum_Sum_TTLCalls_lag2"
                                                    )
                                                  )
                                                  + col(
                                                    "Sum_Sum_TTLCalls_lag13"
                                                  )
                                                )
                                                + col(
                                                  "Sum_Sum_TTLCalls_lag4"
                                                )
                                              )
                                              + col(
                                                "Sum_Sum_TTLCalls_lag5"
                                              )
                                            )
                                            + col("Sum_Sum_TTLCalls_lag6")
                                          )
                                          + col("Sum_Sum_TTLCalls_lag7")
                                        )
                                        + col("Sum_Sum_TTLCalls_lag8")
                                      )
                                      + col("Sum_Sum_TTLCalls_lag9")
                                    )
                                    + col("Sum_Sum_TTLCalls_lag10")
                                  )
                                  + col("Sum_Sum_TTLCalls_lag11")
                                )
                                + col("Sum_Sum_TTLCalls_lag12")
                              )
                              + col("Sum_Sum_TTLCalls_lag13")
                            )
                            + col("Sum_Sum_TTLCalls_lag14")
                          )
                          + col("Sum_Sum_TTLCalls_lag15")
                        )
                        + col("Sum_Sum_TTLCalls_lag16")
                      )
                      + col("Sum_Sum_TTLCalls_lag17")
                    )
                    + col("Sum_Sum_TTLCalls_lag18")
                  )
                  + col("Sum_Sum_TTLCalls_lag19")
                )
                + col("Sum_Sum_TTLCalls_lag20")
              )
              + col("Sum_Sum_TTLCalls")
            )
            / (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    col("Sum_Sum_TTLHours_lag1")
                                                    + col(
                                                      "Sum_Sum_TTLHours_lag2"
                                                    )
                                                  )
                                                  + col(
                                                    "Sum_Sum_TTLHours_lag3"
                                                  )
                                                )
                                                + col(
                                                  "Sum_Sum_TTLHours_lag4"
                                                )
                                              )
                                              + col(
                                                "Sum_Sum_TTLHours_lag5"
                                              )
                                            )
                                            + col("Sum_Sum_TTLHours_lag6")
                                          )
                                          + col("Sum_Sum_TTLHours_lag7")
                                        )
                                        + col("Sum_Sum_TTLHours_lag8")
                                      )
                                      + col("Sum_Sum_TTLHours_lag9")
                                    )
                                    + col("Sum_Sum_TTLHours_lag10")
                                  )
                                  + col("Sum_Sum_TTLHours_lag11")
                                )
                                + col("Sum_Sum_TTLHours_lag12")
                              )
                              + col("Sum_Sum_TTLHours_lag13")
                            )
                            + col("Sum_Sum_TTLHours_lag14")
                          )
                          + col("Sum_Sum_TTLHours_lag15")
                        )
                        + col("Sum_Sum_TTLHours_lag16")
                      )
                      + col("Sum_Sum_TTLHours_lag17")
                    )
                    + col("Sum_Sum_TTLHours_lag18")
                  )
                  + col("Sum_Sum_TTLHours_lag19")
                )
                + col("Sum_Sum_TTLHours_lag20")
              )
              + col("Sum_Sum_TTLHours")
            )
          )
        )\
        .drop("Sum_Sum_TTLCalls_lag1")\
        .drop("Sum_Sum_TTLCalls_lag2")\
        .drop("Sum_Sum_TTLCalls_lag13")\
        .drop("Sum_Sum_TTLCalls_lag4")\
        .drop("Sum_Sum_TTLCalls_lag5")\
        .drop("Sum_Sum_TTLCalls_lag6")\
        .drop("Sum_Sum_TTLCalls_lag7")\
        .drop("Sum_Sum_TTLCalls_lag8")\
        .drop("Sum_Sum_TTLCalls_lag9")\
        .drop("Sum_Sum_TTLCalls_lag10")\
        .drop("Sum_Sum_TTLCalls_lag11")\
        .drop("Sum_Sum_TTLCalls_lag12")\
        .drop("Sum_Sum_TTLCalls_lag13")\
        .drop("Sum_Sum_TTLCalls_lag14")\
        .drop("Sum_Sum_TTLCalls_lag15")\
        .drop("Sum_Sum_TTLCalls_lag16")\
        .drop("Sum_Sum_TTLCalls_lag17")\
        .drop("Sum_Sum_TTLCalls_lag18")\
        .drop("Sum_Sum_TTLCalls_lag19")\
        .drop("Sum_Sum_TTLCalls_lag20")\
        .drop("Sum_Sum_TTLHours_lag1")\
        .drop("Sum_Sum_TTLHours_lag2")\
        .drop("Sum_Sum_TTLHours_lag3")\
        .drop("Sum_Sum_TTLHours_lag4")\
        .drop("Sum_Sum_TTLHours_lag5")\
        .drop("Sum_Sum_TTLHours_lag6")\
        .drop("Sum_Sum_TTLHours_lag7")\
        .drop("Sum_Sum_TTLHours_lag8")\
        .drop("Sum_Sum_TTLHours_lag9")\
        .drop("Sum_Sum_TTLHours_lag10")\
        .drop("Sum_Sum_TTLHours_lag11")\
        .drop("Sum_Sum_TTLHours_lag12")\
        .drop("Sum_Sum_TTLHours_lag13")\
        .drop("Sum_Sum_TTLHours_lag14")\
        .drop("Sum_Sum_TTLHours_lag15")\
        .drop("Sum_Sum_TTLHours_lag16")\
        .drop("Sum_Sum_TTLHours_lag17")\
        .drop("Sum_Sum_TTLHours_lag18")\
        .drop("Sum_Sum_TTLHours_lag19")\
        .drop("Sum_Sum_TTLHours_lag20")\
        .drop("id")
