from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_36(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
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
        .drop("Sum_Sum_TTLHours_lag20")
