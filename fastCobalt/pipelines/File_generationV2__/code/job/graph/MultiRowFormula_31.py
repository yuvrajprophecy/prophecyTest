from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def MultiRowFormula_31(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "RT Percent of Calls",
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
                                                    col("`Percent of Calls_lag1`")
                                                    + col(
                                                      "`Percent of Calls_lag2`"
                                                    )
                                                  )
                                                  + col(
                                                    "`Percent of Calls_lag3`"
                                                  )
                                                )
                                                + col(
                                                  "`Percent of Calls_lag4`"
                                                )
                                              )
                                              + col(
                                                "`Percent of Calls_lag5`"
                                              )
                                            )
                                            + col("`Percent of Calls_lag1`")
                                          )
                                          + col("`Percent of Calls_lag7`")
                                        )
                                        + col("`Percent of Calls_lag8`")
                                      )
                                      + col("`Percent of Calls_lag9`")
                                    )
                                    + col("`Percent of Calls_lag10`")
                                  )
                                  + col("`Percent of Calls_lag11`")
                                )
                                + col("`Percent of Calls_lag12`")
                              )
                              + col("`Percent of Calls_lag13`")
                            )
                            + col("`Percent of Calls_lag14`")
                          )
                          + col("`Percent of Calls_lag15`")
                        )
                        + col("`Percent of Calls_lag16`")
                      )
                      + col("`Percent of Calls_lag17`")
                    )
                    + col("`Percent of Calls_lag18`")
                  )
                  + col("`Percent of Calls_lag19`")
                )
                + col("`Percent of Calls_lag20`")
              )
              + col("`Percent of Calls`")
            )\
              .cast(
              IntegerType()
            )
            / lit(20)
          )
        )\
        .drop("Percent of Calls_lag1")\
        .drop("Percent of Calls_lag2")\
        .drop("Percent of Calls_lag3")\
        .drop("Percent of Calls_lag4")\
        .drop("Percent of Calls_lag5")\
        .drop("Percent of Calls_lag1")\
        .drop("Percent of Calls_lag7")\
        .drop("Percent of Calls_lag8")\
        .drop("Percent of Calls_lag9")\
        .drop("Percent of Calls_lag10")\
        .drop("Percent of Calls_lag11")\
        .drop("Percent of Calls_lag12")\
        .drop("Percent of Calls_lag13")\
        .drop("Percent of Calls_lag14")\
        .drop("Percent of Calls_lag15")\
        .drop("Percent of Calls_lag16")\
        .drop("Percent of Calls_lag17")\
        .drop("Percent of Calls_lag18")\
        .drop("Percent of Calls_lag19")\
        .drop("Percent of Calls_lag20")
