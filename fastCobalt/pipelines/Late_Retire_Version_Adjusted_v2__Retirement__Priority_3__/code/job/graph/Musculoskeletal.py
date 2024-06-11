from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Musculoskeletal(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          (((((col("PROC_CD_DESC").contains(lit("musculoskeletal")) | col("PROC_CD_DESC").contains(lit("joint"))) | col("PROC_CD_DESC").contains(lit("vertebroplasties"))) | col("PROC_CD_DESC").contains(lit("spinal"))) | col("PROC_CD_DESC").contains(lit("kyphoplasties"))) | col("PROC_CD_DESC").contains(lit("knee")))
          | col("PROC_CD_DESC").contains(lit("hip "))
        )
    )
