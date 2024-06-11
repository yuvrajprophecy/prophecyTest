from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Vision(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(
        (
          ((((((col("PROC_CD_DESC").contains(lit("cataract")) | col("PROC_CD_DESC").contains(lit("cataract "))) | col("PROC_CD_DESC").contains(lit("lens"))) | col("PROC_CD_DESC").contains(lit("iris"))) | col("PROC_CD_DESC").contains(lit("intracapsular"))) | col("PROC_CD_DESC").contains(lit("xtracaslr"))) | col("PROC_CD_DESC").contains(lit("extracapsular")))
          | (col("PROC_CD_TYP_CD").cast(StringType()) == lit("VSN"))
        )
    )
