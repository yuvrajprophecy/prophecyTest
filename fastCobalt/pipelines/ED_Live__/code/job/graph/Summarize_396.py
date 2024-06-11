from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_396(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("Diagnosis"))

    return df1.agg(
        avg(col("edcnpa")).alias("Avg_edcnpa"), 
        avg(col("edcnnpa")).alias("Avg_edcnnpa"), 
        avg(col("alcohol")).alias("Avg_alcohol"), 
        avg(col("psych")).alias("Avg_psych"), 
        avg(col("noner")).alias("Avg_noner"), 
        avg(col("epct")).alias("Avg_epct"), 
        avg(col("injury")).alias("Avg_injury"), 
        avg(col("drug")).alias("Avg_drug")
    )
