from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CrossTab_4081(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("`Received Date`").alias("Received Date"))
    df2 = df1.pivot("DRUG_CLM_TIER_CD", ["TIER2", "UNKTIER", "NA", "TIER3", "TIER1"])

    return df2.agg(sum(col("counter")).alias("counter"))
