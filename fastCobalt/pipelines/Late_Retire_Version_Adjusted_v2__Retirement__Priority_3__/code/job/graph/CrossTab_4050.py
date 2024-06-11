from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def CrossTab_4050(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("`Received Date`").alias("Received Date"))
    df2 = df1.pivot("CLM_LN_POS_CD", ["Outpatient", "Inpatient", "Emergency_Room"])

    return df2.agg(sum(col("Counter")).alias("Counter"))
