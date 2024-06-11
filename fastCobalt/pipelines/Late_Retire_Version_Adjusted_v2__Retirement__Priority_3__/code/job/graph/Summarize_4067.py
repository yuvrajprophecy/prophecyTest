from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Summarize_4067(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("MBR_INDV_BE_KEY"), col("`Received Date`").alias("Received Date"))

    return df1.agg(
        concat_ws(",", collect_list(col("`Procedure Type`"))).alias("Concat_Procedure Type"), 
        count(col("CLM_LN_PROC_CD_SK")).alias("Count"), 
        sum(col("MemberPay")).alias("Sum_MemberPay")
    )
